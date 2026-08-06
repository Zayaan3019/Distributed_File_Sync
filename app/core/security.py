"""
Transport security and path containment.

This module exists because of the Phase 0 audit (docs/SECURITY_AUDIT.md). Three
findings drive it:

  * **CRITICAL — arbitrary file write.** Received filenames were used verbatim.
    Sending ``file_path="../VICTIM.txt"`` wrote attacker bytes outside the sync
    root; ``"/tmp/x"`` escaped by absolute path. `safe_join` is the fix and it
    is applied to *every* path that arrives from the network.
  * **CRITICAL — no authentication.** Any TCP client that said HELLO was
    trusted. `Authenticator` replaces that with a mutual challenge-response
    over a pre-shared key: neither side learns anything until both have proven
    they hold the key.
  * **HIGH — cleartext transport.** File contents and paths crossed the wire in
    plain JSON. `SecureChannel` wraps every frame in AES-256-GCM.

Design constraints followed here:

  * The only new dependency is ``cryptography``, which is already a transitive
    dependency of the test environment on both platforms. Rolling our own AEAD
    was never on the table.
  * The pre-shared key never appears on the wire, not even hashed. Only HMAC
    proofs over freshly exchanged nonces do, so a passive observer cannot
    replay them and an active one cannot mount an offline dictionary attack
    without also breaking HMAC-SHA256.
  * Session keys are per-connection and per-direction. Reusing one key in both
    directions with a counter nonce would repeat a (key, nonce) pair, which is
    a total break of GCM.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import os
import secrets
import struct
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Optional, Tuple

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: Length of the random nonce each side contributes to the handshake. 32 bytes
#: makes accidental repetition impossible for any realistic connection count.
NONCE_LEN = 32

#: AES-GCM key length. 256-bit, because there is no reason to economise here.
KEY_LEN = 32

#: AES-GCM IV length in bytes, fixed by the construction.
IV_LEN = 12

#: PBKDF2 iteration count for turning a human-typed pre-shared key into key
#: material. The handshake runs once per connection, so a cost that is
#: imperceptible to us is still a real barrier to offline guessing.
KDF_ITERATIONS = 200_000

#: Fixed salt for the root key derivation. A per-connection salt is impossible
#: here -- both peers must derive the same root key from the same PSK before
#: they have exchanged anything. Per-connection uniqueness is instead supplied
#: by the exchanged nonces in `derive_session_keys`, which is where it actually
#: matters.
ROOT_SALT = b"dfs-psk-root-v1"

#: Domain separation tags. Signing the same nonces with different tags stops a
#: proof from one role being replayed as the other role's proof -- the
#: reflection attack that kills naive challenge-response.
TAG_CLIENT = b"dfs-auth-client-v1"
TAG_SERVER = b"dfs-auth-server-v1"
TAG_C2S = b"dfs-session-c2s-v1"
TAG_S2C = b"dfs-session-s2c-v1"

#: Upper bound on a single encrypted frame. Mirrors Protocol.MAX_MESSAGE_SIZE
#: plus AEAD overhead; enforced independently here so the transport is bounded
#: even if the JSON layer above it changes.
MAX_FRAME = 16 * 1024 * 1024

#: Windows device names that are not legal file names even though they look
#: like ordinary relative paths. A peer sending "COM1" would make the daemon
#: open a serial port on a Windows node.
_WINDOWS_RESERVED = {
    "CON", "PRN", "AUX", "NUL",
    *(f"COM{i}" for i in range(1, 10)),
    *(f"LPT{i}" for i in range(1, 10)),
}


class SecurityError(Exception):
    """Base class for security failures."""


class PathTraversalError(SecurityError):
    """A received path tried to escape the sync root."""


class AuthenticationError(SecurityError):
    """A peer failed to prove knowledge of the pre-shared key."""


class ChannelError(SecurityError):
    """A frame failed to decrypt, or violated the framing rules."""


# ---------------------------------------------------------------------------
# Path containment
# ---------------------------------------------------------------------------


def sanitize_relative_path(path: str) -> str:
    """
    Validate a peer-supplied path and return it in canonical relative form.

    This is the single chokepoint for every filename that arrives from the
    network. The rules are deliberately deny-by-default: anything not clearly a
    plain relative path inside the tree is rejected rather than normalised into
    something plausible, because "normalise until it looks safe" is how
    traversal bugs come back.

    Rejected:
        * absolute paths, POSIX (``/etc/passwd``) or Windows (``C:\\x``)
        * UNC paths (``\\\\server\\share``)
        * any ``..`` component, at any depth
        * NUL and other control characters
        * Windows reserved device names (``CON``, ``COM1``, ...)
        * empty paths, and paths that normalise to nothing
        * trailing dots or spaces on a component, which Windows silently strips
          and which therefore let two distinct paths collide

    Args:
        path: Path as received from a peer.

    Returns:
        Canonical relative path using forward slashes.

    Raises:
        PathTraversalError: If the path is not a safe relative path.

    Example:
        >>> sanitize_relative_path("docs/notes.txt")
        'docs/notes.txt'
        >>> sanitize_relative_path("../etc/passwd")
        Traceback (most recent call last):
        PathTraversalError: ...
    """
    if not isinstance(path, str):
        raise PathTraversalError(f"path must be a string, got {type(path).__name__}")

    if not path or not path.strip():
        raise PathTraversalError("empty path")

    if "\x00" in path:
        raise PathTraversalError("path contains NUL")

    if any(ord(ch) < 32 for ch in path):
        raise PathTraversalError("path contains control characters")

    # Normalise separators before any structural check, so that a Windows-style
    # "..\\..\\x" is caught by the same '..' rule as the POSIX form.
    unified = path.replace("\\", "/")

    if unified.startswith("//"):
        raise PathTraversalError(f"UNC path rejected: {path!r}")

    if unified.startswith("/"):
        raise PathTraversalError(f"absolute path rejected: {path!r}")

    # Drive-relative and drive-absolute Windows paths: "C:x", "C:/x".
    if len(unified) >= 2 and unified[1] == ":" and unified[0].isalpha():
        raise PathTraversalError(f"drive-qualified path rejected: {path!r}")

    parts = [part for part in unified.split("/") if part not in ("", ".")]
    if not parts:
        raise PathTraversalError(f"path normalises to nothing: {path!r}")

    for part in parts:
        if part == "..":
            raise PathTraversalError(f"parent traversal rejected: {path!r}")
        if part != part.rstrip(" ."):
            # "foo." and "foo " both open as "foo" on Windows, so two paths
            # that are distinct on Linux would collide there.
            #
            # rstrip, NOT strip: stripping both ends rejects every dotfile.
            # `.git` became an invalid name, `git init` failed inside the
            # mount, and Definition-of-Done #1 could not be met. Caught by
            # running real `git` against a real mount -- a Python-only test
            # would never have created a `.git` directory.
            #
            # A name that is entirely dots (`...`) still fails here. That is
            # intentional: it is a legal Linux name but an illegal Windows
            # one, so allowing it would let a Linux peer create a path a
            # Windows peer can never materialise.
            raise PathTraversalError(f"trailing dot or space in component: {path!r}")
        stem = part.split(".")[0].upper()
        if stem in _WINDOWS_RESERVED:
            raise PathTraversalError(f"reserved device name: {path!r}")

    return "/".join(parts)


def safe_join(root: str, relative_path: str) -> Path:
    """
    Join a peer-supplied relative path onto the sync root, safely.

    Two independent checks, because either alone has a known bypass:

    1. `sanitize_relative_path` rejects the path *textually*. This catches
       ``../`` before it ever reaches the filesystem.
    2. The result is then resolved and confirmed to still live under the
       resolved root. This catches what textual analysis cannot: a symlink
       already inside the tree that points outside it. An attacker who can
       create ``link -> /etc`` inside the sync directory would otherwise write
       to ``link/passwd`` using a path with no ``..`` in it at all.

    Args:
        root: Absolute path of the sync root.
        relative_path: Path as received from a peer.

    Returns:
        Absolute path guaranteed to be inside ``root``.

    Raises:
        PathTraversalError: If the path escapes the root by either route.
    """
    canonical = sanitize_relative_path(relative_path)

    root_resolved = Path(root).resolve()
    candidate = root_resolved / canonical

    # `strict=False` so a path that does not exist yet -- the common case for
    # an incoming create -- still resolves. Symlinks in the existing prefix are
    # followed, which is exactly the case we are defending against.
    resolved = Path(os.path.normpath(str(candidate)))
    try:
        resolved = candidate.resolve()
    except (OSError, RuntimeError):
        # A resolution loop or a permission error is itself disqualifying.
        raise PathTraversalError(f"cannot resolve path: {relative_path!r}")

    if resolved != root_resolved and root_resolved not in resolved.parents:
        raise PathTraversalError(
            f"path escapes sync root: {relative_path!r} -> {resolved}"
        )

    return resolved


def is_within_root(root: str, candidate: str) -> bool:
    """
    Report whether an absolute path lies inside the sync root.

    Args:
        root: Absolute path of the sync root.
        candidate: Absolute path to test.

    Returns:
        True if ``candidate`` is ``root`` or below it.
    """
    try:
        root_resolved = Path(root).resolve()
        target = Path(candidate).resolve()
    except (OSError, RuntimeError):
        return False
    return target == root_resolved or root_resolved in target.parents


# ---------------------------------------------------------------------------
# Key derivation
# ---------------------------------------------------------------------------


def derive_root_key(psk: str) -> bytes:
    """
    Stretch a human-supplied pre-shared key into 256 bits of key material.

    Args:
        psk: The pre-shared key, as configured on both peers.

    Returns:
        A 32-byte root key.

    Raises:
        ValueError: If the key is empty or trivially short.
    """
    if not psk:
        raise ValueError("pre-shared key must not be empty")
    if len(psk) < 8:
        raise ValueError("pre-shared key must be at least 8 characters")

    return hashlib.pbkdf2_hmac(
        "sha256", psk.encode("utf-8"), ROOT_SALT, KDF_ITERATIONS, dklen=KEY_LEN
    )


def derive_session_keys(root_key: bytes, client_nonce: bytes,
                        server_nonce: bytes) -> Tuple[bytes, bytes]:
    """
    Derive per-connection, per-direction AES keys from the handshake nonces.

    Both nonces feed every derivation, so a session key is fresh even if one
    side has a broken RNG, and a recorded session cannot be replayed against a
    new connection.

    Args:
        root_key: Output of `derive_root_key`.
        client_nonce: The initiator's nonce.
        server_nonce: The responder's nonce.

    Returns:
        Tuple of (client-to-server key, server-to-client key).
    """
    transcript = client_nonce + server_nonce

    def _kdf(tag: bytes) -> bytes:
        return hmac.new(root_key, tag + transcript, hashlib.sha256).digest()

    return _kdf(TAG_C2S), _kdf(TAG_S2C)


def compute_proof(root_key: bytes, tag: bytes, client_nonce: bytes,
                  server_nonce: bytes, node_id: str) -> bytes:
    """
    Compute one side's authentication proof.

    The proof binds both nonces and the claimed node id. Binding the node id
    is what stops a peer from authenticating correctly and then claiming to be
    a different node -- without it, the identity in HELLO would be unverified
    even though the key check passed.

    Args:
        root_key: Output of `derive_root_key`.
        tag: `TAG_CLIENT` or `TAG_SERVER`, for domain separation.
        client_nonce: The initiator's nonce.
        server_nonce: The responder's nonce.
        node_id: The node id being claimed by the proving side.

    Returns:
        A 32-byte HMAC proof.
    """
    message = tag + client_nonce + server_nonce + node_id.encode("utf-8")
    return hmac.new(root_key, message, hashlib.sha256).digest()


# ---------------------------------------------------------------------------
# Encrypted channel
# ---------------------------------------------------------------------------


class SecureChannel:
    """
    An authenticated, encrypted, length-prefixed byte channel over a TCP pair.

    Wire format per frame::

        [4 bytes big-endian ciphertext length][ciphertext || 16-byte GCM tag]

    The IV is **not** transmitted. It is derived from a monotonic per-direction
    counter that both sides track independently. That saves 12 bytes a frame,
    but far more importantly it makes replay and reordering detectable: a
    replayed frame decrypts under the wrong counter and fails authentication,
    rather than being silently accepted as it would be with a transmitted IV.

    The frame length is bound into the AEAD as associated data, so an attacker
    cannot truncate or splice frames without the tag check failing.

    Attributes:
        send_key: AES key for outbound frames.
        recv_key: AES key for inbound frames.
    """

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                 send_key: bytes, recv_key: bytes):
        """
        Initialize the channel.

        Args:
            reader: Stream to read encrypted frames from.
            writer: Stream to write encrypted frames to.
            send_key: AES-256 key for the outbound direction.
            recv_key: AES-256 key for the inbound direction.
        """
        self._reader = reader
        self._writer = writer
        self._send = AESGCM(send_key)
        self._recv = AESGCM(recv_key)
        self._send_counter = 0
        self._recv_counter = 0
        self._send_lock = asyncio.Lock()

    @staticmethod
    def _iv(counter: int) -> bytes:
        """
        Build the 12-byte IV for a frame counter.

        Args:
            counter: Monotonic frame counter for this direction.

        Returns:
            A 12-byte IV.
        """
        return b"\x00\x00\x00\x00" + struct.pack("!Q", counter)

    async def send(self, plaintext: bytes) -> None:
        """
        Encrypt and write one frame.

        Args:
            plaintext: Bytes to send.

        Raises:
            ChannelError: If the frame exceeds `MAX_FRAME`.
        """
        if len(plaintext) > MAX_FRAME:
            raise ChannelError(f"frame too large: {len(plaintext)} bytes")

        async with self._send_lock:
            counter = self._send_counter
            self._send_counter += 1
            # A 64-bit counter cannot realistically wrap, but if it ever did,
            # the (key, IV) pair would repeat and GCM would be broken outright.
            if counter >= (1 << 63):
                raise ChannelError("frame counter exhausted; reconnect required")

            length_prefix = struct.pack("!I", len(plaintext) + 16)
            ciphertext = self._send.encrypt(
                self._iv(counter), plaintext, length_prefix
            )
            self._writer.write(length_prefix + ciphertext)
            await self._writer.drain()

    async def receive(self) -> Optional[bytes]:
        """
        Read, authenticate and decrypt one frame.

        Returns:
            The plaintext, or None when the peer closed the connection.

        Raises:
            ChannelError: If the frame is oversized or fails authentication.
        """
        try:
            header = await self._reader.readexactly(4)
        except asyncio.IncompleteReadError:
            return None

        length = struct.unpack("!I", header)[0]

        # Bound the allocation before reading a single byte of body. This is
        # the same defence the JSON layer has, restated at the transport layer
        # so it holds regardless of what runs above.
        if length > MAX_FRAME + 16:
            raise ChannelError(f"frame too large: {length} bytes")
        if length < 16:
            raise ChannelError(f"frame shorter than GCM tag: {length} bytes")

        try:
            body = await self._reader.readexactly(length)
        except asyncio.IncompleteReadError:
            raise ChannelError("truncated frame")

        counter = self._recv_counter
        self._recv_counter += 1

        try:
            return self._recv.decrypt(self._iv(counter), body, header)
        except Exception as exc:
            # Deliberately opaque: distinguishing "bad tag" from "bad counter"
            # would leak oracle information.
            raise ChannelError("frame authentication failed") from exc


@dataclass
class AuthResult:
    """
    Outcome of a successful handshake.

    Attributes:
        peer_node_id: The authenticated peer's node id.
        channel: The encrypted channel to use from here on.
    """

    peer_node_id: str
    channel: SecureChannel


class Authenticator:
    """
    Mutual pre-shared-key authentication for peer connections.

    The exchange, with the initiator on the left::

        --> AUTH_INIT   : version, node_id, client_nonce
        <-- AUTH_SRV    : version, node_id, server_nonce, server_proof
        --> AUTH_CLI    : client_proof
        <-- AUTH_OK

    Both proofs are HMAC-SHA256 over both nonces plus the prover's node id,
    under distinct domain-separation tags. The responder proves first, so a
    client that has the wrong key learns nothing it could grind offline beyond
    what it already had; the initiator proves second, so an unauthenticated
    client is disconnected before it can send a single application message.

    This is the fix for the Phase 0 CRITICAL "any peer that connects can write
    files".

    Attributes:
        node_id: This node's identifier.
        root_key: Key material derived from the pre-shared key.
    """

    #: Handshake wire version, independent of the application Protocol.VERSION.
    HANDSHAKE_VERSION = 1

    #: Seconds to wait for each handshake step. An attacker holding sockets
    #: open mid-handshake is a resource-exhaustion vector otherwise.
    TIMEOUT = 10.0

    def __init__(self, node_id: str, psk: str):
        """
        Initialize the authenticator.

        Args:
            node_id: This node's identifier.
            psk: The pre-shared key, identical on every peer.
        """
        self.node_id = node_id
        self.root_key = derive_root_key(psk)

    @staticmethod
    async def _send_raw(writer: asyncio.StreamWriter, payload: bytes) -> None:
        """
        Send a length-prefixed cleartext handshake frame.

        Args:
            writer: Stream to write to.
            payload: Frame body.
        """
        writer.write(struct.pack("!I", len(payload)) + payload)
        await writer.drain()

    @staticmethod
    async def _recv_raw(reader: asyncio.StreamReader, limit: int = 4096) -> bytes:
        """
        Read a length-prefixed cleartext handshake frame.

        Handshake frames are tiny and fixed-shape, so the limit here is far
        tighter than the application limit -- there is no legitimate reason for
        a handshake frame to exceed a few hundred bytes.

        Args:
            reader: Stream to read from.
            limit: Maximum acceptable frame size.

        Returns:
            The frame body.

        Raises:
            AuthenticationError: On a truncated or oversized frame.
        """
        try:
            header = await reader.readexactly(4)
        except asyncio.IncompleteReadError:
            raise AuthenticationError("connection closed during handshake")

        length = struct.unpack("!I", header)[0]
        if length > limit:
            # A wildly out-of-range length almost always means the peer is not
            # speaking this protocol at all, not that its key is wrong. The
            # first four bytes of an HTTP response decode as 1213486160
            # ("HTTP"), which is exactly what turned up when another service on
            # the machine already held the port -- and the caller's advice to
            # "check that both peers share the same --psk" sent the reader
            # looking in entirely the wrong place.
            if header.isascii() and header.decode("ascii", "replace").isprintable():
                raise AuthenticationError(
                    f"peer is not speaking this protocol: handshake began with "
                    f"{header.decode('ascii', 'replace')!r}. Check that the "
                    f"address really is another sync node and that nothing "
                    f"else holds the port"
                )
            raise AuthenticationError(f"oversized handshake frame: {length}")

        try:
            return await reader.readexactly(length)
        except asyncio.IncompleteReadError:
            raise AuthenticationError("truncated handshake frame")

    def _encode(self, **fields) -> bytes:
        """
        Encode a handshake frame as compact JSON.

        Args:
            **fields: Frame fields; bytes values are hex-encoded.

        Returns:
            UTF-8 JSON bytes.
        """
        import json

        encoded = {
            key: (value.hex() if isinstance(value, bytes) else value)
            for key, value in fields.items()
        }
        return json.dumps(encoded, separators=(",", ":")).encode("utf-8")

    @staticmethod
    def _decode(payload: bytes) -> dict:
        """
        Decode a handshake frame.

        Args:
            payload: UTF-8 JSON bytes.

        Returns:
            The decoded mapping.

        Raises:
            AuthenticationError: If the frame is not valid JSON.
        """
        import json

        try:
            data = json.loads(payload.decode("utf-8"))
        except Exception as exc:
            raise AuthenticationError("malformed handshake frame") from exc
        if not isinstance(data, dict):
            raise AuthenticationError("handshake frame is not an object")
        return data

    async def authenticate_as_client(self, reader: asyncio.StreamReader,
                                     writer: asyncio.StreamWriter) -> AuthResult:
        """
        Run the handshake as the connection initiator.

        Args:
            reader: Stream reader for the connection.
            writer: Stream writer for the connection.

        Returns:
            The authenticated peer id and the encrypted channel.

        Raises:
            AuthenticationError: If the peer cannot prove it holds the key.
        """
        client_nonce = secrets.token_bytes(NONCE_LEN)

        await self._send_raw(writer, self._encode(
            v=self.HANDSHAKE_VERSION, node_id=self.node_id, nonce=client_nonce,
        ))

        response = self._decode(
            await asyncio.wait_for(self._recv_raw(reader), self.TIMEOUT)
        )

        if response.get("v") != self.HANDSHAKE_VERSION:
            raise AuthenticationError(
                f"handshake version mismatch: peer sent {response.get('v')!r}"
            )

        peer_id = response.get("node_id")
        if not isinstance(peer_id, str) or not peer_id:
            raise AuthenticationError("peer did not supply a node_id")

        try:
            server_nonce = bytes.fromhex(response["nonce"])
            server_proof = bytes.fromhex(response["proof"])
        except (KeyError, ValueError) as exc:
            raise AuthenticationError("malformed server proof") from exc

        if len(server_nonce) != NONCE_LEN:
            raise AuthenticationError("server nonce has wrong length")

        expected = compute_proof(
            self.root_key, TAG_SERVER, client_nonce, server_nonce, peer_id
        )
        # compare_digest, not ==, so proof verification is not a timing oracle.
        if not hmac.compare_digest(expected, server_proof):
            raise AuthenticationError("server failed pre-shared-key check")

        client_proof = compute_proof(
            self.root_key, TAG_CLIENT, client_nonce, server_nonce, self.node_id
        )
        await self._send_raw(writer, self._encode(proof=client_proof))

        confirmation = self._decode(
            await asyncio.wait_for(self._recv_raw(reader), self.TIMEOUT)
        )
        if not confirmation.get("ok"):
            raise AuthenticationError("server rejected our proof")

        c2s, s2c = derive_session_keys(self.root_key, client_nonce, server_nonce)
        return AuthResult(peer_id, SecureChannel(reader, writer, c2s, s2c))

    async def authenticate_as_server(self, reader: asyncio.StreamReader,
                                     writer: asyncio.StreamWriter) -> AuthResult:
        """
        Run the handshake as the connection acceptor.

        Args:
            reader: Stream reader for the connection.
            writer: Stream writer for the connection.

        Returns:
            The authenticated peer id and the encrypted channel.

        Raises:
            AuthenticationError: If the peer cannot prove it holds the key.
        """
        request = self._decode(
            await asyncio.wait_for(self._recv_raw(reader), self.TIMEOUT)
        )

        if request.get("v") != self.HANDSHAKE_VERSION:
            raise AuthenticationError(
                f"handshake version mismatch: peer sent {request.get('v')!r}"
            )

        peer_id = request.get("node_id")
        if not isinstance(peer_id, str) or not peer_id:
            raise AuthenticationError("peer did not supply a node_id")

        try:
            client_nonce = bytes.fromhex(request["nonce"])
        except (KeyError, ValueError) as exc:
            raise AuthenticationError("malformed client nonce") from exc

        if len(client_nonce) != NONCE_LEN:
            raise AuthenticationError("client nonce has wrong length")

        server_nonce = secrets.token_bytes(NONCE_LEN)
        server_proof = compute_proof(
            self.root_key, TAG_SERVER, client_nonce, server_nonce, self.node_id
        )

        await self._send_raw(writer, self._encode(
            v=self.HANDSHAKE_VERSION, node_id=self.node_id,
            nonce=server_nonce, proof=server_proof,
        ))

        answer = self._decode(
            await asyncio.wait_for(self._recv_raw(reader), self.TIMEOUT)
        )

        try:
            client_proof = bytes.fromhex(answer["proof"])
        except (KeyError, ValueError) as exc:
            raise AuthenticationError("malformed client proof") from exc

        expected = compute_proof(
            self.root_key, TAG_CLIENT, client_nonce, server_nonce, peer_id
        )
        if not hmac.compare_digest(expected, client_proof):
            # Tell the peer nothing beyond "no". Naming the reason would help
            # an attacker distinguish a wrong key from a malformed frame.
            await self._send_raw(writer, self._encode(ok=False))
            raise AuthenticationError(
                f"peer {peer_id!r} failed pre-shared-key check"
            )

        await self._send_raw(writer, self._encode(ok=True))

        c2s, s2c = derive_session_keys(self.root_key, client_nonce, server_nonce)
        # Direction is relative to the initiator, so the server's send key is
        # the server-to-client key. Getting this backwards would make every
        # frame fail to decrypt -- which is at least a loud failure, not a
        # silent one.
        return AuthResult(peer_id, SecureChannel(reader, writer, s2c, c2s))
