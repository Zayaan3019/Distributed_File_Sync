"""
Tests for the transport-security and path-containment layer.

These correspond directly to Phase 0 audit findings. Each finding that was
demonstrated with a working exploit has a test here that would fail again if
the fix were reverted.
"""

import asyncio
import os
import tempfile
from pathlib import Path

import pytest

from app.core.security import (
    Authenticator,
    AuthenticationError,
    ChannelError,
    PathTraversalError,
    SecureChannel,
    compute_proof,
    derive_root_key,
    derive_session_keys,
    is_within_root,
    safe_join,
    sanitize_relative_path,
)


class TestPathSanitisation:
    """
    CRITICAL finding: received filenames were used verbatim, so a peer sending
    '../VICTIM.txt' wrote outside the sync root. Probe output is in
    docs/SECURITY_AUDIT.md.
    """

    @pytest.mark.parametrize("evil", [
        "../etc/passwd",
        "../../../../../../etc/passwd",
        "ok/../../escape.txt",
        "a/b/../../../c",
        "..",
        "../",
        "sub/..",
    ])
    def test_parent_traversal_rejected(self, evil):
        with pytest.raises(PathTraversalError):
            sanitize_relative_path(evil)

    @pytest.mark.parametrize("evil", [
        "/etc/passwd",
        "/tmp/x",
        "//server/share/x",
        "C:/Windows/System32/x",
        "C:\\Windows\\x",
        "d:relative",
    ])
    def test_absolute_and_drive_paths_rejected(self, evil):
        with pytest.raises(PathTraversalError):
            sanitize_relative_path(evil)

    @pytest.mark.parametrize("evil", [
        "..\\..\\windows\\x",
        "sub\\..\\..\\out.txt",
    ])
    def test_backslash_traversal_rejected(self, evil):
        """Windows-style separators must hit the same rule as POSIX ones."""
        with pytest.raises(PathTraversalError):
            sanitize_relative_path(evil)

    @pytest.mark.parametrize("evil", ["a\x00b", "nul\x00", "bell\x07"])
    def test_control_characters_rejected(self, evil):
        with pytest.raises(PathTraversalError):
            sanitize_relative_path(evil)

    @pytest.mark.parametrize("evil", ["", "   ", ".", "./", "./."])
    def test_empty_paths_rejected(self, evil):
        with pytest.raises(PathTraversalError):
            sanitize_relative_path(evil)

    @pytest.mark.parametrize("evil", ["COM1", "con", "LPT9.txt", "NUL", "aux"])
    def test_windows_device_names_rejected(self, evil):
        """A peer sending 'COM1' would make a Windows node open a serial port."""
        with pytest.raises(PathTraversalError):
            sanitize_relative_path(evil)

    @pytest.mark.parametrize("evil", ["foo.", "bar ", "a/b./c", "x/y ", "..."])
    def test_trailing_dot_or_space_rejected(self, evil):
        """Windows silently strips these, so two paths would collide."""
        with pytest.raises(PathTraversalError):
            sanitize_relative_path(evil)

    @pytest.mark.parametrize("good", [" leading.txt", "a/ b.txt"])
    def test_leading_space_is_allowed(self, good):
        """
        Only *trailing* dots and spaces are stripped by Windows. A leading
        space survives on both platforms, so it creates no collision and is
        not our business to forbid.
        """
        assert sanitize_relative_path(good) == good.strip("/")

    @pytest.mark.parametrize("good,expected", [
        ("notes.txt", "notes.txt"),
        ("docs/notes.txt", "docs/notes.txt"),
        ("docs\\notes.txt", "docs/notes.txt"),
        ("./docs/./notes.txt", "docs/notes.txt"),
        ("a/b/c/d.bin", "a/b/c/d.bin"),
        ("file with spaces.txt", "file with spaces.txt"),
        ("unicode-ß-文件.txt", "unicode-ß-文件.txt"),
    ])
    def test_legitimate_paths_pass(self, good, expected):
        assert sanitize_relative_path(good) == expected

    @pytest.mark.parametrize("dotfile", [
        ".git", ".gitignore", ".bashrc", ".config/app.conf",
        ".git/objects/ab/cdef", "repo/.git/HEAD",
    ])
    def test_dotfiles_are_allowed(self, dotfile):
        """
        Regression: an over-eager strip(' .') rejected every dotfile, so
        `git init` failed inside the mount and Definition-of-Done #1 was
        unreachable. Caught only by running real git against a real mount.
        """
        assert sanitize_relative_path(dotfile) == dotfile


class TestSafeJoin:
    """safe_join must contain both textual and symlink escapes."""

    def test_join_produces_path_under_root(self, tmp_path):
        result = safe_join(str(tmp_path), "a/b/c.txt")
        assert str(result).startswith(str(tmp_path.resolve()))

    def test_traversal_blocked(self, tmp_path):
        with pytest.raises(PathTraversalError):
            safe_join(str(tmp_path), "../outside.txt")

    @pytest.mark.skipif(os.name == "nt", reason="symlink creation needs privileges on Windows")
    def test_symlink_escape_blocked(self, tmp_path):
        """
        A path with no '..' in it can still escape, if a symlink inside the
        tree points out of it. Textual validation alone cannot catch this,
        which is why safe_join also resolves and re-checks.
        """
        root = tmp_path / "sync"
        root.mkdir()
        outside = tmp_path / "outside"
        outside.mkdir()
        (outside / "secret.txt").write_text("secret")

        (root / "link").symlink_to(outside, target_is_directory=True)

        with pytest.raises(PathTraversalError):
            safe_join(str(root), "link/secret.txt")

    def test_is_within_root(self, tmp_path):
        inner = tmp_path / "a" / "b"
        inner.mkdir(parents=True)
        assert is_within_root(str(tmp_path), str(inner))
        assert not is_within_root(str(inner), str(tmp_path))


class TestKeyDerivation:
    """Key material must be deterministic across peers and fresh per session."""

    def test_root_key_is_deterministic(self):
        assert derive_root_key("shared-secret") == derive_root_key("shared-secret")

    def test_different_psks_give_different_keys(self):
        assert derive_root_key("secret-one") != derive_root_key("secret-two")

    @pytest.mark.parametrize("bad", ["", "short"])
    def test_weak_psk_rejected(self, bad):
        with pytest.raises(ValueError):
            derive_root_key(bad)

    def test_session_keys_differ_by_direction(self):
        """
        Reusing one key in both directions with a counter nonce repeats a
        (key, IV) pair, which is a total break of AES-GCM.
        """
        root = derive_root_key("shared-secret")
        c2s, s2c = derive_session_keys(root, b"a" * 32, b"b" * 32)
        assert c2s != s2c

    def test_session_keys_depend_on_both_nonces(self):
        root = derive_root_key("shared-secret")
        first = derive_session_keys(root, b"a" * 32, b"b" * 32)
        second = derive_session_keys(root, b"a" * 32, b"c" * 32)
        assert first != second

    def test_proof_is_domain_separated_by_role(self):
        """
        Without distinct tags, a server's proof could be reflected back as a
        client's proof and authentication would be trivially bypassable.
        """
        from app.core.security import TAG_CLIENT, TAG_SERVER

        root = derive_root_key("shared-secret")
        client = compute_proof(root, TAG_CLIENT, b"a" * 32, b"b" * 32, "node")
        server = compute_proof(root, TAG_SERVER, b"a" * 32, b"b" * 32, "node")
        assert client != server

    def test_proof_binds_node_id(self):
        """Otherwise a peer could authenticate as A and then claim to be B."""
        from app.core.security import TAG_CLIENT

        root = derive_root_key("shared-secret")
        as_a = compute_proof(root, TAG_CLIENT, b"a" * 32, b"b" * 32, "node_a")
        as_b = compute_proof(root, TAG_CLIENT, b"a" * 32, b"b" * 32, "node_b")
        assert as_a != as_b


async def _connected_pair():
    """
    Open a real loopback socket pair.

    Returns:
        Tuple of (server_reader, server_writer, client_reader, client_writer,
        close callable).
    """
    endpoints = {}
    ready = asyncio.Event()

    async def handler(reader, writer):
        endpoints["server"] = (reader, writer)
        ready.set()
        await asyncio.sleep(30)

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]

    client_reader, client_writer = await asyncio.open_connection("127.0.0.1", port)
    await asyncio.wait_for(ready.wait(), timeout=5)
    server_reader, server_writer = endpoints["server"]

    async def close():
        client_writer.close()
        server_writer.close()
        server.close()
        await server.wait_closed()

    return server_reader, server_writer, client_reader, client_writer, close


class TestMutualAuthentication:
    """
    CRITICAL finding: any TCP client that said HELLO was trusted to write
    files. These tests pin the replacement handshake.
    """

    @pytest.mark.asyncio
    async def test_matching_psk_authenticates_both_ways(self):
        sr, sw, cr, cw, close = await _connected_pair()
        try:
            server = Authenticator("server_node", "shared-secret-key")
            client = Authenticator("client_node", "shared-secret-key")

            results = await asyncio.gather(
                server.authenticate_as_server(sr, sw),
                client.authenticate_as_client(cr, cw),
            )
            server_result, client_result = results

            assert server_result.peer_node_id == "client_node"
            assert client_result.peer_node_id == "server_node"
        finally:
            await close()

    @pytest.mark.asyncio
    async def test_wrong_psk_is_rejected(self):
        sr, sw, cr, cw, close = await _connected_pair()
        try:
            server = Authenticator("server_node", "the-real-secret")
            client = Authenticator("attacker", "a-guessed-secret")

            outcomes = await asyncio.gather(
                server.authenticate_as_server(sr, sw),
                client.authenticate_as_client(cr, cw),
                return_exceptions=True,
            )
            assert any(isinstance(o, AuthenticationError) for o in outcomes), outcomes
        finally:
            await close()

    @pytest.mark.asyncio
    async def test_client_verifies_server_before_proving_itself(self):
        """
        The responder proves first. A client talking to an impostor must
        abort without ever sending its own proof, so the impostor gains no
        material to grind offline.
        """
        sr, sw, cr, cw, close = await _connected_pair()
        try:
            impostor = Authenticator("impostor", "wrong-secret-key")
            client = Authenticator("client_node", "the-real-secret")

            outcomes = await asyncio.gather(
                impostor.authenticate_as_server(sr, sw),
                client.authenticate_as_client(cr, cw),
                return_exceptions=True,
            )
            client_outcome = outcomes[1]
            assert isinstance(client_outcome, AuthenticationError)
            assert "server failed" in str(client_outcome)
        finally:
            await close()

    @pytest.mark.asyncio
    async def test_oversized_handshake_frame_rejected(self):
        """An attacker-controlled length prefix must not drive allocation."""
        import struct

        sr, sw, cr, cw, close = await _connected_pair()
        try:
            server = Authenticator("server_node", "shared-secret-key")
            cw.write(struct.pack("!I", 0xFFFFFFFF))
            await cw.drain()

            with pytest.raises(AuthenticationError, match="oversized"):
                await server.authenticate_as_server(sr, sw)
        finally:
            await close()

    @pytest.mark.asyncio
    async def test_garbage_handshake_rejected(self):
        sr, sw, cr, cw, close = await _connected_pair()
        try:
            import struct

            payload = b"not json at all"
            cw.write(struct.pack("!I", len(payload)) + payload)
            await cw.drain()

            server = Authenticator("server_node", "shared-secret-key")
            with pytest.raises(AuthenticationError):
                await server.authenticate_as_server(sr, sw)
        finally:
            await close()


class TestSecureChannel:
    """Frames must be confidential, authenticated, and replay-resistant."""

    @pytest.mark.asyncio
    async def test_round_trip(self):
        sr, sw, cr, cw, close = await _connected_pair()
        try:
            server = Authenticator("server_node", "shared-secret-key")
            client = Authenticator("client_node", "shared-secret-key")
            s_result, c_result = await asyncio.gather(
                server.authenticate_as_server(sr, sw),
                client.authenticate_as_client(cr, cw),
            )

            await c_result.channel.send(b"hello over the wire")
            assert await s_result.channel.receive() == b"hello over the wire"

            await s_result.channel.send(b"and back again")
            assert await c_result.channel.receive() == b"and back again"
        finally:
            await close()

    @pytest.mark.asyncio
    async def test_ciphertext_does_not_contain_plaintext(self):
        """The whole point of encryption in transit."""
        sr, sw, cr, cw, close = await _connected_pair()
        try:
            server = Authenticator("server_node", "shared-secret-key")
            client = Authenticator("client_node", "shared-secret-key")
            s_result, c_result = await asyncio.gather(
                server.authenticate_as_server(sr, sw),
                client.authenticate_as_client(cr, cw),
            )

            secret = b"TOP-SECRET-FILE-CONTENT-0123456789"
            await c_result.channel.send(secret)

            raw = await sr.read(4 + len(secret) + 16)
            assert secret not in raw
        finally:
            await close()

    @pytest.mark.asyncio
    async def test_tampered_frame_fails_authentication(self):
        keys = derive_session_keys(derive_root_key("shared-secret-key"),
                                   b"a" * 32, b"b" * 32)
        reader = asyncio.StreamReader()
        captured = bytearray()

        class Sink:
            def write(self, data):
                captured.extend(data)

            async def drain(self):
                pass

        sender = SecureChannel(None, Sink(), keys[0], keys[1])
        await sender.send(b"authentic message")

        # Flip one bit of ciphertext.
        tampered = bytearray(captured)
        tampered[8] ^= 0x01
        reader.feed_data(bytes(tampered))

        receiver = SecureChannel(reader, None, keys[1], keys[0])
        with pytest.raises(ChannelError):
            await receiver.receive()

    @pytest.mark.asyncio
    async def test_replayed_frame_fails(self):
        """
        The IV comes from a per-direction counter that is never transmitted,
        so a replayed frame decrypts under the wrong counter and the tag check
        fails. With a transmitted IV it would be silently accepted.
        """
        keys = derive_session_keys(derive_root_key("shared-secret-key"),
                                   b"a" * 32, b"b" * 32)
        captured = bytearray()

        class Sink:
            def write(self, data):
                captured.extend(data)

            async def drain(self):
                pass

        sender = SecureChannel(None, Sink(), keys[0], keys[1])
        await sender.send(b"transfer 100 dollars")

        reader = asyncio.StreamReader()
        reader.feed_data(bytes(captured))
        reader.feed_data(bytes(captured))  # the replay

        receiver = SecureChannel(reader, None, keys[1], keys[0])
        assert await receiver.receive() == b"transfer 100 dollars"

        with pytest.raises(ChannelError):
            await receiver.receive()

    @pytest.mark.asyncio
    async def test_oversized_frame_rejected_before_allocation(self):
        import struct

        keys = derive_session_keys(derive_root_key("shared-secret-key"),
                                   b"a" * 32, b"b" * 32)
        reader = asyncio.StreamReader()
        reader.feed_data(struct.pack("!I", 0xFFFFFFFF))

        receiver = SecureChannel(reader, None, keys[0], keys[1])
        with pytest.raises(ChannelError, match="too large"):
            await receiver.receive()
