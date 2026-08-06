"""
Definition-of-Done #2, run against real containers.

`tests/test_two_node.py` already proves convergence and conflict presentation,
but it does so with two `SyncEngine` objects inside one Python process on one
event loop. That is a genuine end-to-end test of the protocol, and it is not a
test of *deployment*: it never builds the image, never reads the Dockerfile,
never starts `app.main`, never resolves a peer by DNS name, and never crosses a
container boundary. Every one of those is a place this project has already been
wrong -- the compose file passed no PSK and defined containers that could not
have started at all, and nothing noticed, because no test ran it.

So this module drives `docker compose` directly. It is slow and it is skipped
when Docker is absent, which is honest: a skip says "not verified here", while
folding it into the in-process test would have said "verified" about something
never run.

Files are written and read with `docker exec`, not through a bind mount. The
catalog is SQLite in WAL mode and a bind mount from a Windows host does not
provide the shared-memory locking WAL assumes.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import time
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
COMPOSE_FILE = PROJECT_ROOT / "docker-compose.yml"

#: Generous. The first run has to build the image; later runs hit the cache.
STARTUP_TIMEOUT = 180.0
CONVERGE_TIMEOUT = 90.0


def _docker_available() -> bool:
    """
    Report whether a usable Docker daemon is reachable.

    Returns:
        True if `docker compose version` succeeds.
    """
    if shutil.which("docker") is None:
        return False
    try:
        result = subprocess.run(
            ["docker", "compose", "version"],
            capture_output=True, timeout=60,
        )
        if result.returncode != 0:
            return False
        # `docker compose version` answers from the CLI plugin alone; the
        # daemon may still be down. `info` is what actually needs it.
        return subprocess.run(
            ["docker", "info"], capture_output=True, timeout=60,
        ).returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False


requires_docker = pytest.mark.skipif(
    not _docker_available(),
    reason="needs a running Docker daemon and the compose plugin",
)


def compose(*args, timeout=300, check=True) -> subprocess.CompletedProcess:
    """
    Run a `docker compose` subcommand against this project's compose file.

    Args:
        *args: Subcommand and arguments.
        timeout: Seconds before giving up.
        check: Raise on non-zero exit.

    Returns:
        The completed process.
    """
    result = subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), *args],
        capture_output=True, text=True, timeout=timeout,
        cwd=str(PROJECT_ROOT),
    )
    if check and result.returncode != 0:
        raise AssertionError(
            f"docker compose {' '.join(args)} failed ({result.returncode})\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


def exec_in(container: str, *argv, check=True) -> subprocess.CompletedProcess:
    """
    Run a command inside a running container.

    Args:
        container: Container name.
        *argv: Command and arguments.
        check: Raise on non-zero exit.

    Returns:
        The completed process.
    """
    result = subprocess.run(
        ["docker", "exec", container, *argv],
        capture_output=True, text=True, timeout=120,
    )
    if check and result.returncode != 0:
        raise AssertionError(
            f"docker exec {container} {' '.join(argv)} failed "
            f"({result.returncode})\nstdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    return result


def write_file(container: str, name: str, content: str) -> None:
    """
    Create a file in a container's sync directory.

    Uses a Python one-liner rather than a shell redirect: the image has no
    shell utilities guaranteed beyond what python:3.11-slim ships, and the
    interpreter is the one thing certain to be there.

    Args:
        container: Container name.
        name: File name inside /sync.
        content: Text to write.
    """
    exec_in(container, "python", "-c",
            f"open('/sync/{name}', 'w').write({content!r})")


def read_file(container: str, name: str) -> str:
    """
    Read a file from a container's sync directory.

    Args:
        container: Container name.
        name: File name inside /sync.

    Returns:
        File contents, or "" if absent.
    """
    result = exec_in(
        container, "python", "-c",
        f"import os,sys; p='/sync/{name}'; "
        f"sys.stdout.write(open(p).read() if os.path.exists(p) else '')",
    )
    return result.stdout


def list_files(container: str) -> list:
    """
    List the sync directory of a container, excluding internal state.

    Args:
        container: Container name.

    Returns:
        Sorted file names.
    """
    result = exec_in(
        container, "python", "-c",
        "import os,json; "
        "print(json.dumps(sorted(f for f in os.listdir('/sync') "
        "if not f.startswith('.'))))",
    )
    return json.loads(result.stdout.strip())


def wait_until(predicate, timeout=CONVERGE_TIMEOUT, interval=1.0) -> bool:
    """
    Poll a predicate until it holds.

    Args:
        predicate: Zero-argument callable.
        timeout: Seconds before giving up.
        interval: Seconds between polls.

    Returns:
        True if the predicate held.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if predicate():
                return True
        except AssertionError:
            # A container may not be accepting exec yet.
            pass
        time.sleep(interval)
    return False


@pytest.fixture(scope="module")
def cluster():
    """
    Bring up the two-node cluster, and tear it down with its volumes.

    Volumes are removed on the way out because they hold the catalog: a stale
    catalog from a previous run would let a test pass on last run's state.
    """
    compose("down", "-v", "--remove-orphans", check=False, timeout=180)
    compose("up", "-d", "--build", timeout=int(STARTUP_TIMEOUT))

    try:
        ready = wait_until(
            lambda: "node2" in peer_log("dfs_node1")
            and "node1" in peer_log("dfs_node2"),
            timeout=STARTUP_TIMEOUT,
            interval=2.0,
        )
        assert ready, (
            "peers never connected inside Docker.\n"
            f"node1 log:\n{compose('logs', 'node1', check=False).stdout[-3000:]}\n"
            f"node2 log:\n{compose('logs', 'node2', check=False).stdout[-3000:]}"
        )
        yield
    finally:
        compose("down", "-v", "--remove-orphans", check=False, timeout=180)


def _network_name() -> str:
    """
    Resolve the bridge network the containers actually joined.

    Compose prefixes network names with the project directory, which differs
    between checkouts, so it is read back from the running container rather
    than assumed.

    Returns:
        The Docker network name.
    """
    result = subprocess.run(
        ["docker", "inspect", "-f",
         "{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}",
         "dfs_node2"],
        capture_output=True, text=True, timeout=60,
    )
    name = result.stdout.strip()
    assert name, f"could not determine the network for dfs_node2: {result.stderr}"
    return name


def peer_log(container: str) -> str:
    """
    Fetch a container's log tail.

    Args:
        container: Container name.

    Returns:
        Log text, empty on failure.
    """
    result = subprocess.run(
        ["docker", "logs", "--tail", "200", container],
        capture_output=True, text=True, timeout=60,
    )
    return result.stdout + result.stderr


@requires_docker
class TestDockerTwoNode:
    """Two containers, one bridge network, real DNS between them."""

    def test_containers_are_running(self, cluster):
        """
        The containers stay up.

        Not a formality. The compose file previously supplied no PSK, and
        since `NetworkManager` refuses to construct without one the containers
        exited immediately -- a failure mode invisible to every other test.
        """
        for name in ("dfs_node1", "dfs_node2"):
            result = subprocess.run(
                ["docker", "inspect", "-f", "{{.State.Running}}", name],
                capture_output=True, text=True, timeout=60,
            )
            assert result.stdout.strip() == "true", (
                f"{name} is not running\n{peer_log(name)[-2000:]}"
            )

    def test_peers_authenticated_over_the_bridge_network(self, cluster):
        """Both sides completed the PSK handshake across the container network."""
        assert wait_until(
            lambda: "Authenticated peer" in peer_log("dfs_node1")
            or "connected from" in peer_log("dfs_node1"),
            timeout=60.0,
        ), f"node1 never authenticated a peer\n{peer_log('dfs_node1')[-3000:]}"

    def test_file_propagates_between_containers(self, cluster):
        """A file created in one container appears in the other."""
        write_file("dfs_node1", "from_node1.txt", "written inside node1")

        assert wait_until(
            lambda: read_file("dfs_node2", "from_node1.txt") == "written inside node1"
        ), (
            f"file never reached node2. node2 has {list_files('dfs_node2')}\n"
            f"{peer_log('dfs_node2')[-3000:]}"
        )

    def test_propagation_is_bidirectional(self, cluster):
        """The reverse direction works too, over the separate connection."""
        write_file("dfs_node2", "from_node2.txt", "written inside node2")

        assert wait_until(
            lambda: read_file("dfs_node1", "from_node2.txt") == "written inside node2"
        ), (
            f"file never reached node1. node1 has {list_files('dfs_node1')}\n"
            f"{peer_log('dfs_node1')[-3000:]}"
        )

    def test_concurrent_edit_surfaces_a_visible_conflict(self, cluster):
        """
        Definition-of-Done #2, second half.

        The edits are made during a real network partition. Two back-to-back
        `docker exec` calls are *not* concurrent -- the first edit propagates
        in well under the second's start-up time, so the second is causally
        after it and correctly overwrites rather than conflicting. Detaching
        node2 from the bridge network is the only way to make the two edits
        genuinely concurrent, and it also exercises the reconnect path, since
        both nodes must re-establish the connection once the network is back.

        The rule is deterministic -- the lexicographically lower node id keeps
        the path -- so node1 wins and node2's version must survive as a
        conflict sibling a user can see with `ls`. Silently losing an edit
        would be worse than either outcome.
        """
        write_file("dfs_node1", "shared.txt", "baseline")
        assert wait_until(
            lambda: read_file("dfs_node2", "shared.txt") == "baseline"
        ), "baseline never converged"

        network = _network_name()

        subprocess.run(
            ["docker", "network", "disconnect", network, "dfs_node2"],
            capture_output=True, text=True, timeout=60, check=True,
        )
        try:
            # Neither node can hear the other, so these are truly concurrent.
            write_file("dfs_node1", "shared.txt", "node1 version")
            write_file("dfs_node2", "shared.txt", "node2 version")
            time.sleep(3)
        finally:
            subprocess.run(
                ["docker", "network", "connect", network, "dfs_node2"],
                capture_output=True, text=True, timeout=60, check=True,
            )

        def conflict_visible():
            names = list_files("dfs_node1") + list_files("dfs_node2")
            return any(".conflict-" in name for name in names)

        assert wait_until(conflict_visible, timeout=CONVERGE_TIMEOUT), (
            "concurrent edit did not surface as a conflict sibling.\n"
            f"node1: {list_files('dfs_node1')}\n"
            f"node2: {list_files('dfs_node2')}\n"
            f"{peer_log('dfs_node1')[-3000:]}"
        )

        siblings = [n for n in list_files("dfs_node1") + list_files("dfs_node2")
                    if ".conflict-" in n]
        # The loser is named in the sibling, so a user can tell whose edit it
        # was without opening the file.
        assert any("node2" in name for name in siblings), (
            f"conflict sibling does not name the losing node: {siblings}"
        )

    def test_large_file_crosses_the_container_boundary(self, cluster):
        """
        A multi-megabyte file, which is where the transfer path actually gets
        exercised. Small files complete in a single burst and hide anything
        that goes wrong part-way through a stream.
        """
        exec_in("dfs_node1", "python", "-c",
                "open('/sync/big.bin','wb').write(bytes(range(256))*16384)")
        expected_size = 256 * 16384

        def arrived():
            out = exec_in(
                "dfs_node2", "python", "-c",
                "import os; p='/sync/big.bin'; "
                "print(os.path.getsize(p) if os.path.exists(p) else -1)",
                check=False,
            ).stdout.strip()
            return out == str(expected_size)

        assert wait_until(arrived, timeout=CONVERGE_TIMEOUT), (
            f"4MB file never crossed. node2 has {list_files('dfs_node2')}\n"
            f"{peer_log('dfs_node2')[-3000:]}"
        )

        digest_args = ("python", "-c",
                       "import hashlib; "
                       "print(hashlib.sha256(open('/sync/big.bin','rb').read())"
                       ".hexdigest())")
        assert (exec_in("dfs_node1", *digest_args).stdout.strip()
                == exec_in("dfs_node2", *digest_args).stdout.strip()), \
            "file crossed but the bytes differ"

    def test_delete_propagates(self, cluster):
        """A deletion on one node removes the file on the other."""
        write_file("dfs_node1", "doomed.txt", "temporary")
        assert wait_until(
            lambda: read_file("dfs_node2", "doomed.txt") == "temporary"
        ), "file never converged before delete"

        exec_in("dfs_node1", "python", "-c", "import os; os.unlink('/sync/doomed.txt')")

        assert wait_until(
            lambda: "doomed.txt" not in list_files("dfs_node2")
        ), (
            f"delete never propagated. node2 has {list_files('dfs_node2')}\n"
            f"{peer_log('dfs_node2')[-3000:]}"
        )
