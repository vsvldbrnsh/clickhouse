"""
ClickHouse Keeper DDL queue cleanup utility.

A task is considered stuck when:
  - shards/hostname:9000/tries_to_execute == b'0' for all listed shards, AND
  - finished/ has no children (task never completed)

Usage (from project root):
    # Dry run — show what would be deleted:
    docker run --rm --network clickhouse-net \
        -v "$(pwd)/scripts:/scripts" python:3.12-slim \
        bash -c "pip install kazoo -q && python3 /scripts/clear_ddl_queue.py"

    # Actually delete:
    docker run --rm --network clickhouse-net \
        -v "$(pwd)/scripts:/scripts" python:3.12-slim \
        bash -c "pip install kazoo -q && python3 /scripts/clear_ddl_queue.py --delete"
"""

import sys
import time
from kazoo.client import KazooClient

DRY_RUN = "--delete" not in sys.argv
KEEPER_HOST = "keeper1"
KEEPER_PORT = 9181
DDL_ROOT = "/clickhouse/task_queue/ddl"


def delete_recursive(zk, path):
    if not zk.exists(path):
        return
    for child in zk.get_children(path):
        delete_recursive(zk, f"{path}/{child}")
    zk.delete(path)


def is_stuck(zk, task_path):
    """Return True if the task has no finished entries and tries_to_execute=0 on all shards."""
    finished_path = f"{task_path}/finished"
    if zk.exists(finished_path) and zk.get_children(finished_path):
        return False  # has finished entries — not stuck

    shards_path = f"{task_path}/shards"
    if not zk.exists(shards_path):
        return False

    for shard in zk.get_children(shards_path):
        tries_path = f"{shards_path}/{shard}/tries_to_execute"
        if zk.exists(tries_path):
            data, _ = zk.get(tries_path)
            if data.strip() != b"0":
                return False  # still has tries remaining
    return True


def main():
    print(f"Connecting to {KEEPER_HOST}:{KEEPER_PORT}...")
    zk = KazooClient(hosts=f"{KEEPER_HOST}:{KEEPER_PORT}", timeout=30)
    zk.start(timeout=15)
    time.sleep(0.5)

    tasks = sorted(zk.get_children(DDL_ROOT))
    print(f"Found {len(tasks)} tasks in DDL queue\n")

    stuck = []
    for task in tasks:
        path = f"{DDL_ROOT}/{task}"
        data, _ = zk.get(path)
        query_line = next(
            (l for l in data.decode(errors="replace").splitlines() if l.startswith("query:")),
            "query: (unknown)"
        )
        if is_stuck(zk, path):
            stuck.append((task, query_line[7:].strip()[:80]))
            print(f"  STUCK  {task}  {query_line[7:].strip()[:80]}")
        else:
            print(f"  OK     {task}")

    print(f"\n{len(stuck)} stuck task(s) found.")

    if not stuck:
        print("Nothing to do.")
        zk.stop()
        return

    if DRY_RUN:
        print("\nDry run — pass --delete to remove stuck tasks.")
        zk.stop()
        return

    print("\nDeleting stuck tasks...")
    for task, query in stuck:
        path = f"{DDL_ROOT}/{task}"
        delete_recursive(zk, path)
        print(f"  Deleted: {task}  ({query})")

    zk.stop()
    print("\nDone. Restart ClickHouse nodes or wait ~10s for DDLWorker to pick up.")


if __name__ == "__main__":
    main()
