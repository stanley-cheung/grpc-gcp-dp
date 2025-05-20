import subprocess

CELL = "wv"
CELL_IPV6_PREFIX = "2001:4860:8040:0826"

def query_cell_tasks(page: int):
    print(f"Querying cell {CELL} page {page}")
    p = subprocess.run(
        [
            'gqui',  'from',
            f"flatten(/ls/{CELL}/borg/{CELL}/bns/cloud-bigtable/anviltop-prod.frontend/{page}, Entry)",
            'proto', 'BNSAddrs',
            'SELECT Entry.unnamed_port, Entry.task_uid',
            '--select_format=csv',
        ],
        capture_output=True,
        text=True,
    )
    if p.returncode:
        return False
    result = p.stdout.split()[1:]
    tasks = []
    for line in result:
        (port, uid) = line.split(",")
        ipv6 = "".join([CELL_IPV6_PREFIX,":",uid[2:6],":",uid[6:10],":",uid[10:14],":",uid[14:18]])
        tasks.append({
            'ipv6': ipv6,
            'port': port,
        })
    print(f"Got {len(tasks)} results")
    return tasks

def write_script_to_file(tasks):
    with open("task-query.py", "w") as f:
        s = """import socket
addresses = [
"""
        s += "    ('google.com', 80)"
        s += """]
for address in addresses:
    try:
        sock = socket.create_connection(address, timeout=5)
        print(f"Successful connecting to {address}")
    except Exception as e:
        print(f"Connection failed: {address}")
"""
        f.write(s)

def run_script_in_vm():
    print('Copying file to VM')
    p = subprocess.run(
        [
            "gcloud", "compute", "scp", "task-query.py",
            "dp-sc-octant-vm-01-do-not-delete:~/",
            "--zone=us-central1-a",
        ],
        capture_output=True,
        text=True,
    )
    print(p.returncode)
    print('Running script in VM')
    p = subprocess.run(
        [
            "gcloud", "compute", "ssh",
            "dp-sc-octant-vm-01-do-not-delete",
            "--zone=us-central1-a",
            "--",
            "python3 ~/task-query.py",
        ],
        capture_output=True,
        text=True,
    )
    print(p.stdout)

def main():
    tasks = []
    page = 0
    while page_tasks := query_cell_tasks(page):
        tasks = tasks + page_tasks
        page += 1
    # for task in tasks:
    #    print(f"{task['ipv6']} {task['port']}")
    print(f"Got {len(tasks)} total results")
    write_script_to_file(tasks)
    run_script_in_vm()


if __name__ == "__main__":
    main()
