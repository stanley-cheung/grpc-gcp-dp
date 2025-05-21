r"""A script to test connection to each borg task from a cell for a CBT job.

cell prefix mapping:
https://source.corp.google.com/piper///depot/google3/net/fabric/ipv6/data/gdu/cell-prefix-mappings.data

Sample usage:
$ python3 test-borg-task-connection.py \
  --cell=wv --cell_ipv6_prefix=2001:4860:8040:0826
$ python3 test-borg-task-connection.py \
  --cell=wi --cell_ipv6_prefix=2001:4860:8040:07b2

Optional parameters:
  --vm_name=VM_NAME           VM needs to be created with
                   --private-ipv6-google-access-type=enable-outbound-vm-access
  --num_runs=N                ping each task N times
  --num_secs_between_runs=N   wait N seconds before each run
"""

import argparse
import subprocess

args = None


def query_cell_tasks(page: int):
  """Use gqui to query the list of tasks from a borg cell."""
  print(f'Querying gqui for tasks on cell {args.cell} page {page} ...')
  p = subprocess.run(
      [
          'gqui', 'from',
          f'flatten(/ls/{args.cell}/borg/{args.cell}/bns/cloud-bigtable/'
          f'anviltop-prod.frontend/{page}, Entry)',
          'proto', 'BNSAddrs',
          'SELECT Entry.unnamed_port, Entry.task_uid',
          '--select_format=csv',
      ],
      capture_output=True,
      text=True,
      check=False,
  )
  if p.returncode:
    print(f'Done with querying cell {args.cell}')
    return False

  result = p.stdout.split()[1:]

  tasks = []
  for line in result:
    (port, uid) = line.split(',')
    ipv6 = ''.join([args.cell_ipv6_prefix,
                    ':', uid[2:6], ':', uid[6:10],
                    ':', uid[10:14], ':', uid[14:18]])
    tasks.append({
        'ipv6': ipv6,
        'port': port,
    })
  print(f'Got {len(tasks)} results')
  return tasks


def write_script_to_file(tasks) -> None:
  """Write script to a VM."""
  with open(args.task_query_script_name, 'w') as f:
    s = """from collections import defaultdict
import socket
import time

addresses = [
"""
    for task in tasks:
      s += f"    ('{task['ipv6']}', {task['port']}),\n"
    s += """]

num_runs = """+str(args.num_runs)+"""
failed_tally = defaultdict(int)
for i in range(num_runs):
    num_succeeded = 0
    num_failed = 0
    print(f'Run #{i+1}')
    for address in addresses:
        try:
            sock = socket.create_connection(address, timeout=3)
            num_succeeded += 1
        except Exception as e:
            print(f'Connection failed: {address}')
            num_failed += 1
            failed_tally[address] += 1
    print(f'Connected: {num_succeeded}')
    print(f'Failed: {num_failed}')
    if i < num_runs - 1:
        time.sleep("""+str(args.num_secs_between_runs)+""")

print()
if failed_tally:
    print('Report:')
    for k, v in failed_tally.items():
        print(f'Task {k} failed {v}/"""+str(args.num_runs)+""" times.')
else:
    print('No failed connections.')
"""
    f.write(s)


def run_script_in_vm(num_tasks: int) -> None:
  """Run the script from the VM."""
  print(f'Copying script to VM {args.vm_name} ...')
  p = subprocess.run(
      [
          'gcloud', 'compute', 'scp', args.task_query_script_name,
          f'{args.vm_name}:~/', f'--zone={args.zone}',
      ],
      capture_output=True,
      text=True,
      check=False,
  )
  if p.returncode:
    print(f'scp script to {args.vm_name} failed. Exiting ...')
    return

  print(f'Running script in VM {args.vm_name} ...')
  print('Note: Some task addresses may have become outdated between the time')
  print('      the gqui command was run and this query script is being run.')
  print(f'Testing connection to {num_tasks} tasks in cell {args.cell} ...')
  print(f'Running script {args.num_runs} times, {args.num_secs_between_runs} '
        'seconds between runs ...')
  p = subprocess.Popen(
      [
          'gcloud', 'compute', 'ssh', args.vm_name, f'--zone={args.zone}',
          '--', f'python3 ~/{args.task_query_script_name}',
      ],
      stdout=subprocess.PIPE,
      text=True,
  )
  while (l := p.stdout.readline()):
    print(l.rstrip())


def main():
  global args
  parser = argparse.ArgumentParser(
      formatter_class=argparse.ArgumentDefaultsHelpFormatter,
  )
  parser.add_argument('--cell', required=True, help='borg cell')
  parser.add_argument('--cell_ipv6_prefix', required=True, metavar='PREFIX',
                      help='borg cell ipv6 prefix')
  parser.add_argument('--vm_name', default='dp-sc-octant-vm-01-do-not-delete',
                      help='VM name')
  parser.add_argument('--zone', default='us-central1-a', help='VM zone')
  parser.add_argument('--task_query_script_name', default='task-query.py',
                      metavar='NAME', help='script name to be run in VM')
  parser.add_argument('--num_runs', type=int, default=5, metavar='N',
                      help='Number of runs')
  parser.add_argument('--num_secs_between_runs', type=int, default=5,
                      metavar='N',
                      help='Number of seconds to wait between runs')
  args = parser.parse_args()

  p = subprocess.run(
      [
          'gcloud', 'config', 'get-value', 'project'
      ],
      capture_output=True,
      text=True,
      check=False,
  )
  project = p.stdout.rstrip()
  print(f'Current project: {project}')

  tasks = []
  page = 0
  while page_tasks := query_cell_tasks(page):
    tasks = tasks + page_tasks
    page += 1
  write_script_to_file(tasks)
  run_script_in_vm(len(tasks))


if __name__ == '__main__':
  main()
