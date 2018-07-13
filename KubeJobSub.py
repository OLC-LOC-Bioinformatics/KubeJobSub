#!/usr/bin/env python

import subprocess
import argparse
import shutil
# import yaml


def parse_kubectl_describe_nodes():
    """
    Kubectl describe nodes gives a lot of information that for the most part we don't really care about at all.
    This method will run that command and give slurm-esque output (number of nodes, node names, and current loads
    on each node)
    """
    kubectl_output = subprocess.check_output('kubectl describe nodes', shell=True)
    node_names = list()
    node_cpu_loads = dict()
    node_memory_loads = dict()
    node_cpu_capacity = dict()
    node_memory_capacity = dict()
    # TODO: Add something to check that nodes are actually up.

    # Iterate through the output created and populate our data dictionaries.
    # This way of doing things is definitely potentially very brittle - hopefully kubectl keeps output
    # looking consistent between versions.
    kubectl_lines = kubectl_output.decode('utf-8').split('\n')
    current_node_name = None
    for i in range(len(kubectl_lines)):
        if 'Name:' in kubectl_lines[i]:
            current_node_name = kubectl_lines[i].split()[1].rstrip()
            node_names.append(current_node_name)
        if current_node_name is not None:
            if 'cpu:' in kubectl_lines[i] and 'Capacity:' in kubectl_lines[i - 1]:
                node_cpu_capacity[current_node_name] = kubectl_lines[i].split()[1].rstrip()
            elif 'memory:' in kubectl_lines[i] and 'Capacity:' in kubectl_lines[i - 2]:
                node_memory_capacity[current_node_name] = kubectl_lines[i].split()[1].rstrip()
            elif 'cpu' in kubectl_lines[i] and 'Allocated' in kubectl_lines[i - 4]:
                node_cpu_loads[current_node_name] = kubectl_lines[i].split()[2].rstrip()
            elif 'memory' in kubectl_lines[i] and 'Allocated' in kubectl_lines[i - 5]:
                node_memory_loads[current_node_name] = kubectl_lines[i].split()[2].rstrip()

    # Now write the output of data gathered in a user-friendly(ish) format.
    print('Number of nodes in cluster: {}'.format(len(node_names)))
    print('NodeName\tCPU_Capacity\tCPU_Usage\tMemory_Capacity\tMemory_Usage')
    for node_name in node_names:
        print('{name}\t{cpucapacity}\t{cpuusage}\t{memorycapacity}\t{memoryusage}'.format(name=node_name,
                                                                                          cpucapacity=node_cpu_capacity[node_name],
                                                                                          cpuusage=node_cpu_loads[node_name],
                                                                                          memorycapacity=node_memory_capacity[node_name],
                                                                                          memoryusage=node_memory_loads[node_name]))


if __name__ == '__main__':
    # Argparse - should probably have subcommands here - main one for submitting jobs, others
    # for getting cluster info and whatnot.
    parser = argparse.ArgumentParser(description='KubeJobSub')
    subparsers = parser.add_subparsers(help='SubCommand Help', dest='subparsers')
    job_sub_parser = subparsers.add_parser('submit', help='Submits a job to your kubernetes cluster.')
    job_sub_parser.add_argument('-j', '--job_name',
                                type=str,
                                required=True,
                                help='Name of job.')
    job_sub_parser.add_argument('-n', '--num_cpu',
                                type=int,
                                default=1,
                                help='Number of CPUs to request for your job. Must be an integer greater than 0.'
                                     ' Defaults to 1.')
    job_sub_parser.add_argument('-m', '--memory',
                                type=float,
                                default=2,
                                help='Amount of memory to request, in GB.')
    job_sub_parser.add_argument('-v', '--volume',
                                type=str,
                                default='',
                                help='I do not know how I will implement this yet. TBD.')
    job_sub_parser.add_argument('-i', '--image',
                                type=str,
                                required=True,
                                help='Docker image to create container from.')
    info_parser = subparsers.add_parser('info', help='Tells you things about your kubernetes cluster.')
    args = parser.parse_args()

    if shutil.which('kubectl') is None:
        print('ERROR: kubectl not found. Please verify that kubernetes is installed.')
        # quit()
    info_dict = {'apiVersion': 'batch/v1',
                 'kind': 'Job',
                 'metadata': {'name': 'asdf'}}
    # print(yaml.dump(info_dict, default_flow_style=False))

    if 'submit' == args.subparsers:
        print('SUBMIT')
        # Submit a kubejob here.
    elif 'info' == args.subparsers:
        parse_kubectl_describe_nodes()
