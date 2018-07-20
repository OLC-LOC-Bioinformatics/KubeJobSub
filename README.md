[![PyPI version](https://badge.fury.io/py/KubeJobSub.svg)](https://badge.fury.io/py/KubeJobSub)

# KubeJobSub/AzureStorage

This repository contains a Python package for two purposes - submitting jobs to a kubernetes cluster, and manipulating
azure file storage with a bash-like interface.

## KubeJobSub

Writing YAML files for submitting jobs to Kubernetes (or for anything else, for that matter) is the opposite of fun.

This makes that process much easier, although very specific to my own workflows.

### Installation

Use pip to install:

`pip install KubeJobSub`

This should take care of everything for you. Installing in a virtualenv is recommended. Script is Python3 only - if you
try to run using Python2 you will get an error.

### Usage

KubeJobSub can do two things (more may come in the future) - submit jobs to a kubernetes cluster, and get some info about
the cluster you're using.

```
usage: KubeJobSub [-h] {submit,info} ...

KubeJobSub

positional arguments:
  {submit,info}  SubCommand Help
    submit       Submits a job to your kubernetes cluster. Configured to
                 assume you're using azure with a file mount.
    info         Tells you about your kubernetes cluster - number of nodes,and
                 specs/usage for each node.
```

The `submit` subcommand makes the assumption that you're using an Azure file share and want to mount it as a volume.
Support for other things might get added eventually, depending on needs. It will write a YAML file for you
, submit a job to Kubernetes based on that file, and then clean it up. It will do some checks for you to make sure your
job actually gets submitted - it ensures no other jobs have the same name as what you've specified, as well as making sure
that you haven't requested more CPU/Memory than a node in your cluster has.

For example, the following command would do some read trimming for you, and store the results in the mounted file share.
This assumes the FASTQ files you want to trim are in the root of your file share:

```
KubeJobSub submit -j trim -c "bbduk.sh in=/mnt/azure/2014-SEQ-0276_S2_L001_R1_001.fastq.gz in2=/mnt/azure/2014-SEQ-0276_S2_L001_R2_001.fastq.gz out=/mnt/azure/trimmed_R1.fastq.gz out2=/mnt/azure/trimmed_R2.fastq.gz ref=adapters trimq=10 qtrim=w minlength=50" -i cathrine98/bbmap -share my_share_name -n 3 -m 4
```

Full usage options for the `submit` subcommand are below.


```
usage: KubeJobSub submit [-h] -j JOB_NAME -c COMMAND -i IMAGE [-n NUM_CPU]
                            [-m MEMORY] [-v VOLUME] -share SHARE
                            [-secret SECRET] [-k]

optional arguments:
  -h, --help            show this help message and exit
  -j JOB_NAME, --job_name JOB_NAME
                        Name of job.
  -c COMMAND, --command COMMAND
                        The command you want to run. Put it in double quotes.
                        (")
  -i IMAGE, --image IMAGE
                        Docker image to create container from.
  -n NUM_CPU, --num_cpu NUM_CPU
                        Number of CPUs to request for your job. Must be an
                        integer greater than 0. Defaults to 1.
  -m MEMORY, --memory MEMORY
                        Amount of memory to request, in GB. Defaults to 2.
  -v VOLUME, --volume VOLUME
                        The mountpath for your azure file share. Defaults to
                        /mnt/azure
  -share SHARE, --share SHARE
                        Name of Azure file share that you want mounted to the
                        point specified by -v
  -secret SECRET, --secret SECRET
                        The name of the secret created by kubectl for azure
                        file mounting. Defaults to azure-secret. See
                        https://docs.microsoft.com/en-us/azure/aks/azure-
                        files-volume for more information on creating your
                        own.
  -k, --keep            A YAML file will be created to submit your job.
                        Deleted by default once job is submitted, but if this
                        flag is active it will be kept.

```

The info subcommand has no options - just run `KubeJobSub info`. You should see something like:

```
Number of nodes in cluster: 2
NodeName	CPU_Capacity	CPU_Usage	Memory_Capacity	Memory_Usage
aks-nodepool1-25823294-2	4	(28%)	8145492Ki	(26%)
aks-nodepool1-25823294-3	4	(41%)	8145492Ki	(49%)
```

## AzureStorage

I've found Azure File shares to be a bit of a pain to manipulate with Azure's tools, so this tool provides a more bash-esque
interface for manipulating/uploading/downloading files. There are lots of things you can't do yet, and probably lots
of bugs.

### Installation

Also part of the KubeJobSub package, so use pip to install:

`pip install KubeJobSub`

### Usage

The following commands are currently available - each command has it's own help menu that can be accessed with `-h`

```
usage: AzureStorage [-h] {set_credentials,ls,mkdir,upload,download,rm} ...

StorageWrapper: Using azure file shares is kind of a pain.This wraps a bunch
of Azure CLI file share commands into a more linux-esque environment.

positional arguments:
  {set_credentials,ls,mkdir,upload,download,rm}
                        SubCommand Help
    set_credentials     Sets the azure file share and account key as
                        environment variables.
    ls                  Lists files in a directory. Wildcard (*) can be used,
                        but only in final part of expression. (you can ls
                        foo/bar*.py, but not foo*/bar.py)
    mkdir               Makes a directory.
    upload              Uploads a file to azure file storage. Can usewildcard
                        to upload multiple files.
    download            Downloads one or more files from cloud to your
                        machine.
    rm                  Deletes a file. Can be run recursively to delete
                        entire directories with the -r flag.

optional arguments:
  -h, --help            show this help message and exit

```

### Examples

Note that if your credentials haven't been set, you'll be asked to set them. They'll be remembered, and can be changed
(in the event you want to use a different storage account or share) using the `set_credentials` subcommand.

List files in root dir:

`AzureStorage ls`

List all python files in directory `scripts`:

`AzureStorage ls scripts/*.py`

Make a directory called `new-dir` in root directory:

`AzureStorage mkdir new-dir`

Upload all `.py` files in your current directory to `new-dir` in Azure File Storage:

`AzureStorage upload *.py -p new-dir`

Upload a folder called `folder` and all of its subfolders to root in Azure:

`AzureStorage upload -r folder`

Remove all the `.py` files in new-dir:

`AzureStorage rm new-dir/*.py`

Remove a folder called `example` and all of its subfolders and files:

`AzureStorage rm -r example`

Download a file called `file.txt` from directory `dir` to your current working directory:

`AzureStorage download dir/file.txt`

Download folder `folder` and all of its subfolders from root of Azure to directory `foo` on your machine:

`AzureStorage download -r folder foo`
