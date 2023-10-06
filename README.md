# Data Analysis Server

Services for executing data analysis tasks on multiple HPC nodes.

Consists of:

 - Server: One instance needed. Receives jobs and distributes them to workers
 - Worker: Multiple instances can be started on multiple HPC servers. Receive commands from Server, perform 
   analysis and return results to server for dispatch to clients.

Uses the SZRPC framework (https://github.com/michel4j/swift-rpc) for remote procedure calls.

## Usage:

Server:

```bash
usage: app.server [-h] [-v] [-p PORTS PORTS] [-s SIGNAL_THREADS] [-n INSTANCES]

Data Processing Server

options:
  -h, --help            show this help message and exit
  -v                    Verbose Logging
  -p PORTS PORTS, --ports PORTS PORTS
                        Ports
  -s SIGNAL_THREADS, --signal-threads SIGNAL_THREADS
                        Number of Signal threads per worker
  -n INSTANCES, --instances INSTANCES
                        Number of Worker instances
```

For example, to start a server with a front-end address of "tcp://myhpc0.example.com:9990" and a backend address of
"tcp://myhpc0.example.com:9991", run the following command on "myhpc0.example.com". This command will also start
a single worker (`-n 1`) with 32 signal threads (`-s 32`) on "myhpc0.example.com". Signal threads are using for 
signal-strength analysis of images on-the-fly during rastering.  


```bash
$ app.server -n 1 -s 32 -v
```

Worker:

```bash
usage: app.worker [-h] [-v] [-b BACKEND] [-s SIGNAL_THREADS] [-n INSTANCES]

Data Processing Worker

options:
  -h, --help            show this help message and exit
  -v                    Verbose Logging
  -b BACKEND, --backend BACKEND
                        Backend Address
  -s SIGNAL_THREADS, --signal-threads SIGNAL_THREADS
                        Number of Signal threads per worker
  -n INSTANCES, --instances INSTANCES
                        Number of Worker instances
```

For example, to start workers for the above server instance,run the following command on each distinct node of your hpc cluster.
```bash
$ app.worker -b "tcp://myhpc0.example.com:9991" -n 4 -s 32 -v
```
This will start four workers with 32 signal-threads each on this node. 

Persistent deployment of the Server and Worker can be performed in a Unit file or using ProcServ.
