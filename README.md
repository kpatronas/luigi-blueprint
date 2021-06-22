# luigi-blueprint
Create Luigi ETL Flows Without Any Code!

## What is Luigi Blueprint?
People that want to migrate their ETL to Luigi usually had some scripts glued together, migrating those scripts to Luigi requires to re-write all your scripts to Python, which requires coding effort and testing. Luigi Blueprint is a tool that uses the powers of Luigi to handle the queing and parallelization of the tasks but in the same time does not require to add any new code.

## How it works?
Luigi Blueprint uses ini configuration style files that define each individual task that will be executed and the flow of tasks execution as well, Luigi Blueprint is essential a wrapper for the following kind of ETL tasks.

* Execution of SQL Queries against Databases
* Execution of Local commands and scripts
* Execution of Remote commands and and scripts 

The above wrappers allows you to organize your existing SQL queries, commands and scripts to organized under a configuration file, achieving the same ETL flow as you had using a traditional shell script but with the additional functionality of effortless executing tasks in parallel (when is possible)

## Examples
### Executing a single local task
Create directory single_local_task
```
mkdir single_local_task
```
Save the following file as: single_exec.cfg
```
[BUILD]
TASKS: [LOCAL_TASK1()]
WORKERS:8
LOCAL_SCHEDULER:True

[LOCAL_TASK1()]
TYPE:LOCAL_TASK
COMMAND: ls -ltrh
SUCCESS_EXIT_CODE: 0
RESULTS: ./single_local_task/LOCAL_TASK1.txt
CLEANUP: True
```
Execute the blueprint
```
./blue.py -b single_local_task.cfg
WARN - Task "LOCAL_TASK1()" Has no "REQUIRES" parameter, creating.
WARN - Task "LOCAL_TASK1()" "REQUIRES" parameter is empty, defaulting to "[]"
INFO - Task: "LOCAL_TASK1" of Type: "LOCAL_TASK" Previous result: "./single_local_task/LOCAL_TASK1.txt" Deleted.
INFO - Task: "LOCAL_TASK1" of Type: "LOCAL_TASK" Created.
INFO - Task: LOCAL_TASK1 - Starting Execution.
INFO - Task: LOCAL_TASK1 - Succedeed with exit code: 0 check ./single_local_task/LOCAL_TASK1.txt.
INFO - END.
```
Explaination of single_exec.cfg parameters
```
[BUILD]                  <--- This is a mandatory section in all Blueprint configurations
TASKS: [LOCAL_TASK1()]   <--- The tasks that will be executed
WORKERS:8                <--- Maximum number of parallel tasks
LOCAL_SCHEDULER:True     <--- More in this later

[LOCAL_TASK1()]                               <--- The name of the task
TYPE:LOCAL_TASK                               <--- Type of the task, LOCAL_TASK is a task that will be executed localhost
COMMAND: ls -ltrh                             <--- The command/script to be executed.
SUCCESS_EXIT_CODE: 0                          <--- if the exit code of the command/script equals SUCCESS_EXIT_CODE then task is successfull 
RESULTS: ./single_local_task/LOCAL_TASK1.txt  <--- Where to write stdout/stderr
CLEANUP: True                                 <--- if previous run "RESULTS" file exists will be deleted if 'True', setting this to 'False' will not delete it.
```
