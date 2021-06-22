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
### Executing a single local task that will fail intentionally
Save the following as single_local_task_fails.cfg, COMMAND: BORN2FAIL probably does not exist in any system.
```
[BUILD]
TASKS: [LOCAL_TASK1()]
WORKERS:8
LOCAL_SCHEDULER:True

[LOCAL_TASK1()]
TYPE:LOCAL_TASK
COMMAND: BORN2FAIL
SUCCESS_EXIT_CODE: 0
RESULTS: ./single_local_task/LOCAL_TASK1.txt
CLEANUP: True
```
Execute the blueprint
```
./blue.py -b single_local_task_fails.cfg
WARN - Task "LOCAL_TASK1()" Has no "REQUIRES" parameter, creating.
WARN - Task "LOCAL_TASK1()" "REQUIRES" parameter is empty, defaulting to "[]"
INFO - Task: "LOCAL_TASK1" of Type: "LOCAL_TASK" Created.
INFO - Task: LOCAL_TASK1 - Starting Execution.
ERROR - Task: LOCAL_TASK1 - Failed with exit code: 127 check ./single_local_task/LOCAL_TASK1.txt.stderr.
INFO - END.
```
Explaination of the output
```
WARN - Task "LOCAL_TASK1()" Has no "REQUIRES" parameter, creating.
WARN - Task "LOCAL_TASK1()" "REQUIRES" parameter is empty, defaulting to "[]"
INFO - Task: "LOCAL_TASK1" of Type: "LOCAL_TASK" Created.
INFO - Task: LOCAL_TASK1 - Starting Execution.
ERROR - Task: LOCAL_TASK1 - Failed with exit code: 127 check ./single_local_task/LOCAL_TASK1.txt.stderr. <--- This command failed with exit status 127 and the stderr is saved
INFO - END.
```
The contents of ./single_local_task/LOCAL_TASK1.txt.stderr are
```
/bin/sh: 1: BORN2FAIL: not found
```
### Executing two local tasks in parallel
Create directory two_local_parallel_tasks
```
mkdir two_local_parallel_tasks
```
Save the following file as: two_exec.cfg
```
[BUILD]
TASKS: [LOCAL_TASK1(),LOCAL_TASK2()]
WORKERS:8
LOCAL_SCHEDULER:True

[LOCAL_TASK1()]
TYPE:LOCAL_TASK
COMMAND: ls -ltrh
SUCCESS_EXIT_CODE: 0
RESULTS: ./two_local_parallel_tasks/LOCAL_TASK1.txt
CLEANUP: True

[LOCAL_TASK2()]
TYPE:LOCAL_TASK
COMMAND: df -h
SUCCESS_EXIT_CODE: 0
RESULTS: ./two_local_parallel_tasks/LOCAL_TASK2.txt
CLEANUP: True
```
Execute the blueprint
```
./blue.py -b two_exec.cfg
WARN - Task "LOCAL_TASK1()" Has no "REQUIRES" parameter, creating.
WARN - Task "LOCAL_TASK1()" "REQUIRES" parameter is empty, defaulting to "[]"
WARN - Task "LOCAL_TASK2()" Has no "REQUIRES" parameter, creating.
WARN - Task "LOCAL_TASK2()" "REQUIRES" parameter is empty, defaulting to "[]"
INFO - Task: "LOCAL_TASK1" of Type: "LOCAL_TASK" Previous result: "./two_local_parallel_tasks/LOCAL_TASK1.txt" Deleted.
INFO - Task: "LOCAL_TASK1" of Type: "LOCAL_TASK" Created.
INFO - Task: "LOCAL_TASK2" of Type: "LOCAL_TASK" Previous result: "./two_local_parallel_tasks/LOCAL_TASK2.txt" Deleted.
INFO - Task: "LOCAL_TASK2" of Type: "LOCAL_TASK" Created.
INFO - Task: LOCAL_TASK1 - Starting Execution.
INFO - Task: LOCAL_TASK2 - Starting Execution.
INFO - Task: LOCAL_TASK1 - Succedeed with exit code: 0 check ./two_local_parallel_tasks/LOCAL_TASK1.txt.
INFO - Task: LOCAL_TASK2 - Succedeed with exit code: 0 check ./two_local_parallel_tasks/LOCAL_TASK2.txt.
INFO - END
```
Explaination of the configuration
```
TASKS: [LOCAL_TASK1(),LOCAL_TASK2()] <--- Those two tasks will be executed on parallel.
```
This kind of execution does not ensure the order of execution which can be crucial in the case that the execution of one task is depended on the execution of another.

### Executing three local tasks, one is depended by the successfull execution of the two
Create directory three_local_tasks
```
mkdir three_local_tasks
```
Save the following file as: three_local_tasks.cfg
```
[BUILD]
TASKS: [LOCAL_TASK3()]
WORKERS:8
LOCAL_SCHEDULER:True

[LOCAL_TASK3()]
TYPE:LOCAL_TASK
COMMAND: uptime
SUCCESS_EXIT_CODE: 0
REQUIRES: [LOCAL_TASK1(),LOCAL_TASK2()]
RESULTS: ./three_local_tasks/LOCAL_TASK3.txt
CLEANUP: True

[LOCAL_TASK1()]
TYPE:LOCAL_TASK
COMMAND: ls -ltrh
SUCCESS_EXIT_CODE: 0
RESULTS: ./three_local_tasks/LOCAL_TASK1.txt
CLEANUP: True

[LOCAL_TASK2()]
TYPE:LOCAL_TASK
COMMAND: df -h
SUCCESS_EXIT_CODE: 0
RESULTS: ./three_local_tasks/LOCAL_TASK2.txt
CLEANUP: True
```
Execute the blueprint
```
./blue.py -b three_local_tasks.cfg
WARN - Task "LOCAL_TASK1()" Has no "REQUIRES" parameter, creating.
WARN - Task "LOCAL_TASK1()" "REQUIRES" parameter is empty, defaulting to "[]"
WARN - Task "LOCAL_TASK2()" Has no "REQUIRES" parameter, creating.
WARN - Task "LOCAL_TASK2()" "REQUIRES" parameter is empty, defaulting to "[]"
INFO - Task: "LOCAL_TASK3" of Type: "LOCAL_TASK" Created.
INFO - Task: "LOCAL_TASK1" of Type: "LOCAL_TASK" Created.
INFO - Task: "LOCAL_TASK2" of Type: "LOCAL_TASK" Created.
INFO - Task: LOCAL_TASK2 - Starting Execution.
INFO - Task: LOCAL_TASK1 - Starting Execution.
INFO - Task: LOCAL_TASK2 - Succedeed with exit code: 0 check ./three_local_tasks/LOCAL_TASK2.txt.
INFO - Task: LOCAL_TASK1 - Succedeed with exit code: 0 check ./three_local_tasks/LOCAL_TASK1.txt.
INFO - Task: LOCAL_TASK3 - Starting Execution.
INFO - Task: LOCAL_TASK3 - Succedeed with exit code: 0 check ./three_local_tasks/LOCAL_TASK3.txt.
INFO - END.
```
Explaination of the configuration
```
[LOCAL_TASK3()]
TYPE:LOCAL_TASK
COMMAND: uptime
SUCCESS_EXIT_CODE: 0
REQUIRES: [LOCAL_TASK1(),LOCAL_TASK2()]  <--- we have added the REQUIRES parameter, this parameter defines the tasks that MUST successfully executed before executing this one
RESULTS: ./three_local_tasks/LOCAL_TASK3.txt
CLEANUP: True
```
### CLEANUP parameter and executing a task, how it works
As we said before the CLEANUP parameter deletes the previous RESULTS file if set to 'True', but you must be aware that at task which its previous RESULTS file is not deleted the task will not be executed! this might sound odd, but its very helpfull in cases which a long running blueprint has been failed and needs to re-run but its not needed to run all tasks, which might be time consuming.
Do the following modification in the previous example, set the CLEANUP parameter of task LOCAL_TASK1() to False
```
[LOCAL_TASK1()]
TYPE:LOCAL_TASK
COMMAND: ls -ltrh
SUCCESS_EXIT_CODE: 0
RESULTS: ./three_local_tasks/LOCAL_TASK1.txt
CLEANUP: False
```
Execute the blueprint
```
./blue.py -b three_local_tasks.cfg
WARN - Task "LOCAL_TASK1()" Has no "REQUIRES" parameter, creating.
WARN - Task "LOCAL_TASK1()" "REQUIRES" parameter is empty, defaulting to "[]"
WARN - Task "LOCAL_TASK2()" Has no "REQUIRES" parameter, creating.
WARN - Task "LOCAL_TASK2()" "REQUIRES" parameter is empty, defaulting to "[]"
INFO - Task: "LOCAL_TASK3" of Type: "LOCAL_TASK" Previous result: "./three_local_tasks/LOCAL_TASK3.txt" Deleted.
INFO - Task: "LOCAL_TASK3" of Type: "LOCAL_TASK" Created.
INFO - Task: "LOCAL_TASK1" of Type: "LOCAL_TASK" Previous result: "./three_local_tasks/LOCAL_TASK1.txt" Not Deleted.
INFO - Task: "LOCAL_TASK1" of Type: "LOCAL_TASK" Created But Will Not Run Because Previous Result Exists.
INFO - Task: "LOCAL_TASK2" of Type: "LOCAL_TASK" Previous result: "./three_local_tasks/LOCAL_TASK2.txt" Deleted.
INFO - Task: "LOCAL_TASK2" of Type: "LOCAL_TASK" Created.
INFO - Task: LOCAL_TASK2 - Starting Execution.
INFO - Task: LOCAL_TASK2 - Succedeed with exit code: 0 check ./three_local_tasks/LOCAL_TASK2.txt.
INFO - Task: LOCAL_TASK3 - Starting Execution.
INFO - Task: LOCAL_TASK3 - Succedeed with exit code: 0 check ./three_local_tasks/LOCAL_TASK3.txt.
INFO - END.
```
Explaination of the output
```
INFO - Task: "LOCAL_TASK1" of Type: "LOCAL_TASK" Previous result: "./three_local_tasks/LOCAL_TASK1.txt" Not Deleted. <--- CLEANUP: False means that the RESULTS file will not be deleted
INFO - Task: "LOCAL_TASK1" of Type: "LOCAL_TASK" Created But Will Not Run Because Previous Result Exists. <--- Because previous RESULTS file exists, task will not be executed.
```
### Executing a single remote task
Luigi Blueprint can execute tasks over SSH as well
Create directory remote_task1
```
mkdir remote_task1
```
Save the following file as single_remote_task.cfg
```
[BUILD]
TASKS: [REMOTE_TASK_1()]
WORKERS:8
LOCAL_SCHEDULER:True

[MYPASSWORD]
TYPE: CREDS
USER: kpatronas
PASS: mypassword
KEY: /home/kpatronas/.ssh/id_rsa.pub

[REMOTE_TASK_1()]
RESULTS: ./remote_task1/ouput.txt
SUCCESS_EXIT_CODE: 0
TYPE:REMOTE_TASK
COMMAND: /bin/ls -la
CLEANUP: True
HOST: 127.0.0.1
CREDS: MYPASSWORD
```
Execute the blueprint
```
./blue.py -b ./single_remote_task.cfg
WARN - Task "REMOTE_TASK_1()" Has no "PORT" parameter, creating.
WARN - Task "REMOTE_TASK_1()" "PORT" parameter is empty, defaulting to "22"
WARN - Task "REMOTE_TASK_1()" Has no "TIMEOUT" parameter, creating.
WARN - Task "REMOTE_TASK_1()" "TIMEOUT" parameter is empty, defaulting to "10"
WARN - Task "REMOTE_TASK_1()" Has no "REQUIRES" parameter, creating.
WARN - Task "REMOTE_TASK_1()" "REQUIRES" parameter is empty, defaulting to "[]"
WARN - Task "REMOTE_TASK_1()" Has no "USE_PROXY" parameter, creating.
WARN - Task "REMOTE_TASK_1()" "USE_PROXY" parameter is empty, defaulting to "False"
INFO - Task: "REMOTE_TASK_1" of Type: "REMOTE_TASK" Created.
INFO - Task: "REMOTE_TASK_1" - SSH Connect to host: "127.0.0.1".
INFO - Task: REMOTE_TASK_1 - Succedeed with exit code: 0 check ./remote_task1/ouput.txt.
INFO - END.
```
Explaination of the parameters
```
[MYPASSWORD]     <--- This is a credentials session
TYPE: CREDS      <--- Has always a type of CREDS
USER: kpatronas  <--- The username
PASS: mypassword <--- The password
KEY: /home/kpatronas/.ssh/id_rsa.pub <--- The private key, note that password or key are optional but, one of them must exist

[REMOTE_TASK_1()]
RESULTS: ./remote_task1/ouput.txt
SUCCESS_EXIT_CODE: 0
TYPE:REMOTE_TASK     <--- REMOTE_TASK is the type of tasks over SSH
COMMAND: /bin/ls -la
CLEANUP: True
HOST: 127.0.0.1   <--- host to execute COMMAND over SSH
CREDS: MYPASSWORD <--- What credentials to use
```
### Executing a remote task over an SSH proxy
There are cases that the server might be reachable only over an SSH proxy, blueprint supports SSH proxies
Do the following changes to the previous configuration file
```
[BUILD]
TASKS: [REMOTE_TASK_1()]
WORKERS:8
LOCAL_SCHEDULER:True

[GW1()]
TYPE: SSH_PROXY
HOST: 127.0.0.1
PORT: 22
CREDS: MYPASSWORD

[MYPASSWORD]
TYPE: CREDS
USER: kpatronas
PASS: mypassword
KEY: /home/kpatronas/.ssh/id_rsa.pub

[REMOTE_TASK_1()]
RESULTS: ./remote_task1/ouput.txt
SUCCESS_EXIT_CODE: 0
TYPE:REMOTE_TASK
COMMAND: /bin/ls -la
CLEANUP: True
HOST: 127.0.0.1
CREDS: MYPASSWORD
USE_PROXY: True
PROXY: GW1()
```
Execute the blueprint
```
./blue.py -b ./single_remote_task.cfg
WARN - Task "REMOTE_TASK_1()" Has no "PORT" parameter, creating.
WARN - Task "REMOTE_TASK_1()" "PORT" parameter is empty, defaulting to "22"
WARN - Task "REMOTE_TASK_1()" Has no "TIMEOUT" parameter, creating.
WARN - Task "REMOTE_TASK_1()" "TIMEOUT" parameter is empty, defaulting to "10"
WARN - Task "REMOTE_TASK_1()" Has no "REQUIRES" parameter, creating.
WARN - Task "REMOTE_TASK_1()" "REQUIRES" parameter is empty, defaulting to "[]"
INFO - Task: "REMOTE_TASK_1" of Type: "REMOTE_TASK" Previous result: "./remote_task1/ouput.txt" Deleted.
INFO - Task: "REMOTE_TASK_1" of Type: "REMOTE_TASK" Created.
INFO - Task: "REMOTE_TASK_1" - SSH Connect to Proxy: "GW1()".
INFO - Task: "REMOTE_TASK_1" - SSH Connect to host: "127.0.0.1".
INFO - Task: "REMOTE_TASK_1" - host: "127.0.0.1 Executing".
INFO - Task: REMOTE_TASK_1 - Succedeed with exit code: 0 check ./remote_task1/ouput.txt.
INFO - END.
```
Explaintion of the blueprint
```
[GW1()]
TYPE: SSH_PROXY  <--- This is the SSH Proxy configuration section
HOST: 127.0.0.1
PORT: 22
CREDS: MYPASSWORD

[MYPASSWORD]
TYPE: CREDS
USER: kpatronas
PASS: mypassword
KEY: /home/kpatronas/.ssh/id_rsa.pub

[REMOTE_TASK_1()]
RESULTS: ./remote_task1/ouput.txt
SUCCESS_EXIT_CODE: 0
TYPE:REMOTE_TASK
COMMAND: /bin/ls -la
CLEANUP: True
HOST: 127.0.0.1
CREDS: MYPASSWORD
USE_PROXY: True  <--- If True will use whatever PROXY is set bellow
PROXY: GW1()
```
### Executing a database task
To execute this example you will need a database server, in my case i have a maria db installed on localhost
Save the following file as db_remote.cfg, the file that contains the query that i want to execute is data.sql
```
[BUILD]
TASKS: [DB1()]
WORKERS:8
LOCAL_SCHEDULER:True

[MARIADB1]
TYPE:   DB_CONF
ENGINE: mysql
CREDS:  DB1_CREDS
DBHOST: 127.0.0.1
DBNAME: TEST
DBPORT: 3306

[DB1_CREDS]
TYPE: CREDS
USER: test_user
PASS: mypass1

[DB1()]
RESULTS:      data.csv
CLEANUP:      True
TYPE:         DB_TASK
RESULTS_TYPE: csv
QUERY:        data.sql
DB:           MARIADB1
```
Executing the blueprint
```
./blue.py -b db_blueprint.cfg
WARN - Task "DB1_CREDS" Has no "KEY" parameter, creating.
WARN - Task "DB1_CREDS" "KEY" parameter is empty, defaulting to "False"
WARN - Task "DB1()" Has no "REQUIRES" parameter, creating.
WARN - Task "DB1()" "REQUIRES" parameter is empty, defaulting to "[]"
INFO - Task: "DB1" of Type: "DB_TASK" Previous result: "data.csv" Deleted.
INFO - Task: "DB1" of Type: "DB_TASK" Created.
INFO - Task: DB1 - host: 127.0.0.1 Preparing to execute  data.sql
INFO - Executing task: "DB1()" .
INFO - END.
```
Explaination of the parameters
```
[MARIADB1]      <--- This is a DB_CONF section
TYPE:   DB_CONF
ENGINE: mysql     <--- The DB engine to use. it must be the same as the libary of SQLAlchemy 
CREDS:  DB1_CREDS <-- credentials to use
DBHOST: 127.0.0.1 <-- Database host
DBNAME: TEST      <-- Database name
DBPORT: 3306      <-- Database port

[DB1_CREDS]
TYPE: CREDS
USER: test_user
PASS: mypass1

[DB1()]
RESULTS:      data.csv
CLEANUP:      True
TYPE:         DB_TASK  <-- Task is DB_TASK
RESULTS_TYPE: csv      <-- The output will be csv, it could be xlsx also
QUERY:        data.sql <-- the query to execute
DB:           MARIADB1 <-- The database to connect
```
### Executing a DB task over an SSH proxy
Change the previous file to
```
[BUILD]
TASKS: [DB1()]
WORKERS:8
LOCAL_SCHEDULER:True

[MYPASSWORD]
TYPE: CREDS
USER: kpatronas
PASS: mypassword
KEY: /home/kpatronas/.ssh/id_rsa.pub

[GW1()]
TYPE: SSH_PROXY
HOST: 127.0.0.1
PORT: 22
CREDS: MYPASSWORD

[MARIADB1]
TYPE:   DB_CONF
ENGINE: mysql
CREDS:  DB1_CREDS
DBHOST: 127.0.0.1
DBNAME: TEST
DBPORT: 3306

[DB1_CREDS]
TYPE: CREDS
USER: test_user
PASS: mypass1

[DB1()]
RESULTS:      data.csv
CLEANUP:      True
TYPE:         DB_TASK
RESULTS_TYPE: csv
QUERY:        data.sql
DB:           MARIADB1
USE_PROXY:    True
PROXY:        GW1()
```
Explaination of the parameters
```
[DB1()]
RESULTS:      data.csv
CLEANUP:      True
TYPE:         DB_TASK
RESULTS_TYPE: csv
QUERY:        data.sql
DB:           MARIADB1
USE_PROXY:    True  <-- If True use a proxy
PROXY:        GW1() <-- Which Proxy to create an SSH tunnel
```
Executing the query
```
WARN - Task "DB1_CREDS" Has no "KEY" parameter, creating.
WARN - Task "DB1_CREDS" "KEY" parameter is empty, defaulting to "False"
WARN - Task "DB1()" Has no "REQUIRES" parameter, creating.
WARN - Task "DB1()" "REQUIRES" parameter is empty, defaulting to "[]"
INFO - Task: "DB1" of Type: "DB_TASK" Previous result: "data.csv" Deleted.
INFO - Task: "DB1" of Type: "DB_TASK" Created.
INFO - Task: DB1 - host: 127.0.0.1 Preparing to execute  data.sql
INFO - Connect to Proxy: "127.0.0.1".
INFO - Executing task: "DB1()" .
INFO - END.
```
### Combine a local task, a remote task and a DB task
In this example the following are done
LOCAL_TASK_1() Starts executing but... REMOTE_TASK_1() needs to be executed first... BUT... DB1() needs to be executed first
So the execution starts from DB1 which does a query and stores results to a CSV file
Then REMOTE_TASK_1 is executed with this command "cat /home/kpatronas/examples/data.csv | grep -v counter" and stdout is saved to /home/kpatronas/examples/data1.csv
And finally LOCAL_TASK_1() is executed which just counts the number of lines of the previous generated file and stdout is saved to /home/kpatronas/examples/sum.txt

Save the following file as full_test.cfg
```
[BUILD]
TASKS: [LOCAL_TASK_1()]
WORKERS:8
LOCAL_SCHEDULER:True

[MYPASSWORD]
TYPE: CREDS
USER: kpatronas
PASS: mypassword
KEY: /home/kpatronas/.ssh/id_rsa.pub

[GW1()]
TYPE: SSH_PROXY
HOST: 127.0.0.1
PORT: 22
CREDS: MYPASSWORD

[MARIADB1]
TYPE:   DB_CONF
ENGINE: mysql
CREDS:  DB1_CREDS
DBHOST: 127.0.0.1
DBNAME: TEST
DBPORT: 3306

[DB1_CREDS]
TYPE: CREDS
USER: test_user
PASS: mypass1

[DB1()]
RESULTS:      data.csv
CLEANUP:      True
TYPE:         DB_TASK
RESULTS_TYPE: csv
QUERY:        data.sql
DB:           MARIADB1
USE_PROXY:    True
PROXY:        GW1()

[REMOTE_TASK_1()]
RESULTS: /home/kpatronas/examples/data1.csv
SUCCESS_EXIT_CODE: 0
REQUIRES: [DB1()]
TYPE:REMOTE_TASK
COMMAND: cat /home/kpatronas/examples/data.csv | grep -v counter
CLEANUP: True
HOST: 127.0.0.1
CREDS: MYPASSWORD
USE_PROXY: False
PROXY: GW1()

[LOCAL_TASK_1()]
RESULTS: /home/kpatronas/examples/sum.txt
REQUIRES: [REMOTE_TASK_1()]
SUCCESS_EXIT_CODE: 0
TYPE:LOCAL_TASK
COMMAND: cat /home/kpatronas/examples/data1.csv | wc -l
CLEANUP: True
```
Output:
```
./blue.py -b db_blueprint.cfg
WARN - Task "REMOTE_TASK_1()" Has no "PORT" parameter, creating.
WARN - Task "REMOTE_TASK_1()" "PORT" parameter is empty, defaulting to "22"
WARN - Task "REMOTE_TASK_1()" Has no "TIMEOUT" parameter, creating.
WARN - Task "REMOTE_TASK_1()" "TIMEOUT" parameter is empty, defaulting to "10"
WARN - Task "DB1_CREDS" Has no "KEY" parameter, creating.
WARN - Task "DB1_CREDS" "KEY" parameter is empty, defaulting to "False"
WARN - Task "DB1()" Has no "REQUIRES" parameter, creating.
WARN - Task "DB1()" "REQUIRES" parameter is empty, defaulting to "[]"
INFO - Task: "DB1" of Type: "DB_TASK" Previous result: "data.csv" Deleted.
INFO - Task: "DB1" of Type: "DB_TASK" Created.
INFO - Task: "REMOTE_TASK_1" of Type: "REMOTE_TASK" Previous result: "/home/kpatronas/examples/data1.csv" Deleted.
INFO - Task: "REMOTE_TASK_1" of Type: "REMOTE_TASK" Created.
INFO - Task: "LOCAL_TASK_1" of Type: "LOCAL_TASK" Previous result: "/home/kpatronas/examples/sum.txt" Deleted.
INFO - Task: "LOCAL_TASK_1" of Type: "LOCAL_TASK" Created.
INFO - Task: DB1 - host: 127.0.0.1 Preparing to execute  data.sql
INFO - Connect to Proxy: "127.0.0.1".
INFO - Executing task: "DB1()" .
INFO - Task: "REMOTE_TASK_1" - SSH Connect to host: "127.0.0.1".
INFO - Task: "REMOTE_TASK_1" - host: "127.0.0.1 Executing".
INFO - Task: REMOTE_TASK_1 - Succedeed with exit code: 0 check /home/kpatronas/examples/data1.csv.
INFO - Task: LOCAL_TASK_1 - Starting Execution.
INFO - Task: LOCAL_TASK_1 - Succedeed with exit code: 0 check /home/kpatronas/examples/sum.txt.
INFO - END.
```
