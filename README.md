# luigi-blueprint
Create Luigi ETL Flows Without Any Code!

## What is Luigi Blueprint?
People that want to migrate their ETL to Luigi usually had some scripts glued together, migrating those scripts to Luigi requires to re-write all your scripts to Python, which requires coding effort and testing. Luigi Blueprint is a tool that uses the powers of Luigi to handle the queing and parallelization of the tasks but in the same time does not require to add any new code.

## How it works?
Luigi Blueprint uses ini configuration style files that define each individual task that will be executed and the flow of tasks execution as well, Luigi Blueprint is essential a wrapper for the following kind of ETL tasks.

* Execution of SQL Queries against Databases
* Execution of Local commands and scripts
* Execution of Remote commands and and scripts 

The above wrappers allows you to organize your existing SQL queries, commands and scripts to organized under a configuration file, achieving the same ETL flow as you had but with the additional functionality of effortless executing tasks in parallel (when is possible)
