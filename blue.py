#!/usr/bin/env python3
from multiprocessing import Process, Queue
from sqlalchemy import create_engine
from functools import wraps
import subprocess
import argparse
import pandas as pd
import luigi
import time
import sys
import os

from jumpssh import SSHSession


def processify(func):
    def process_func(q, *args, **kwargs):
        try:
            ret = func(*args, **kwargs)
        except Exception:
            ex_type, ex_value, tb = sys.exc_info()
            error = ex_type, ex_value, ''.join(traceback.format_tb(tb))
            ret = None
        else:
            error = None

        q.put((ret, error))

    process_func.__name__ = func.__name__ + 'processify_func'
    setattr(sys.modules[__name__], process_func.__name__, process_func)

    @wraps(func)
    def wrapper(*args, **kwargs):
        q = Queue()
        p = Process(target=process_func, args=[q] + list(args), kwargs=kwargs)
        p.start()
        ret, error = q.get()
        p.join()

        if error:
            ex_type, ex_value, tb_str = error
            print(error)
            message = '%s (in subprocess)\n%s' % (ex_value.message, tb_str)
            raise ex_type(message)

        return ret
    return wrapper

@processify
def q(db_query,engine):
    return pd.read_sql_query(db_query,engine)

def execQuery(db_options,db_query,proxy_options,use_proxy=False):
    '''
    '''
    # Get the database options
    db_engine    = db_options['db_engine']
    db_username  = db_options['db_username']
    db_password  = db_options['db_password']
    db_hostname  = db_options['db_hostname']
    db_name      = db_options['db_name']
    db_port      = int(db_options['db_port'])
    TASK_NAME    = db_options['task_name']
    if use_proxy == 'True':
            sys.stdout.write('INFO - Connect to Proxy: "%s".\n' %(proxy_options['ssh_proxy']))
            ssh_username = proxy_options['ssh_username']
            ssh_password = proxy_options['ssh_password']
            ssh_proxy    = proxy_options['ssh_proxy']
            ssh_port     = int(proxy_options['ssh_port'])
            # Create the SSH Tunnel
            try:
                from sshtunnel import SSHTunnelForwarder
                tunnel = SSHTunnelForwarder((ssh_proxy, ssh_port),ssh_username = ssh_username,ssh_password = ssh_password,remote_bind_address=(db_hostname, db_port),threaded = True )
                tunnel.start()
            except Exception as ex:
                sys.stderr.write('ERROR - Task "%s" Could not create SSH tunnel: "%s" - %s.\n'%(TASK_NAME,str(ex)))
                sys.exit()

            # Create the connection string and the engine
            conn_string = '%s://%s:%s@%s:%s/%s'%(db_engine,db_username,db_password,"localhost",int(tunnel.local_bind_port),db_name)
            engine  = create_engine(conn_string)
            # Execute query, stop ssh tunnel and return results
            try:
                sys.stdout.write('INFO - Executing task: "%s" .\n'%(TASK_NAME))
                results = q(db_query=db_query,engine=engine)
            except Exception as ex:
                sys.stderr.write('ERROR - Could not execute task: "%s" - %s.\n'%(TASK_NAME,str(ex)))
                sys.exit()
            tunnel.stop()
            return results
    else:
        # Create connection string and engine, execute query and return results
        conn_string = '%s://%s:%s@%s:%s/%s'%(db_engine,db_username,db_password,db_hostname,db_port,db_name)
        engine      = create_engine(conn_string)
        try:
            sys.stdout.write('INFO - Executing task: "%s" .\n'%(TASK_NAME))
            return pd.read_sql_query(db_query,engine)
        except Exception as ex:
            sys.stderr.write('ERROR - Could not execute task: "%s" - %s.\n'%(TASK_NAME,str(ex)))
            sys.exit()
            

def cmd_execute(cmd):
    '''
    Execute local tasks
    '''
    completed = subprocess.run(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,)
    return [int(completed.returncode), completed.stdout.decode('utf-8', 'ignore'), completed.stderr.decode('utf-8', 'ignore')]


def section_mgr(blueprint_configuration, section, section_param, verify, action):
    '''
    Sanity checking
    '''
    for v, a in zip(verify.split(','), action.split(',')):

        if 'range(' in v:
            ranges = v[v.find('(')+1:v.find(')')].split('-')
            if int(blueprint_configuration[section][section_param]) not in range(int(ranges[0]), int(ranges[1])):
                if a == "die":
                    sys.stderr.write('ERROR - Task "%s" "%s" parameter exceeds range(%s,%s)\n' %
                                     (section, section_param, ranges[0], ranges[1]))
                    sys.exit(-1)

        if "section_option_allowed_values" in v:
            values = v.split(':')[1].split('-')
            if blueprint_configuration[section][section_param] not in values:
                if a == "die":
                    sys.stderr.write(
                        'ERROR - Task "%s" "%s" parameter has not an allowed value "%s"\n' % (section, section_param, values))
                    sys.exit(-1)

        if v == "section_does_not_exist":
            if section_param not in blueprint_configuration[section]:
                if a == "die":
                    sys.stderr.write(
                        'ERROR - Task "%s" Has no "%s" parameter\n' % (section, section_param))
                    sys.exit(-1)
                if a == "create":
                    sys.stdout.write(
                        'WARN - Task "%s" Has no "%s" parameter, creating.\n' % (section, section_param))
                    blueprint_configuration[section][section_param] = ""

        if v == "section_option_does_not_exist":
            if blueprint_configuration[section][section_param] == "":
                if a == "die":
                    sys.stderr.write(
                        'ERROR - Task "%s" "%s" parameter is empty\n' % (section, section_param))
                    sys.exit(-1)
                if 'create' in a:
                    try:
                        value = a.split(':')[1]
                    except Exception as ex:
                        value = ""
                    blueprint_configuration[section][section_param] = value
                    sys.stdout.write(
                        'WARN - Task "%s" "%s" parameter is empty, defaulting to "%s"\n' % (section, section_param, value))

        if v == "section_option_default":
            if blueprint_configuration[section][section_param] == "":
                if a == "die":
                    sys.stderr.write(
                        'ERROR - Task "%s" "%s" parameter is empty\n' % (section, section_param))
                    sys.exit(-1)

        if v == "requires-verify":
            REQUIRED_TASKS = blueprint_configuration[section][section_param]
            REQUIRED_TASKS = REQUIRED_TASKS.replace('[', '')
            REQUIRED_TASKS = REQUIRED_TASKS.replace(']', '')
            REQUIRED_TASKS = REQUIRED_TASKS.split(",")
            try:
                REQUIRED_TASKS.remove('')
            except Exception as ex:
                pass
            # For each required task in task check if exists in the blueprint
            for REQUIRED_TASK in REQUIRED_TASKS:
                if REQUIRED_TASK == section:
                    sys.stderr.write(
                        'ERROR - Task "%s" Has its self in "REQUIRES" parameter.\n' % (section))
                    sys.exit(-1)

                if REQUIRED_TASK not in blueprint_configuration:
                    sys.stderr.write(
                        'ERROR - Task "%s" in "REQUIRES" of Task: "%s" do not exist in blueprint file\n' % (REQUIRED_TASK, section))
                    sys.exit(-1)

    return blueprint_configuration


class LOCAL_TASK(luigi.Task):

    def requires(self):
        return eval(blueprint_configuration.get(self.__class__.__name__+"()", "REQUIRES"))

    def output(self):
        return luigi.LocalTarget(blueprint_configuration.get(self.__class__.__name__+"()", "RESULTS"))

    def run(self):
        TASK_OPTIONS = {}
        TASK_OPTIONS['COMMAND'] = blueprint_configuration.get(
            self.__class__.__name__+"()", "COMMAND")
        TASK_OPTIONS['RESULTS'] = blueprint_configuration.get(
            self.__class__.__name__+"()", "RESULTS")
        TASK_OPTIONS['SUCCESS_EXIT_CODE'] = blueprint_configuration.get(
            self.__class__.__name__+"()", "SUCCESS_EXIT_CODE")
        sys.stdout.write('INFO - Task: %s - Starting Execution.\n'%(self.__class__.__name__))
        TASK_OUTPUT = cmd_execute(cmd=TASK_OPTIONS['COMMAND'])

        if TASK_OUTPUT[0] == int(TASK_OPTIONS['SUCCESS_EXIT_CODE']):
            with open(TASK_OPTIONS['RESULTS'], "w+") as stdout_results_file:
                stdout_results_file.write(TASK_OUTPUT[1])
            sys.stdout.write('INFO - Task: %s - Succedeed with exit code: %s check %s.\n' %
                             (self.__class__.__name__, TASK_OUTPUT[0], TASK_OPTIONS['RESULTS']))
        else:
            with open(TASK_OPTIONS['RESULTS']+".stderr", "w+") as stderr_results_file:
                stderr_results_file.write(TASK_OUTPUT[2])
            sys.stderr.write('ERROR - Task: %s - Failed with exit code: %s check %s.\n' % (
                self.__class__.__name__, TASK_OUTPUT[0], TASK_OPTIONS['RESULTS']+".stderr"))

    def on_failure(self, exception):
        try:
            sys.exit()
            message = super().on_failure(exception)
        except Exception as ex:
            pass


class REMOTE_TASK(luigi.Task):

    def requires(self):
        return eval(blueprint_configuration.get(self.__class__.__name__+"()", "REQUIRES"))

    def output(self):
        return luigi.LocalTarget(blueprint_configuration.get(self.__class__.__name__+"()", "RESULTS"))

    def run(self):
        TASK_OPTIONS = {}
        TASK_OPTIONS['COMMAND'] = blueprint_configuration.get(
            self.__class__.__name__+"()", "COMMAND")
        TASK_OPTIONS['HOST'] = blueprint_configuration.get(
            self.__class__.__name__+"()", "HOST")
        TASK_OPTIONS['PORT'] = int(blueprint_configuration.get(
            self.__class__.__name__+"()", "PORT"))
        TASK_OPTIONS['TIMEOUT'] = int(blueprint_configuration.get(
            self.__class__.__name__+"()", "TIMEOUT"))
        # Get credentials
        CREDS_NAME = blueprint_configuration.get(
            self.__class__.__name__+"()", "CREDS")
        TASK_OPTIONS['KEY'] = blueprint_configuration.get(CREDS_NAME, "KEY")
        TASK_OPTIONS['USER'] = blueprint_configuration.get(CREDS_NAME, "USER")
        TASK_OPTIONS['PASS'] = blueprint_configuration.get(CREDS_NAME, "PASS")
        TASK_OPTIONS['RESULTS'] = blueprint_configuration.get(
            self.__class__.__name__+"()", "RESULTS")
        TASK_OPTIONS['SUCCESS_EXIT_CODE'] = int(blueprint_configuration.get(
            self.__class__.__name__+"()", "SUCCESS_EXIT_CODE"))
        TASK_OPTIONS['USE_PROXY'] = blueprint_configuration.get(
            self.__class__.__name__+"()", "USE_PROXY").lower()

        if TASK_OPTIONS['USE_PROXY'] == 'false':
            try:
                sys.stdout.write('INFO - Task: "%s" - SSH Connect to host: "%s".\n' %
                                 (self.__class__.__name__, TASK_OPTIONS['HOST']))
                server = SSHSession(TASK_OPTIONS['HOST'], TASK_OPTIONS['USER'],
                                    password=TASK_OPTIONS['PASS'], timeout=TASK_OPTIONS['TIMEOUT'],private_key_file=TASK_OPTIONS['KEY']).open()
            except Exception as ex:
                sys.stderr.write('ERROR - Task: %s - Could not connect to host: %s on port: %s with creds: %s' %
                                 (self.__class__.__name__, TASK_OPTIONS['HOST'], TASK_OPTIONS['PORT'], CREDS_NAME))
        if TASK_OPTIONS['USE_PROXY'] == 'true':
            TASK_OPTIONS['PROXY'] = blueprint_configuration.get(
                self.__class__.__name__+"()", "PROXY")
            PROXY = TASK_OPTIONS['PROXY']
            PROXY_SERVER = blueprint_configuration.get(PROXY, 'HOST')
            PROXY_CREDS  = blueprint_configuration.get(PROXY, 'CREDS')
            PROXY_USER   = blueprint_configuration.get(PROXY_CREDS, 'USER')
            PROXY_PASS   = blueprint_configuration.get(PROXY_CREDS, 'PASS')
            PROXY_KEY    = blueprint_configuration.get(PROXY_CREDS, 'KEY')
            sys.stdout.write('INFO - Task: "%s" - SSH Connect to Proxy: "%s".\n' %
                             (self.__class__.__name__, PROXY))
            proxy_server = SSHSession(
                PROXY_SERVER, PROXY_USER, password=PROXY_PASS, timeout=TASK_OPTIONS['TIMEOUT'],private_key_file=PROXY_KEY).open()
            server = proxy_server.get_remote_session(
                TASK_OPTIONS['HOST'], password=TASK_OPTIONS['PASS'], compress=False, timeout=TASK_OPTIONS['TIMEOUT'],private_key_file=TASK_OPTIONS['KEY'])
            sys.stdout.write('INFO - Task: "%s" - SSH Connect to host: "%s".\n' %
                             (self.__class__.__name__, TASK_OPTIONS['HOST']))

        if server.is_active() == True:
            sys.stdout.write('INFO - Task: "%s" - host: "%s Executing".\n' %
                             (self.__class__.__name__, TASK_OPTIONS['HOST']))
            (exit_code, output) = server.run_cmd(TASK_OPTIONS['COMMAND'])
            if int(exit_code) == TASK_OPTIONS['SUCCESS_EXIT_CODE']:
                with open(TASK_OPTIONS['RESULTS'], "w+") as stdout_results_file:
                    stdout_results_file.write(output)
                    sys.stdout.write('INFO - Task: %s - Succedeed with exit code: %s check %s.\n' % (
                        self.__class__.__name__, exit_code, TASK_OPTIONS['RESULTS']))
            else:
                with open(TASK_OPTIONS['RESULTS']+".err", "w+") as stderr_results_file:
                    stderr_results_file.write(output)
                    sys.stdout.write('ERROR - Task: %s - Failed with exit code: %s check %s.\n' % (
                        self.__class__.__name__, exit_code, TASK_OPTIONS['RESULTS']+".err"))
        else:
            sys.stderr.write('ERROR - Task: %s - Could not connect to host: %s on port: %s with creds: %s' %
                             (self.__class__.__name__, TASK_OPTIONS['HOST'], TASK_OPTIONS['PORT'], CREDS_NAME))


class DB_TASK(luigi.Task):

    def requires(self):
        return eval(blueprint_configuration.get(self.__class__.__name__+"()", "REQUIRES"))

    def output(self):
        return luigi.LocalTarget(blueprint_configuration.get(self.__class__.__name__+"()", "RESULTS"))

    def run(self):
        #DB_OPTIONS    = {}
        #PROXY_OPTIONS = {}

        db_options    = {}
        proxy_options = {}
        USE_PROXY     = False
        DB = blueprint_configuration.get(self.__class__.__name__+"()","DB")
        
        with open(blueprint_configuration.get(self.__class__.__name__+"()","QUERY"),"r") as f:
            QUERY = f.read()

        PROXY     = None
        TASK_NAME = self.__class__.__name__+"()"
        PROXY     = blueprint_configuration.get(self.__class__.__name__+"()","PROXY")
        USE_PROXY = blueprint_configuration.get(self.__class__.__name__+"()","USE_PROXY")

        CREDS_TO_USE              = blueprint_configuration.get(DB,'CREDS')
        db_options['db_engine']   = blueprint_configuration.get(DB,'ENGINE')
        db_options['db_username'] = blueprint_configuration.get(CREDS_TO_USE,'USER')
        db_options['db_password'] = blueprint_configuration.get(CREDS_TO_USE,'PASS')
        db_options['db_hostname'] = blueprint_configuration.get(DB,'DBHOST')
        db_options['db_name']     = blueprint_configuration.get(DB,'DBNAME')
        db_options['db_port']     = blueprint_configuration.get(DB,'DBPORT')
        db_options['task_name']   = TASK_NAME

        if PROXY != None and USE_PROXY == 'True':
            try:
                CREDS_TO_USE = blueprint_configuration.get(PROXY,'CREDS')
                proxy_options['ssh_username'] = blueprint_configuration.get(CREDS_TO_USE,'USER')
                proxy_options['ssh_password'] = blueprint_configuration.get(CREDS_TO_USE,'PASS')
                proxy_options['ssh_port']     = blueprint_configuration.get(PROXY,'PORT')
                proxy_options['ssh_proxy']    = blueprint_configuration.get(PROXY,'HOST')
            except Exception as ex:
                pass
        
        sys.stdout.write('INFO - Task: %s - host: %s Preparing to execute  %s\n' % (self.__class__.__name__, db_options['db_hostname'], blueprint_configuration.get(self.__class__.__name__+"()","QUERY")))
        results = execQuery(db_options    = db_options,    \
                            db_query      = QUERY,         \
                            proxy_options = proxy_options, \
                            use_proxy     = USE_PROXY)

        if blueprint_configuration.get(self.__class__.__name__+"()",'RESULTS_TYPE') == "csv":
            results.to_csv(blueprint_configuration.get(self.__class__.__name__+"()","RESULTS"),encoding = 'utf-8',index=False,header=True,quoting=2)

        if blueprint_configuration.get(self.__class__.__name__+"()",'RESULTS_TYPE') == "xlsx":
            results.to_excel(blueprint_configuration.get(self.__class__.__name__+"()","RESULTS"),index=False,header=True)


def cleanup_tasks(blueprint_configuration):
    pass


def blueprint_sanity_checks(blueprint_configuration):
    '''
    Do some sanity checks to blueprint configuration, set some defaults if needed
    '''
    blueprint_configuration = sanity_checks_build(
        blueprint_configuration=blueprint_configuration)
    blueprint_configuration = sanity_checks_local(
        blueprint_configuration=blueprint_configuration)
    blueprint_configuration = sanity_checks_remote(
        blueprint_configuration=blueprint_configuration)
    blueprint_configuration = sanity_checks_creds(
        blueprint_configuration=blueprint_configuration)
    blueprint_configuration = sanity_checks_proxy(
        blueprint_configuration=blueprint_configuration)
    blueprint_configuration = sanity_checks_db(
        blueprint_configuration=blueprint_configuration)
    return blueprint_configuration


def sanity_checks_proxy(blueprint_configuration):
    for section in blueprint_configuration:
        # DONT CARE FOR THOSE SECTIONS
        if section not in ['BUILD', 'DEFAULT']:

            # IF TASK DOES NOT HAVE A "TYPE" PARAMETER DIE
            section_mgr(blueprint_configuration=blueprint_configuration, section=section,
                        section_param="TYPE", verify="section_does_not_exist", action="die")
            # IF TASK TYPE != "SSH_PROXY" THERE IS NOTHING ELSE TO CHECK, RETURN
            if blueprint_configuration[section]["TYPE"] != "SSH_PROXY":
                continue
            # IF TASK DOES NOT HAVE A "HOST" PARAMETER DIE
            section_mgr(blueprint_configuration=blueprint_configuration, section=section, section_param="HOST",
                        verify="section_does_not_exist,section_option_does_not_exist", action="die,die")
            # IF TASK DOES NOT HAVE A "CREDS" PARAMETER DIE
            section_mgr(blueprint_configuration=blueprint_configuration, section=section, section_param="CREDS",
                        verify="section_does_not_exist,section_option_does_not_exist", action="die,die")
    return blueprint_configuration


def sanity_checks_creds(blueprint_configuration):
    for section in blueprint_configuration:

        # DONT CARE FOR THOSE SECTIONS
        if section not in ['BUILD', 'DEFAULT']:

            # IF TASK DOES NOT HAVE A "TYPE" PARAMETER DIE
            section_mgr(blueprint_configuration=blueprint_configuration, section=section,
                        section_param="TYPE", verify="section_does_not_exist", action="die")

            # IF TASK TYPE != "CREDS" THERE IS NOTHING ELSE TO CHECK, RETURN
            if blueprint_configuration[section]["TYPE"] != "CREDS":
                continue

            # IF TASK DOES NOT HAVE A "USER" PARAMETER DIE
            section_mgr(blueprint_configuration=blueprint_configuration, section=section, section_param="USER",
                        verify="section_does_not_exist,section_option_does_not_exist", action="die,die")
            # IF TASK DOES NOT HAVE A "PASS" PARAMETER
            blueprint_configuration = section_mgr(blueprint_configuration=blueprint_configuration, section=section,
                                                  section_param="PASS", verify="section_does_not_exist,section_option_does_not_exist", action="create,create:False")
            # IF TASK DOES NOT HAVE A "KEY" PARAMETER
            blueprint_configuration = section_mgr(blueprint_configuration=blueprint_configuration, section=section,
                                                  section_param="KEY", verify="section_does_not_exist,section_option_does_not_exist", action="create,create:False")
    return blueprint_configuration

def sanity_checks_db(blueprint_configuration):
    '''
    Do some sanity checks to blueprint configuration for remote tasks, set some defaults if needed.
    '''
    for section in blueprint_configuration:

        # DONT CARE FOR THOSE SECTIONS
        if section not in ['BUILD', 'DEFAULT']:

            # IF TASK DOES NOT HAVE A "TYPE" PARAMETER DIE
            section_mgr(blueprint_configuration=blueprint_configuration, section=section,
                        section_param="TYPE", verify="section_does_not_exist", action="die")
            # IF TASK TYPE != "LOCAL_TASK" THERE IS NOTHING ELSE TO CHECK, RETURN
            if blueprint_configuration[section]["TYPE"] != "DB_TASK":
                continue
            # IF TASK DOES NOT HAVE A "COMMAND" PARAMETER DIE
            section_mgr(blueprint_configuration=blueprint_configuration, section=section, section_param="QUERY",
                        verify="section_does_not_exist,section_option_does_not_exist", action="die,die")
            # IF TASK DOES NOT HAVE A "HOST" PARAMETER
            section_mgr(blueprint_configuration=blueprint_configuration, section=section, section_param="DB",
                        verify="section_does_not_exist,section_option_does_not_exist", action="die,die")
            # IF TASK RESULTS OPTION IS MISSING DIE
            section_mgr(blueprint_configuration=blueprint_configuration, section=section, section_param="RESULTS",
                        verify="section_does_not_exist,section_option_does_not_exist", action="die,die")
            # IF TASK REQUIRES OPTION IS MISSING CREATE EMPTY
            blueprint_configuration = section_mgr(blueprint_configuration=blueprint_configuration, section=section,
                                                  section_param="REQUIRES", verify="section_does_not_exist,section_option_does_not_exist", action="create,create:[]")
            # IF TASK CLEANUP OPTION IS MISSING CREATE False
            blueprint_configuration = section_mgr(blueprint_configuration=blueprint_configuration, section=section, section_param="CLEANUP",
                                                  verify="section_does_not_exist,section_option_does_not_exist", action="create,create:False")
            blueprint_configuration = section_mgr(blueprint_configuration=blueprint_configuration, section=section, section_param="USE_PROXY",
                                                  verify="section_does_not_exist,section_option_does_not_exist", action="create,create:False")
            blueprint_configuration = section_mgr(blueprint_configuration=blueprint_configuration, section=section, section_param="PROXY",
                                                  verify="section_does_not_exist,section_option_does_not_exist", action="create,create:False")
                        # IF TASK REQUIRES OPTION IS MISSING CREATE EMPTY
            blueprint_configuration = section_mgr(blueprint_configuration=blueprint_configuration, section=section,
                                                  section_param="REQUIRES", verify="section_does_not_exist,section_option_does_not_exist", action="create,create:[]")

    return blueprint_configuration

def sanity_checks_remote(blueprint_configuration):
    '''
    Do some sanity checks to blueprint configuration for remote tasks, set some defaults if needed.
    '''
    for section in blueprint_configuration:

        # DONT CARE FOR THOSE SECTIONS
        if section not in ['BUILD', 'DEFAULT']:

            # IF TASK DOES NOT HAVE A "TYPE" PARAMETER DIE
            section_mgr(blueprint_configuration=blueprint_configuration, section=section,
                        section_param="TYPE", verify="section_does_not_exist", action="die")
            # IF TASK TYPE != "LOCAL_TASK" THERE IS NOTHING ELSE TO CHECK, RETURN
            if blueprint_configuration[section]["TYPE"] != "REMOTE_TASK":
                continue
            # IF TASK DOES NOT HAVE A "COMMAND" PARAMETER DIE
            section_mgr(blueprint_configuration=blueprint_configuration, section=section, section_param="COMMAND",
                        verify="section_does_not_exist,section_option_does_not_exist", action="die,die")
            # IF TASK DOES NOT HAVE A "HOST" PARAMETER
            section_mgr(blueprint_configuration=blueprint_configuration, section=section, section_param="HOST",
                        verify="section_does_not_exist,section_option_does_not_exist", action="die,die")
            # IF TASK DOES NOT HAVE A "SUCCESS_EXIT_CODE" PARAMETER CREATE IT WITH A DEFAULT VALUE OF "0"
            blueprint_configuration = section_mgr(blueprint_configuration=blueprint_configuration, section=section,
                                                  section_param="SUCCESS_EXIT_CODE", verify="section_does_not_exist,section_option_does_not_exist", action="create,create:0")
            # IF TASK SUCCESS_EXIT_CODE PARAMETER NOT IN RANGE(0,255) DIE
            section_mgr(blueprint_configuration=blueprint_configuration, section=section,
                        section_param="SUCCESS_EXIT_CODE", verify="range(0-256)", action="die")
            # IF TASK DOES NOT HAVE A "PORT" CREATE IT
            blueprint_configuration = section_mgr(blueprint_configuration=blueprint_configuration, section=section,
                                                  section_param="PORT", verify="section_does_not_exist,section_option_does_not_exist", action="create,create:22")
            # IF TASK PORT NOT IN RANGE(0,65535) DIE
            section_mgr(blueprint_configuration=blueprint_configuration, section=section,
                        section_param="PORT", verify="range(0-65535)", action="die")
            # IF TASK DOES NOT HAVE A "TIMEOUT" CREATE IT
            blueprint_configuration = section_mgr(blueprint_configuration=blueprint_configuration, section=section,
                                                  section_param="TIMEOUT", verify="section_does_not_exist,section_option_does_not_exist", action="create,create:10")
            # IF TASK TIMEOUT NOT IN RANGE(0,60) DIE
            section_mgr(blueprint_configuration=blueprint_configuration, section=section,
                        section_param="TIMEOUT", verify="range(0-60)", action="die")
            # IF TASK RESULTS OPTION IS MISSING DIE
            section_mgr(blueprint_configuration=blueprint_configuration, section=section, section_param="RESULTS",
                        verify="section_does_not_exist,section_option_does_not_exist", action="die,die")
            # IF TASK REQUIRES OPTION IS MISSING CREATE EMPTY
            blueprint_configuration = section_mgr(blueprint_configuration=blueprint_configuration, section=section,
                                                  section_param="REQUIRES", verify="section_does_not_exist,section_option_does_not_exist", action="create,create:[]")
            # IF TASK CLEANUP OPTION IS MISSING CREATE False
            blueprint_configuration = section_mgr(blueprint_configuration=blueprint_configuration, section=section, section_param="CLEANUP",
                                                  verify="section_does_not_exist,section_option_does_not_exist", action="create,create:False")
            # IF TASK USE_PROXY OPTION IS MISSING CREATE False
            blueprint_configuration = section_mgr(blueprint_configuration=blueprint_configuration, section=section, section_param="USE_PROXY",
                                                  verify="section_does_not_exist,section_option_does_not_exist", action="create,create:False")
            # IF TASK CLEANUP OPTION is not True or False die
            section_mgr(blueprint_configuration=blueprint_configuration, section=section,
                        section_param="CLEANUP", verify="section_option_allowed_values:True-False", action="die")

    return blueprint_configuration


def sanity_checks_local(blueprint_configuration):
    '''
    Do some sanity checks to blueprint configuration for local tasks, set some defaults if needed.
    '''
    for section in blueprint_configuration:

        # DONT CARE FOR THOSE SECTIONS
        if section not in ['BUILD', 'DEFAULT']:

            # IF TASK DOES NOT HAVE A "TYPE" PARAMETER DIE
            section_mgr(blueprint_configuration=blueprint_configuration, section=section,
                        section_param="TYPE", verify="section_does_not_exist", action="die")
            # IF TASK TYPE != "LOCAL_TASK" THERE IS NOTHING ELSE TO CHECK, RETURN
            if blueprint_configuration[section]["TYPE"] != "LOCAL_TASK":
                continue
            # IF TASK DOES NOT HAVE A "COMMAND" PARAMETER DIE
            section_mgr(blueprint_configuration=blueprint_configuration, section=section, section_param="COMMAND",
                        verify="section_does_not_exist,section_option_does_not_exist", action="die,die")
            # IF TASK DOES NOT HAVE A "SUCCESS_EXIT_CODE" CREATE IT
            blueprint_configuration = section_mgr(blueprint_configuration=blueprint_configuration, section=section,
                                                  section_param="SUCCESS_EXIT_CODE", verify="section_does_not_exist,section_option_does_not_exist", action="create,create:0")
            # IF TASK SUCCESS_EXIT_CODE NOT IN RANGE(0,255) DIE
            section_mgr(blueprint_configuration=blueprint_configuration, section=section,
                        section_param="SUCCESS_EXIT_CODE", verify="range(0-256)", action="die")
            # IF TASK RESULTS OPTION IS MISSING DIE
            section_mgr(blueprint_configuration=blueprint_configuration, section=section, section_param="RESULTS",
                        verify="section_does_not_exist,section_option_does_not_exist", action="die,die")
            # IF TASK REQUIRES OPTION IS MISSING CREATE EMPTY
            blueprint_configuration = section_mgr(blueprint_configuration=blueprint_configuration, section=section,
                                                  section_param="REQUIRES", verify="section_does_not_exist,section_option_does_not_exist", action="create,create:[]")
            # IF TASK CLEANUP OPTION IS MISSING CREATE False
            blueprint_configuration = section_mgr(blueprint_configuration=blueprint_configuration, section=section, section_param="CLEANUP",
                                                  verify="section_does_not_exist,section_option_does_not_exist", action="create,create:False")
            # IF TASK CLEANUP OPTION is not True or False die
            section_mgr(blueprint_configuration=blueprint_configuration, section=section,
                        section_param="CLEANUP", verify="section_option_allowed_values:True-False", action="die")

    return blueprint_configuration


def sanity_checks_build(blueprint_configuration):
    '''
    Do some sanity checks to blueprint configuration for BUILD sections, set some defaults if needed
    '''
    BUILD_TASKS_TO_EXECUTE = []

    # Die if BUILD parameter not exists
    if "BUILD" not in blueprint_configuration:
        sys.stderr.write(
            'ERROR - No BUILD section in blueprint %s\n' % (cmd_args.b))
        sys.exit(-1)

    # Die if "BUILD""TASKS" section is missing
    if "TASKS" not in blueprint_configuration["BUILD"]:
        sys.stderr.write('ERROR - No TASKS option in BUILD section\n')
        sys.exit(-1)

    BUILD_TASKS_TO_EXECUTE = blueprint_configuration.get("BUILD", "TASKS")
    BUILD_TASKS_TO_EXECUTE = BUILD_TASKS_TO_EXECUTE.replace('[', '')
    BUILD_TASKS_TO_EXECUTE = BUILD_TASKS_TO_EXECUTE.replace(']', '')
    BUILD_TASKS_TO_EXECUTE = BUILD_TASKS_TO_EXECUTE.split(',')
    # Remove empty elements
    try:
        BUILD_TASKS_TO_EXECUTE.remove('')
    except Exception as ex:
        pass
    if len(BUILD_TASKS_TO_EXECUTE) == 0:
        sys.stderr.write('ERROR - BUILD, TASKS section is empty.\n')
        sys.exit(-1)

    # Die if a "BUILD""TASKS" task does not exist in the blueprint
    for build_section in BUILD_TASKS_TO_EXECUTE:
        if not build_section in blueprint_configuration:
            sys.stderr.write(
                'ERROR - Build Task %s does not exist in blueprint.\n' % (build_section))
            sys.exit(-1)

    # Get workers option or set a default one
    # Die if given value is not a single number or if value is a string
    if "WORKERS" not in blueprint_configuration["BUILD"]:
        sys.stdout.write('WARN - BUILD, WORKERS missing, defaulting to "8"\n')
        blueprint_configuration["BUILD"]["WORKERS"] = "8"
    else:
        try:
            int(blueprint_configuration["BUILD"]["WORKERS"])
        except Exception as ex:
            sys.stderr.write(
                'ERROR - BUILD, WORKERS is not an integer or a single number.\n')
            sys.exit(-1)

    if "LOCAL_SCHEDULER" not in blueprint_configuration["BUILD"]:
        blueprint_configuration["BUILD"]["LOCAL_SCHEDULER"] = "True"
    else:
        if blueprint_configuration["BUILD"]["LOCAL_SCHEDULER"] not in ["True", "False"]:
            sys.stderr.write(
                'ERROR - BUILD, LOCAL_SCHEDULER must be either "True" or "False".\n')
            sys.exit(-1)

    # Die if there is a non known parameter for BUILD section
    for parameter in blueprint_configuration["BUILD"]:
        if parameter not in ['local_scheduler', 'workers', 'tasks']:
            sys.stderr.write(
                'ERROR - "%s" Is not a valid parameter for BUILD section\n' % (parameter))
            sys.exit(-1)

    return blueprint_configuration


def get_blueprint_configuration(blueprint):
    '''
    Summary: 
        Return a blueprint configuration object.

    Description:
        A blueprint configuration object is
        actually a luigi configuration text file
        With some extras to cover our needs.

    Parameters:
        blueprint (string): The filename of the blueprint.

    Returns:
        Object: a luigi configuration object.
    '''
    try:
        with open(blueprint) as b:
            pass
        config = luigi.configuration.get_config()
        config.read(filenames=blueprint)
        return config
    except Exception as ex:
        sys.stderr.write(
            'ERROR - Cannot read blueprint \'%s\' - Exception: %s\n' % (blueprint, str(ex)))
        sys.exit(-1)


# Program Starts Here
# Create command line arguments, our only argument is the blueprint file.
cmd_options = argparse.ArgumentParser(description='Luigi Blueprint')
cmd_options.add_argument(
    '-b', type=str, help='Blueprint to execute', action='store', required=True)
cmd_args = cmd_options.parse_args()
# Pass file, get configuration object
blueprint_configuration = get_blueprint_configuration(blueprint=cmd_args.b)
# Do sanity checks and set default values if needed
blueprint_configuration = blueprint_sanity_checks(
    blueprint_configuration=blueprint_configuration)

# Build the tasks
for task in blueprint_configuration:
    if task not in ['BUILD', 'DEFAULT']:
        TASK_TYPE = blueprint_configuration[task]["TYPE"]
        if TASK_TYPE in ["LOCAL_TASK", "REMOTE_TASK", "DB_TASK"]:
            results_test = blueprint_configuration[task]["RESULTS"]
            cleanup_test = blueprint_configuration[task]["CLEANUP"]
            try:
                task = task.replace('()', '')
                exec("class %s(%s):\n\tpass\n" % (task, TASK_TYPE))

                if os.path.isfile(results_test) == True and cleanup_test == "True":
                    os.remove(results_test)

                    sys.stdout.write(
                        'INFO - Task: "%s" of Type: "%s" Previous result: "%s" Deleted.\n' % (task, TASK_TYPE, results_test))

                if os.path.isfile(results_test) == True and cleanup_test == "False":
                    sys.stdout.write(
                        'INFO - Task: "%s" of Type: "%s" Previous result: "%s" Not Deleted.\n' % (task, TASK_TYPE, results_test))
                    sys.stdout.write(
                        'INFO - Task: "%s" of Type: "%s" Created But Will Not Run Because Previous Result Exists.\n' % (task, TASK_TYPE))

                if os.path.isfile(results_test) == False:
                    sys.stdout.write(
                        'INFO - Task: "%s" of Type: "%s" Created.\n' % (task, TASK_TYPE))
            except Exception as ex:
                sys.stderr.write(
                    'ERROR - Task "%s" Cannot be created: %s\n' % (task, str(ex)))
                sys.exit(-1)

if __name__ == '__main__':
    TASKS = eval(blueprint_configuration.get("BUILD", "TASKS"))
    luigi.build(TASKS, local_scheduler=True, workers=int(
        blueprint_configuration.get("BUILD", "workers")), log_level='NOTSET')
    sys.stdout.write('INFO - END.\n')
