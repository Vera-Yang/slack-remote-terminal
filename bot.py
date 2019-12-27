import time
import re
from slackclient import SlackClient
import json
import inspect
import os
import traceback
import threading
import tempfile
import subprocess

with open('config.json') as f:
    config = json.load(f)

# SLACK_BOT_TOKEN = os.environ['SLACK_BOT_TOKEN'] or config['SLACK_BOT_TOKEN']
SLACK_BOT_TOKEN = "xoxb-13984615488-888062954951-2FlEloGRCBUgWWKbdmVYTfMu"

# instantiate Slack client
slack_client = SlackClient(SLACK_BOT_TOKEN)

# constants
RTM_READ_DELAY = 0.5  # 1 second delay between reading from RTM
MENTION_REGEX = "^<@(|[WU].+?)>(.*)"
MENTION_REGEX_EMPTY = ".*<@(|[WU].+?)>.*"


def ignore_exception(ignore_exception=Exception, default_val=None):
    def dec(function):
        def _dec(*args, **kwargs):
            try:
                return function(*args, **kwargs)
            except ignore_exception:
                return default_val

        return _dec
    return dec


class Commands:
    @staticmethod
    def help(args, reply, api_call, event):
        """
        Usage:
            {Database} {Operations}
        Example: 
            Deploy PostgreSQL
        Support Database:
            PostgreSQL
            MySQL 8
            MySQL 5
            SQL Server 2019
            SQL Server 2017
            DB2
        Support Operations:
            Deploy
            Destroy
            Start
            Stop
        """
        reply('\n'.join(["`{}`\n{}".format(name, m.__doc__)
                         for name, m in inspect.getmembers(Commands, predicate=inspect.isfunction)]),
              thread_ts=event['ts'], reply_broadcast=True)

    @staticmethod
    def upload(args, reply, api_call, event):
        """
        get file from host
        _ upload file_path _
        """
        if not args:
            reply('you mast send me file path', 'upload error', reply_broadcast=True)
            return

        path = os.path.expanduser(' '.join(args))
        if os.path.isdir(path):
            reply('`{}` is a directory, this is not supported yet'.format(path), 'upload error', reply_broadcast=True)
        elif os.path.isfile(path):
            reply('Uploading', thread_ts=event['ts'])
            with open(path, 'rb') as f:
                api_call('files.upload',
                         file=f,
                         filename=os.path.basename(path),
                         filetype=os.path.splitext(path)[-1],
                         title=path, reply_broadcast=True)
        else:
            reply('`{}` file not found'.format(path), 'upload error', reply_broadcast=True)

    log_files = {}

    @staticmethod
    def terminal(args, reply, api_call, event):
        """
        execute command/script on bash or other interpreter
        _ command/script _
        or
        _ terminal command/script _
        """

        f = tempfile.NamedTemporaryFile()
        proc = subprocess.Popen(config["INTERPRETER"],
                                stderr=f.file,
                                stdout=f.file,
                                stdin=subprocess.PIPE,
                                cwd=os.path.expanduser('~'))

        Commands.log_files[str(proc.pid)] = f
        command = ' '.join(args)
        reply('Running on {}'.format(proc.pid))
        # reply('/giphy togepi')
        proc.communicate(command.encode())
        fl = f.tell()
        f.seek(0)
        if fl < config['MAX_TEXT_SIZE']:
            reply(f.read(), mrkdwn=False, reply_broadcast=True)
        else:
            api_call('files.upload',
                     file=f,
                     filename='log.txt',
                     filetype='txt',
                     title=command,
                     reply_broadcast=True)
        f.close()
        del Commands.log_files[str(proc.pid)]
        if proc.returncode == 0 and args[0] == "docker" and args[1] == "run":
            reply_info = generate_reply(command)
            reply(reply_info, "Deploy Success", reply_broadcast=True)
        title = '{} exited with: {}'.format(proc.pid, proc.returncode)
        reply(title, reply_broadcast=fl >= config['MAX_TEXT_SIZE'])

    @staticmethod
    def getlog(args, reply, api_call, event):
        """
        get log from active process which was scheduled by bot
        _ getlog process_id [size] _
        """

        if not args:
            reply("Process id not passed", "Error", reply_broadcast=True)

        args = [a.strip() for a in args]
        pid = args[0]
        if pid in Commands.log_files:
            log_file = Commands.log_files[pid]
            fl = log_file.file.tell()
            with open(log_file.name, 'rb') as f:
                if len(args) > 1:
                    nl = args[1]
                    try_pars_int = ignore_exception(ValueError)(int)
                    v = try_pars_int(nl)
                    if not v:
                        reply("`{}` is not int".format(nl), "Error", reply_broadcast=True)
                        return
                    v = min(v, fl)
                    f.seek(-v, 2)
                    fl = v

                if fl < config['MAX_TEXT_SIZE']:
                    reply(f.read(), mrkdwn=False, reply_broadcast=True)
                else:
                    api_call('files.upload',
                             file=f,
                             filename='log.txt',
                             filetype='txt',
                             title=str(pid),
                             reply_broadcast=True)
        else:
            reply('can\'t find process id {}', "Error", reply_broadcast=True)

def generate_reply(command):
    db_type, port = get_db_type(command)
    reply = '''
    Deploy %s successfully.
    Connection info:
        Host/IP: 10.197.116.46
        Port: %s
        User: tpch
        Password: mstr123#
        Database: tpch
    ''' % (db_type, port)
    print reply
    return reply

def get_db_type(command):
    image_name = re.findall(r"--name (.+?)_", command)
    db_type = {
        "postgres": "Postgre SQL",
        "mysql5": "MySQL 5",
        "mysql8": "MySQL 8",
        "sqlserver2017": "SQL Server 2017",
        "sqlserver2019": "SQL Server 2019"
    }
    default_port = {
        "postgres": "5432",
        "mysql5": "3306",
        "mysql8": "3307",
        "sqlserver2017": "1433",
        "sqlserver2019": "1434"
    }
    db_name = image_name[0]
    print db_type[db_name], default_port[db_name]
    return db_type[db_name], default_port[db_name]


def parse_bot_commands(slack_events, bot_id, ims):
    """
        Parses a list of events coming from the Slack RTM API to find bot commands.
        If a bot command is found, this function returns a tuple of command and channel.
        If its not found, then this function returns None, None.
    """
    for event in slack_events:
        if event["type"] == "message" and not "subtype" in event:
            user_id, message = parse_direct_mention(event["text"], event, ims, bot_id)
            if user_id == bot_id:
                # remove url tags
                match = re.match('(.*)<(.+?)\|(.+?)>(.*)', message)
                if match:
                    message = match.group(1) + match.group(3) + match.group(4)
                return message, event
    return None, None


def parse_direct_mention(message_text, event, ims, bot_id):
    """
        Finds a direct mention (a mention that is at the beginning) in message text
        and returns the user ID which was mentioned. If there is no direct mention or direct message, returns None
    """

    print ("Processing: " + message_text)

    matches = re.search(MENTION_REGEX, message_text)
    if matches:
        return matches.group(1), matches.group(2).strip()
    matches = re.search(MENTION_REGEX_EMPTY, message_text)
    if matches:
        return matches.group(1), ''
    if event['channel'] in ims:
        return bot_id, message_text

    return None, None


def handle_command(command, event):
    """
        Executes bot or terminal command
    """
    start_time = time.time()
    channel = event['channel']

    def api_call(*args, **kwargs):
        if time.time() - start_time > config['MENTION_CHANNEL_AFTER']:
            if time.time() - start_time:
                kwargs['text'] = '<!channel> \n' + (kwargs['text'] if 'text' in kwargs else '')

        if 'files.upload' in args:
            kwargs['channels'] = channel
        elif 'chat.postMessage' in args:
            kwargs['channel'] = channel
        kwargs['thread_ts'] = event['ts']
        # print args, kwargs
        j = slack_client.api_call(*args, **kwargs)
        if 'ok' not in j or not j['ok']:
            print('Method: {}\n, args:{}\n response: {}'.format(args, kwargs, json.dumps(j, indent=2)))
        return j

    def reply(text, title=None, **kwargs):
        if title:
            text = '*{}*\n{}'.format(title, text)
        if not text:
            text = '`Empty`'
        return api_call("chat.postMessage", text=text, **kwargs)

    if not command:
        Commands.help([], reply, api_call, event)
        return

    subs = command.split(' ')
    if hasattr(Commands, subs[0]):
        ex = getattr(Commands, subs[0])
        subs = subs[1:]
    else:
        ex = Commands.terminal

    def runInThread():
        try:
            ex(subs, reply, api_call, event)
        except:
            reply("```\n{}\n```".format(traceback.format_exc()), 'Error')

    thread = threading.Thread(target=runInThread)
    thread.start()


def run_loop():
    if slack_client.rtm_connect(with_team_state=False):
        print("Terminal Bot connected and running!")
        # Read bot's user ID by calling Web API method `auth.test`
        test = slack_client.api_call("auth.test")
        bot_id = test["user_id"]
        print('User info:')
        print(json.dumps(test))
        
        ims_r = slack_client.api_call("im.list", limit=1000)
        if 'ok' not in ims_r or not ims_r['ok'] or 'ims' not in ims_r:
            print("Can't get direct messages list : {}".format(json.dumps(ims_r)))
            return
        ims = [x['id'] for x in ims_r['ims']]
        if config['NOTIFY_ON_CONNECTION']:
            for im in ims:
                slack_client.api_call('chat.postMessage',
                                    channel=im,
                                    text='Hello I am {}, ready to help you'.format(test['user']))
        while True:
            command, event = parse_bot_commands(slack_client.rtm_read(), bot_id, ims)
            if command is not None:
                user_id = event["user"].lower()
                parsed_command = parse_command(command, user_id)
                handle_command(parsed_command, event)                
            time.sleep(RTM_READ_DELAY)
    else:
        print("Connection failed. Exception traceback printed above.")

def parse_command(command, user_id):
    subs = command.split(' ')
    print(subs)
    operation = subs[0].upper()
    if operation in ["DEPLOY", "DESTROY", "START", "STOP"]:
        parsed_command = {
            "DEPLOY": gen_deploy(subs[1], user_id),
            "DESTROY": gen_destroy(subs[1], user_id),
            "START": gen_start(subs[1], user_id),
            "STOP": gen_stop(subs[1], user_id)
        }
        return parsed_command[operation]
    else:
        return command

def gen_deploy(db_type, user_id):
    deploy_cmd = {
        "POSTGRESQL": "docker run --name postgres_%s -p 5432:5432 -d postgres:latest" % user_id,
        "MYSQL": "docker run --name mysql_%s -p 3307:3306 -e MYSQL_ROOT_PASSWORD=mstr123# -d mysql:8" % user_id,
        "MYSQL8": "docker run --name mysql8_%s -p 3307:3306 -e MYSQL_ROOT_PASSWORD=mstr123# -d mysql:8" % user_id,
        "MYSQL5": "docker run --name mysql5_%s -p 3306:3306 -e MYSQL_ROOT_PASSWORD=mstr123# -d mysql:5" % user_id,
        "SQLSERVER2019": "docker run --name sqlserver2019_%s -e 'ACCEPT_EULA=Y' -e 'SA_PASSWORD=mstr123#' -p 1434:1433 -d mcr.microsoft.com/mssql/server:2019-latest" % user_id,
        "SQLSERVER2017": "docker run --name sqlserver2017_%s -e 'ACCEPT_EULA=Y' -e 'SA_PASSWORD=mstr123#' -p 1433:1433 -d mcr.microsoft.com/mssql/server:2017-latest" % user_id
    }
    print "Deploy command:", deploy_cmd[db_type.upper()]
    return deploy_cmd[db_type.upper()]

def gen_destroy(db_type, user_id):
    return "docker rm -f %s_%s" % (db_type, user_id)

def gen_start(db_type, user_id):
    return "docker start %s_%s" % (db_type, user_id)

def gen_stop(db_type, user_id):
    return "docker stop %s_%s" % (db_type, user_id)

if __name__ == "__main__":
    # while True:
        try:
            run_loop()
        except KeyboardInterrupt:
            raise
        except SystemExit:
            raise
        except:
            traceback.print_exc()
            time.sleep(10)
            pass
