import sys
import re
import time
from Table import Table
from Condition import Condition

def get_args(s) :
    """
    Given a string describing a command, returns the arguments as a list.
    Inputs:
    s - string describing a command
    Output:
    list containing the arguments
    """
    p = s.find('(')
    return [t.strip() for t in s[p+1:-1].split(',')]

def process_line(line,env) :
    """
    Given a command line and the environment, processes the command and updates the environment.
    Inputs:
    line - line containing commands
    env - dictionary containing environment variables
    """
    if ':=' in line :
        toks = [t.strip() for t in line.split(':=')]
        name = toks[0]
        command = toks[1]
        args = get_args(command)
        if command.startswith('inputfromfile') :
            env[name] = Table(args[0])
        elif command.startswith('select') :
            c = Condition(args[1],[env[args[0]]],[args[0]])
            env[name] = env[args[0]].select(c)
        elif command.startswith('project') :
            env[name] = env[args[0]].project(args[1:])
        elif command.startswith('join') :
            c = Condition(args[2],[env[args[0]],env[args[1]]],[args[0],args[1]])
            env[name] = env[args[0]].join(env[args[1]],args[0],args[1],c)
        elif command.startswith('concat') :
            env[name] = env[args[0]].concat(env[args[1]])
        elif command.startswith('sort') :
            env[name] = env[args[0]].sort(args[1:])
        elif command.startswith('sumgroup') :
            env[name] = env[args[0]].sumgroup(args[1],args[2:])
        elif command.startswith('avggroup') :
            env[name] = env[args[0]].avggroup(args[1],args[2:])
        elif command.startswith('countgroup') :
            env[name] = env[args[0]].countgroup(args[1],args[2:])
        elif command.startswith('sum') :
            env[name] = env[args[0]].sum(args[1])
        elif command.startswith('avg') :
            env[name] = env[args[0]].avg(args[1])
        elif command.startswith('count') :
            env[name] = env[args[0]].count(args[1])
        elif command.startswith('movsum') :
            env[name] = env[args[0]].movsum(args[1],int(args[2]))
        elif command.startswith('movavg') :
            env[name] = env[args[0]].movavg(args[1],int(args[2]))
    else :
        args = get_args(line)
        if line.startswith('Btree') :
            env[args[0]].btree(args[1])
        elif line.startswith('Hash') :
            env[args[0]].hash(args[1])
        elif line.startswith('outputtofile') :
            env[args[0]].write(args[1])

def main() :
    """
    Runs the command processer expecting input from stdin.
    """
    env = dict()
    while True :
        try :
            #turn all whitespace to spaces
            line = re.sub(r'\s+',' ',input()).strip()
            c_idx = line.find('//')
            if c_idx > -1 :
                line = line[:c_idx].strip()
            if len(line) == 0 :
                continue
            print('> '+line)
            sys.stdout.flush()
            s_t = time.perf_counter()
            process_line(line,env)
            e_t = time.perf_counter()
            print(f'Processed line in {e_t-s_t}s')
        except EOFError :
            return

if __name__ == "__main__" :
    main()
