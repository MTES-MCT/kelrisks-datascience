import subprocess
import sys

from kelrisks.tests.helpers import setup_database, teardown_database


class BaseCommand():

    def run_from_argv(self, argv):
        raise NotImplemented()


class RunTest(BaseCommand):

    def run_from_argv(self, argv):
        setup_database()
        subprocess.run(['python', '-m', 'unittest'] + argv)
        teardown_database()


def get_commands():
    return {
        'test': RunTest
    }


def parse_argv(argv):
    # first argument is manage.py
    cmd = argv[1]
    return (cmd, argv[2:])

if __name__ == "__main__":

    (command_str, argv) = parse_argv(sys.argv)
    commands = get_commands()
    try:
        command_klass = commands[command_str]
    except KeyError:
        sys.exit()
    command = command_klass()
    command.run_from_argv(argv)
