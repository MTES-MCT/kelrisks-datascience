import subprocess
import sys

from airflow.hooks.postgres_hook import PostgresHook


class BaseCommand():

    def run_from_argv(self, argv):
        raise NotImplemented()


class RunTest(BaseCommand):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.hook = PostgresHook("postgres_kelrisks")
        test_db = "kelrisks_test"
        self.drop_stmt = "DROP DATABASE IF EXISTS %s" % test_db
        self.create_stmt = "CREATE DATABASE %s" % test_db

    def setup_database(self):
        self.hook.run([self.drop_stmt, self.create_stmt], autocommit=True)

    def teardown_database(self):
        self.hook.run(self.drop_stmt, autocommit=True)

    def run_from_argv(self, argv):
        self.setup_database()
        subprocess.run(['python', '-m', 'unittest'] + argv)
        self.teardown_database()


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
