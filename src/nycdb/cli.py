"""
TODO:
- create a config.ini flag that stores the postgres parametrs
    - to achieve this, use ConfigParser to read config.ini
    and then ChainMap collections to manage namespace: 
    https://stackoverflow.com/questions/35142992/dynamically-set-default-value-from-cfg-file-through-argparse-python
- add postgis datasets
"""

import argparse
import logging
import os
import subprocess
import sys
import configparser
from .dataset import Dataset, datasets


POSTGRES_DEFAULTS = {
    'user': os.environ.get('NYCDB_POSTGRES_USER', 'nycdb'),
    'password': os.environ.get('NYCDB_POSTGRES_PASSWORD', 'nycdb'),
    'host': os.environ.get('NYCDB_POSTGRES_HOST', '127.0.0.1'),
    'database': os.environ.get('NYCDB_POSTGRES_DB', 'nycdb'),
    'port': os.environ.get('NYCDB_POSTGRES_PORT', '5432')
}


def parse_args():
    parser = argparse.ArgumentParser(description='NYC-DB: utilities for the database of NYC housing data')

    # Download, Load, Verify, Dump, Reload
    parser.add_argument('--download', action='store', help='downloads file for provided dataset')
    parser.add_argument('--load', action='store', help='loads dataset into postgres')
    parser.add_argument('--verify', action='store', help='verifies a dataset by checking the table row count')
    parser.add_argument('--dump', action='store', help='creates a sql dump of the datasets in the current folder')
    parser.add_argument('--reload', action='store', help='overwrites dataset into postgres')
    # list and verify all
    parser.add_argument('--list-datasets', action='store_true', help='lists all datasets')
    parser.add_argument('--verify-all', action='store_true', help='verifies all datasets')
    # DB CONNECTION
    parser.add_argument(
        "-U",
        "--user",
        help="Postgres user. default: {}".format(POSTGRES_DEFAULTS['user'])
    )
    parser.add_argument(
        "-P",
        "--password",
        help="Postgres password. default: {}".format(POSTGRES_DEFAULTS['password']),
    )
    parser.add_argument(
        "-H",
        "--host",
        help="Postgres host: default: {}".format(POSTGRES_DEFAULTS['host']),
    )
    parser.add_argument(
        "-D",
        "--database",
        help="postgres database: default: {}".format(POSTGRES_DEFAULTS['database']),
    )
    parser.add_argument(
        "--port",
        help="Postgres port: default: {}".format(POSTGRES_DEFAULTS['port']),
    )
    # change location of data dir
    parser.add_argument("--root-dir", help="location of data directory", default="./data")
    # easily inspect the database from the command-line
    parser.add_argument("--dbshell", action="store_true", help="runs psql interactively")
    parser.add_argument("--hide-progress", action="store_true", help="hide the progress bar")
    # overwrites  default values with config.ini parameters, but is in turn
    # overwritten by user-defined args
    parser.add_argument(
        "--config-section", help="section to read from config.ini"
    )
    parser.add_argument(
        "--config-path", help="path to config.ini"
    )
    return parser.parse_args()


def print_datasets():
    for ds in datasets().keys():
        print(ds)


def verify_all(args):
    exit_status = 0

    for ds in datasets().keys():
        if not Dataset(ds, args=args).verify():
            exit_status = 1

    sys.exit(exit_status)


def run_dbshell(args):
    env = os.environ.copy()
    env['PGPASSWORD'] = args.password
    retval = subprocess.call([
        'psql', '-h', args.host, '-p', args.port, '-U', args.user, '-d', args.database
    ], env=env)
    sys.exit(retval)


def dispatch(args):
    print(args)
    if args.list_datasets:
        print_datasets()
    elif args.verify:
        if Dataset(args.verify, args=args).verify():
            sys.exit(0)
        else:
            sys.exit(1)
    elif args.verify_all:
        verify_all(args)
    elif args.download:
        Dataset(args.download, args=args).download_files()
    elif args.load:
        Dataset(args.load, args=args).db_import()
    elif args.reload:
        Dataset(args.reload, args=args).db_reimport()
    elif args.dump:
        Dataset(args.dump, args=args).dump()
    elif args.dbshell:
        run_dbshell(args=args)


def main():
    logging.basicConfig(level=logging.DEBUG)
    args = parse_args()
    # figure out if only the database configs can be parsed out, prob from 
    # POSTGRES_DEFAULTS keys, and then put back into 'args' after chainmap()
    # config = {k: v for k, v in vars(args).items() if v is not None}
    # args = config  # update args with config params
    dispatch(args)


if __name__ == '__main__':
    main()
