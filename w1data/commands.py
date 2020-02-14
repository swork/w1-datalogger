#! /usr/bin/env python

import argparse, configparser, sys, os
from . import common, metadata, observations, rollup, w1datapoint

import logging
logger = logging.getLogger(__name__)

debug_done = False

class LocalArgumentParser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.add_argument('--debug', '-d', nargs='?', const='all')
        self.add_argument('--config', '-c', default=os.path.expanduser("~/.w1.conf"))
        self.add_argument('--rollup-location', default=None)
        self.add_argument('--raw-location', default=None)

    def _resolve(self, a):
        """
        get basic stuff from config file, from env vars, and from command line
        """
        # Set defaults here, unless a bad idea
        raw_location = None
        rollup_location = None

        try:
            config_string = open(a.config, 'r').read()
        except IOError:
            config_string = None

        c = configparser.ConfigParser()
        success = False
        if config_string:
            try:
                c.read_string(config_string, source=a.config)
                success = True
            except configparser.MissingSectionHeaderError:
                config_string = "[global]\n" + config_string

            if not success:
                c.read_string(config_string, source=a.config)
                success = True

        if success:
            if not a.raw_location:  # command-line option takes precedence
                a.raw_location = os.path.expanduser(
                    os.environ.get('raw_location',
                                   c['global'].get('raw_location', raw_location)))
            if not a.rollup_location:
                a.rollup_location = os.path.expanduser(
                    os.environ.get('rollup_location',
                                   c['global'].get('rollup_location', rollup_location)))
        return a

    def parse_args_permissive(self, *args, **kwargs):
        t = super().parse_known_args(*args, **kwargs)
        try:
            a = t[0]
        except ValueError:
            a = t
        return self._resolve(a)

    def parse_args(self, *args, **kwargs):
        a = super().parse_args(*args, **kwargs)
        return self._resolve(a)

def rollup_command():
    """
    Maintain a collection of monthly rollup JSON blobs from raw observation JSON blobs
    """
    direct_name = "w1rollup"
    _, applied_name = os.path.split(sys.argv[0])
    p = LocalArgumentParser()
    if applied_name != direct_name:
        p.add_argument('rollup_command')
    a = p.parse_args()
    do_debug(a)

    if a.rollup_location is None or a.raw_location is None:
        logger.error("Need dirs for raw and rollup data, see --help")
        sys.exit(64)  # EX_USAGE

    return rollup.do_rollup(
        os.path.expanduser(a.rollup_location),
        os.path.expanduser(a.raw_location))

def testcli_command():
    """
    Confidence the CLI is doing the needful
    """
    p = LocalArgumentParser()
    p.add_argument('testcli_command')
    p.add_argument('--option', action='store_true')
    a = p.parse_args()
    do_debug(a)

    if a.testcli_command != 'testcli':
        raise RuntimeError("something's goofy with CLI logic")
    print(repr(a))

def do_debug(a):
    global debug_done
    if not debug_done:
        logging.basicConfig(format="%(levelname)s:%(filename)s:%(lineno)d:%(message)s")
        if a.debug:
            modules = set(a.debug.split(','))
            if 'commands' in modules or 'all' in modules:
                logger.setLevel(logging.DEBUG)
            if 'common' in modules or 'all' in modules:
                common.logger.setLevel(logging.DEBUG)
            if 'metadata' in modules or 'all' in modules:
                metadata.logger.setLevel(logging.DEBUG)
            if 'observations' in modules or 'all' in modules:
                observations.logger.setLevel(logging.DEBUG)
            if 'rollup' in modules or 'all' in modules:
                rollup.logger.setLevel(logging.DEBUG)
            if 'w1datapoint' in modules or 'all' in modules:
                w1datapoint.logger.setLevel(logging.DEBUG)
        debug_done = True

def main():
    """
    CLI operation
    """
    p = LocalArgumentParser()
    p.add_argument('command')
    a = p.parse_args_permissive()
    do_debug(a)

    if a.command == 'rollup':
        return rollup_command()

    if a.command == 'testcli':
        return testcli_command()

    sys.stderr.write(argparse.ArgumentError("Unknown command {}".format(a.command)))
    return 64  # EX_USAGE in BSD sysexits, just to pick a standard

if __name__ == '__main__':
    sys.exit(main())
