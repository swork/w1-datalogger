#! /usr/bin/env python

import argparse, configparser, sys, os
from rollup import do_rollup

class LocalArgumentParser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.add_argument('--debug', '-d', action='store_true')
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

def rollup():
    """
    Maintain a collection of monthly rollup JSON blobs from raw observation JSON blobs
    """
    p = LocalArgumentParser()
    p.add_argument('rollup_command')
    a = p.parse_args()

    if a.rollup_command != 'rollup':
        raise RuntimeError("something's goofy with CLI logic")
    return do_rollup(a.rollup_location, a.raw_location)

def plot():
    """
    Generate a GNUPlot rendering of rollup data (or, optionally, raw)
    """
    p = LocalArgumentParser()
    p.add_argument('plot_command')
    p.add_argument('--raw', action='store_true')
    a = p.parse_args()
    if a.plot_command != 'plot':
        raise RuntimeError("something's goofy with CLI logic")
    return do_plot(a.rollup_location, a.raw_location, a.raw)

def testcli():
    """
    Confidence the CLI is doing the needful
    """
    p = LocalArgumentParser()
    p.add_argument('testcli_command')
    p.add_argument('--option', action='store_true')
    a = p.parse_args()
    if a.testcli_command != 'testcli':
        raise RuntimeError("something's goofy with CLI logic")
    print(repr(a))

def main():
    """
    CLI operation
    """
    p = LocalArgumentParser()
    p.add_argument('command')
    a = p.parse_args_permissive()

    if a.command == 'rollup':
        return rollup()

    if a.command == 'plot':
        return plot()

    if a.command == 'testcli':
        return testcli()

    sys.stderr.write(argparse.ArgumentError("Unknown command {}".format(a.command)))
    return 64  # EX_USAGE in BSD sysexits, just to pick a standard

if __name__ == '__main__':
    sys.exit(main())
