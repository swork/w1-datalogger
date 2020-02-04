#! /usr/bin/env python3

import sys, os, re, argparse, json, dateutil.parser

class W1Datapoint:
    class ItAintMe(ValueError):
        pass
    def __init__(self):
        pass

class W1Datapoint_Linux_w1therm(W1Datapoint):
    """
    String from type-28 (therm sensor) w1slave pseudofile, parsed. If the
    w1therm Linux kernel module is loaded the result includes that line parsed;
    else we calculate the temperature from the raw sensor data (TBD).
    """
    w1therm_value_re = re.compile(r'''
    ^
    (?P<raw> ([0-9a-fA-F]{2} \s){9})
    \s* : \s*
    crc=(?P<crc_value> [0-9a-fA-F]{2}) \s*
    (?P<crc_ok> NO|YES)
    \s* \n
    (?P<w1therm_driver_result>
      (?P<raw2> ([0-9a-fA-F]{2} \s*){9})
      t = (?P<temp> \d+) \s*
    )?
    ''', re.X | re.M)

    def __init__(self, w1_string):
        mo = self.w1therm_value_re.match(w1_string)
        if not mo:
            raise W1Datapoint.ItAintMe("regex match failure")
        super().__init__()
        self.w1therm_string = w1_string
        self.consistent = mo.group('crc_ok') == 'YES'
        if mo.group('temp'):
            self.temp = float(mo.group('temp')) / 1000
        else:
            # calculate temp from raw data. TBD
            raise RuntimeError("temp calc from raw sensor data is not yet implemented")

class Observation:
    handlers = {}

    @classmethod
    def register_datapoint_handler(cls, w1_type_str, obsCls):
        cls.handlers[w1_type_str] = obsCls

    def __init__(self, isotime_str, key, value):
        self.time_struct = dateutil.parser.isoparse(isotime_str)
        self.time_key = self.time_struct.timestamp()
        type_str, _ = key.split('-')
        try:
            self.datapoint = self.handlers[type_str](value)
        except KeyError:
            raise W1Datapoint.ItAintMe()

    def __lt__(self, other):
        return self.time_key < other.time_key

class Observations:
    w1s_re = re.compile(r'^28-(?P<ser>[a-zA-Z0-9])+/w1_slave$')
    v1watershed = dateutil.parser.isoparse('2020-02-03T08:20:03+00:00')

    class NotADataObservation(Exception):
        pass

    def __init__(self):
        self.observations = dict()
        pass

    @classmethod
    def transform1(cls, obj):
        """
        Reformat old records.
        Earliest observations used datapoint name as a key,
        requiring an expensive search. Hork those into Karl Normal Form (all
        static keys), with a 'datapoints' field carrying a list of observations
        with individual "key" entries. Original format allowed for multiple
        observations per recording, but no instances used this so no need to
        account for it here.
        """
        for k in obj.keys():
            if k[:3] == '28-':
                obj['datapoints'] = [{
                    "isotime": obj[k]["isotime"],
                    "key": k,
                    "value": obj[k]["value"]
                }]
                del obj[k]
                break

    @classmethod
    def get_fallback_time(cls, obj):
        fb = None
        try:
            fb = obj['scan_end']
        except KeyError:
            pass
        if fb is None:
            try:
                fb = obj['recording_isotime']
            except KeyError:
                pass
        if fb is None:
            try:
                fb = obj['scan_start']
            except KeyError:
                raise RuntimeError('No fallback time found: {}'.format(repr(obj)))
        return fb

    def process_observation(self, obj):
        if 'uptime' in obj:
            raise Observations.NotADataObservation()
        try:
            dps = obj['datapoints']
        except KeyError:
            self.transform1(obj)
            dps = obj['datapoints']
        fallback_time = None
        for p in dps:
            try:
                isotime = p['isotime']
            except KeyError:
                isotime = self.get_fallback_time(obj)
            k = p['key']
            if k in self.observations:
                self.observations[k].append(Observation(isotime, k, p['value']))
            else:
                self.observations[k] = [Observation(isotime, k, p['value'])]

    def process_json_file(self, infile):
        obj = json.load(infile)
        if isinstance(obj, list):
            for elem in obj:
                try:
                    self.process_observation(elem)
                except Observations.NotADataObservation:
                    pass  # TBD: process status info
        elif isinstance(obj, dict):
            try:
                self.process_observation(elem)
            except Observations.NotADataObservation:
                pass  # TBD: process status info
        else:
            raise RuntimeError("Unrecognized JSON input (need object or array)")

    def process_dir(self, dirname, entries):
        for entry in entries:
            self.process_json_file(open(os.path.join(dirname, entry), "r"))

def main():
    """
    CLI operation
    """
    p = argparse.ArgumentParser()
    p.add_argument('--debug', '-d', action='store_true', default=False)
    p.add_argument('--plot', action='store_true', default=False)
    p.add_argument('input', default='-')
    a = p.parse_args()

    Observation.register_datapoint_handler("28", W1Datapoint_Linux_w1therm)

    observations = Observations()

    infile = None
    if a.input == '-':
        observations.process_json_file(sys.stdin)
    elif os.path.isdir(a.input):
        observations.process_dir(a.input, os.scandir(a.input))
    else:
        observations.process_json_file(open(a.input, 'r'))

    for k in observations.observations.keys():
        print("{}: {} entries".format(k, len(observations.observations[k])))

    if a.plot:
        for key in observations.observations.keys():
            kn, _ = key.split('/')
            with open('{}.data'.format(kn), "w") as of:
                of.write("time temp\n")
                for obs in sorted(observations.observations[key]):
                    of.write("{} {}\n".format(obs.time_key, obs.datapoint.temp))
    return 0

if __name__ == '__main__':
    sys.exit(main())
