#! /usr/bin/env python3

import sys, os, re, argparse, json
from datetime import datetime
import dateutil.parser
from dateutil import relativedelta
import numpy, table

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
    s3_prefix = 's3://'
    w1s_re = re.compile(r'^28-(?P<ser>[a-zA-Z0-9])+/w1_slave$')
    v1watershed = dateutil.parser.isoparse('2020-02-03T08:20:03+00:00')

    class NotADataObservation(Exception):
        pass

    def __init__(self, rollup_location=None, raw_location=None):
        self.observations = dict()
        self.rollup_location = rollup_location
        self.raw_location = raw_location
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

    def process_w1logger_json(self, blob):
        """Bring into the dataset one observation recorded by w1datalogger.w1logger (or
        a list of these, if we're processing a rollup):

        [
          {
            "scan_start": "2020-02-05T05:35:02.232+00:00",
            "datapoints": [
              {
                "isotime": "2020-02-05T05:35:02.233+00:00",
                "key": "28-011912588b87/w1_slave",
                "value": "03 01 4b 46 7f ff 0c 10 30 : crc=30 YES\n03 01 4b 46 7f ff 0c 10 30 t=16187\n"
              }
            ],
            "scan_end": "2020-02-05T05:35:03.152+00:00",
            "recording_isotime": "2020-02-05T05:35:03+00:00",
            "recording_observer": "803a554a-4882-40d9-a710-deff0c4f47e2",
            "recording_event": "dd2942ea-ae16-424f-af96-0c54e0bc637e"
          }
        ]

        scan_start is the recording device's idea of UTC when it started
        polling the 1-wire bus for datapoints.

        scan_end is the recording device's idea of UTC when it finished
        polling the 1-wire bus for datapoints.

        datapoints is a list of stuff found on the 1-wire bus during this scan.
        Formats are straight outta Linux: key is name of pseudofile under
        /sys/bus/w1/devices, and value is the raw content of that pseudofile.
        All the "isotime" values will fall between the scan_start and scan_end
        times.

        recording_* items are added by the datalogger API receiver:

        recording_isotime is the time at which the API was invoked.

        recording_observer is the API path endpoint used for the invocation.
        ATM the obscurity of these endpoint paths serves as authentication; the
        intent is that these endpoints are assigned 1:1 with recording devices.
        The w1_slave paths ought to indentify the actual data stream, but these
        are hardware addresses so use and assignment might change over time and
        correlation with the recording_observer value can serve as a check on
        these assignments.

        recording_event is the context.aws_request_id of this recording, so
        it's a uniquifier for recordings that happen through the same endpoint
        in the same second.
        """
        if isinstance(blob, list):
            for elem in blob:
                try:
                    self.process_observation(elem)
                except Observations.NotADataObservation:
                    pass  # TBD: process status info
        elif isinstance(blob, dict):
            try:
                self.process_observation(elem)
            except Observations.NotADataObservation:
                pass  # TBD: process status info
        else:
            raise RuntimeError("Unrecognized JSON input (need object or array)")

    def process_w1logger_file(self, infile):
        """Read in a single observation recording (which might have multiple
        observations in it, if the client cached some and reported them later;
        also note each observation includes datapoints for the entire 1-Wire
        bus), or a rollup file (simply a collection of observation recordings
        rewritten as a list).
        """
        blob = json.load(infile)
        return self.process_w1logger_json(blob)

    def process_w1logger_dir(self, raw_location, skippers=None):
        """Read in all bare observation files in a named dir.

        skippers specifies optionally skipping specific time ranges:

         - a list of ranges
         - each of which is a begin/end pair
         - each of which is a datetime object,
           + or a (datetime, event_string) tuple,
           + or a (datetime, (event1, event2, ...)) tuple.

        (event strings are needed to safely handle unlikely isotime collisions)

        so [(dateutil.parser.isoparse("2020-01-01T00:00:00"), ("asdf-asdf-sa-asdf","bvcd-grew")],
            (dateutil.parser.isoparse("2020-01-02T00:00:00"), "sads-gssd-hf-wert")]
        skips anything recorded on the first full day of January UNLESS the
        recording falls in the same moment as a range endpoint AND the
        recording's event ID is NOT in the associated list.

        The event IDs are optional, so a simpler case is [isodate_earlier,
        isodate_later] - this is safe if the exact isodates aren't a dataset
        you're augmenting and you can handle some ambiguity about whether the
        range is inclusive or exclusive.

        """
        if raw_location[0:len(self.s3_prefix)] == self.s3_prefix:
            raise RuntimeError("not yet implemented")

        print("raw_location:{}".format(raw_location))
        entries = os.scandir(raw_location)
        for entry in entries:
            if not self.skip_observation_by_filename(entry.name, skippers):
                self.process_w1logger_file(open(os.path.join(raw_location, entry), "r"))

    def skip_observation_by_filename(self, basename, skippers):
        isotime, entry_event = basename.split(';')
        entry_dt = dateutil.parser.isoparse(isotime)
        for row in skippers:
            if entry_dt >= row[0] and entry_dt < row[1]:
                return True
        return False

    def skippers_from_rollup_dir(self, rollup_location):
        skippers = []
        if rollup_location[0:len(self.s3_prefix)] == self.s3_prefix:
            raise RuntimeError("not yet implemented")
        for entry in os.scandir(rollup_location):
            mo = rollup_direntry_re.match(entry.name)
            if not mo:
                continue
            begin_dt = datetime(year=int(mo.group('year')), month=int(mo.group('month')), utc_offset=0)
            end_dt = begin_dt + relativedelta(months=1)
            skippers.append([[begin_dt,], [end_dt,]])
        return skippers  # TBD: event IDs

    def write_rollups(self):
        """Rollups idea is gathering individual observation blobs into a list of
        everything for a calendar month, so reprocessing raw data later is
        faster/cheaper and storage and transfer costs are lower. Have to do it
        to find out if these advantages are actually real.
        """
        pass

class Skippers:
    def __init__(self):
        self._specs = []

    class SkipperSpec:
        def __init__(self, end_datetime, end_entries, begin_datetime, begin_entries):
            self._begin = (begin_datetime, begin_entries)
            self._end = (end_datetime, end_entries)

        def is_skip(self, dt, entry):
            if dt < begin_datetime:
                return False
            if dt == begin_datetime:
                if entry in begin_entries:
                    return True
                return False
            if dt > end_datetime:
                return False
            if dt == end_datetime:
                if entry in end_entries:
                    return True
                return False
            return True

    def add_skip(self, end_datetime, end_entries=(), begin_datetime=None, begin_entries=()):
        if begin_datetime is None:
            begin_datetime = dateutil.parser.isoparse("1970-01-01T00:00:00")
        if begin_datetime > end_datetime:
            raise RuntimeError("Skip-range endpoints reversed")
        ss = SkipperSpec(end_datetime, begin_datetime, end_entries, begin_entries)
        self._specs.append(ss)

    def is_skip(self, dt, entry):
        for ss in self._specs:
            if ss.skip(dt, entry):
                return True
        return False

def do_rollup(rollup_location, raw_location):
    print("do_rollup(rollup_location:{}, raw_location:{})".format(rollup_location, raw_location))
    Observation.register_datapoint_handler("28", W1Datapoint_Linux_w1therm)
    observations = Observations()

    skippers = observations.skippers_from_rollup_dir(rollup_location)
    observations.process_w1logger_dir(raw_location, skippers)
    observations.write_rollups(rollup_location)
    return 0

def cli_rollup():
    """
    Maintain a dir of monthly rollup JSON blobs from dir of raw observation JSON blobs
    """
    p = argparse.ArgumentParser()
    p.add_argument('--debug', '-d', action='store_true')
    p.add_argument('rollup_location')
    p.add_argument('raw_location')
    a = p.parse_args()
    return do_rollup(a.rollup_location, a.raw_location)


def main():
    """
    CLI operation
    """
    p = argparse.ArgumentParser()
    p.add_argument('--debug', '-d', action='store_true')
    p.add_argument('--rollup', nargs=1)
    p.add_argument('--plot', action='store_true')
    p.add_argument('--update_database', action='store_true')
    p.add_argument('input', nargs='?', const='-')
    a = p.parse_args()

    Observation.register_datapoint_handler("28", W1Datapoint_Linux_w1therm)
    observations = Observations()

    if a.rollup:
        sys.exit(do_rollup(a.rollup[0], a.input))

    infile = None
    if a.input == '-':
        observations.process_w1logger_file(sys.stdin)
    elif os.path.isdir(a.input):
        observations.process_w1logger_dir(a.input)
    else:
        observations.process_w1logger_file(open(a.input, 'r'))

    for k in observations.observations.keys():
        print("{}: {} entries".format(k, len(observations.observations[k])))

    # Make separate passes for each output phase - work harder but keep the code simpler

    if a.plot:
        for key in observations.observations.keys():  # like "28-011912588b87/w1_slave"
            kn, _ = key.split('/')
            with open('{}.data'.format(kn), "w") as of:
                of.write("time temp\n")
                for obs in sorted(observations.observations[key]):
                    of.write("{} {}\n".format(obs.time_key, obs.datapoint.temp))

    if a.rollup:
        for key in observations.observations.keys():  # like "28-011912588b87/w1_slave"
            kn, _ = key.split('/')
        ncols = 1 + len(observations.observations)
        nrows = max(map(lambda x: len(x), observations.observations.values()))
        print("ncols:{} nrows:{}".format(ncols, nrows))
        rollup = numpy.empty([nrows, ncols], dtype=numpy.float64)
        print(rollup); sys.exit(0)

    return 0

if __name__ == '__main__':
    sys.exit(main())
