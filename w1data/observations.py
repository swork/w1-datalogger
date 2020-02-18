#! /usr/bin/env python3

"""A w1logger data collecting gizmo is assigned an endpoint UUID. That maps to a
key prefix in the AWS bucket observations.ste.vework.org, and should be changed
when the configuration of sensors on the data collecting gizmo changes.

Associated with it is a map from sensor IDs (in Linux w1 format, like
"28-01234567") to sensor metadata, primarily a name. This'll be in file
key/METADATA.json along with other metadata related to the endpoint UUID and
associated gizmo.

Observations are JSON objects in files named for the UTC isotime of collection
and the AWS event ID supplied as context to AWS Lambda, separated by a
semicolon. Their format is discussed elsewhere. These observations accumulate
as frequently and as long as the data collection gizmo arranges.

So there might be several endpoint UUIDs (top-level key prefixes in
s3://observations.ste.vework.org/), some overlapping in time (when two or more
gizmos were operating simultaneously) and some disjoint in time (a gizmo gets
reconfigured and assigned a new endpoint UUID, or one gizmo dies and another
takes its place - again, with a new endpoint UUID).

To transition these datapoints into the problem domain and to reduce the
processing involved in working with them, we have a monthly rollup for each
sensor named in metadata. (This might be different sensors with different
hardware serial numbers at different times via different endpoints; similarly
the same sensor might appear in metadata for different datapoints via different
endpoints at disjoint times.) Each rollup is a file named for the datastream
(not for the sensor but for the quantity the sensor was measuring) and for the
calendar year and month of observations it contains.

Rollups are the end result of w1-datalogger processing, and the starting point
for actual consumption of the data they contain. They are updated
asynchronously with respect to data gathering, idempotently (but with heuristic
assumptions for performance) and lazily, driven by data consumers' demands. If
nobody asks about the data, observations accumulate and rollups go untouched.
"""

import sys, os, re, argparse, json, uuid
from datetime import datetime
import dateutil.parser
from dateutil import relativedelta
from .w1datapoint import W1Datapoint
from .common import location_is_s3
from .metadata import measurement_for_skey

import logging
logger = logging.getLogger(__name__)

class Observation:
    """An instance of a measurement at a specific time, together with metadata.

    A datapoint logged through w1datalogger is a blob of JSON that includes a
    time of collection, time of logging (might be different if buffering
    happened, etc.), a couple more time values coping with slow measurement
    retrievals, a UUID, a measurement key (the hardware address of a 1Wire
    device at present, but more in the future), a resulting value, and probably
    some other stuff. It is stored alongside a METADATA.json file that
    describes the data-gathering hardware, the data-logger instance and labels
    this stream of datapoints in the problem domain.

    This class is an intersection between a set of raw data observations plus
    associated metadata and downstream processing, including the middleware
    rollup classes that collect many observations into boiled-down monthly
    per-stream collections. Those are suitable for working with the gathered
    data at a low level; other consumers of Observation instances might be
    interested in working with the data stream environment (raw sensor data,
    timings, etc.).
    """
    _handlers = {}

    def __repr__(self):
        return "<Observation {} {} {}>".format(self.datetime.strftime("%FT%T"), self.sensor_key, self.datapoint.value)

    def __init__(self, isotime_str, sensor_key, value, event_uuid, metadata=None):
        """isotime_str is to whatever resolution makes sense. No promises if
        not explicitly UTC! sensor_key is like "28-011912588b87/w1_slave" for
        a Linux 1Wire endpoint. value is the raw sensor data, which will be
        converted to a data value by a handler matching the sensor_key. uuid is
        just that, a uniquifier (it's an AWS Lambda event_id in all existing
        uses). metadata is the content of METADATA.json in the dir containing
        the file that holds this Observation.
        """
        self.datetime = dateutil.parser.isoparse(isotime_str)
        self.time_key = self.datetime.timestamp()
        self.sensor_key = sensor_key
        self.uuid = event_uuid

        # Determine what data handler should process value.
        handler_key = None
        for _, handler in self._handlers.items():
            handler_key = handler.key_from_sensor_key(sensor_key)
            if handler_key:
                break

        # If sensor_key doesn't match any handler we know about, bomb out (fix
        # observations.py's registration scheme)
        if handler_key is None:
            raise W1Datapoint.ItAintMe("sensor_key {} doesn't parse".format(sensor_key))

        try:
            self.datapoint = self._handlers[handler_key](value)
        except KeyError:
            raise W1Datapoint.ItAintMe(
                "recognized a handler key scheme but no handler for {}".format(sensor_key))

        self.metadata = metadata

    def year_month_measurement(self):
        return (self.datetime.year,
                self.datetime.month,
                measurement_for_skey(self.sensor_key, self.metadata))

    def __lt__(self, other):
        return self.time_key < other.time_key

    @classmethod
    def register_datapoint_handler(cls, w1_type_str, obsCls):
        cls._handlers[w1_type_str] = obsCls

    @property
    def key(self):
        """Rollup uses this tuple to uniquely identify an observation in
        problem-domain terms.
        """
        return (self.datetime.utctimetuple(),
                self.uuid,
                self.metadata['collector']['sensors'][self.sensor_key]['name'])

class Observations:
    w1s_re = re.compile(r'^28-(?P<ser>[a-zA-Z0-9])+/w1_slave$')
    v1watershed = dateutil.parser.isoparse('2020-02-03T08:20:03+00:00')

    class NotADataObservation(Exception):
        pass

    def __init__(self, raw_location):
        self.observations = dict()
        self.raw_location = raw_location

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
        logger.debug("transform1 incoming:{}".format(obj))
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
        """OLD"""
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

            try:
                uuid = p['recording_event']
            except KeyError:
                uuid = uuid.uuid4()

            k = p['key']
            if k in self.observations:
                self.observations[k].append(Observation(
                    isotime, k, p['value'], uuid, self.current_metadata))
            else:
                self.observations[k] = [Observation(
                    isotime, k, p['value'], uuid, self.current_metadata)]

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

    def metadata(self, abs_dirname):
        mf_name = os.path.join(abs_dirname, "METADATA.json")
        logger.debug("Processing {}".format(mf_name))
        try:
            with open(mf_name, 'r') as mf:
                return json.load(mf)
        except IOError:
            logger.debug("No readable METADATA.json in dir {}".format(
                self.raw_location))
        except json.decoder.JSONDecodeError:
            logger.error("Broken JSON in {}: {}".format(mf_name, sys.exc_info()[1]))
            sys.exit(65)  # EX_DATAERR
        return {}

    def generate_dir(self, dirname, metadata_base):
        metadata = metadata_base.copy()
        metadata.update(self.metadata(dirname))
        logger.debug("dirname:{} metadata:{}".format(dirname, metadata))
        with os.scandir(dirname) as s:
            for entry in s:
                if not entry.is_file():
                    continue
                if not entry.name[-5:] == '.json':
                    continue
                if entry.name == 'METADATA.json':
                    continue

                abs_filename = os.path.join(dirname, entry.name)
                try:
                    with open(abs_filename, 'r') as f:
                        blob_list = json.load(f)
                except:
                    logger.exception()
                    continue

                for blob in blob_list:

                    # w1datalogger includes some logger health info for us to ignore.
                    if 'uptime' in blob:
                        continue

                    try:
                        dps = blob['datapoints']
                    except (KeyError, TypeError):
                        self.transform1(blob)
                        dps = blob['datapoints']
                    fallback_time = None
                    for p in dps:
                        try:
                            isotime = p['isotime']
                        except KeyError:
                            isotime = self.get_fallback_time(blob)

                        try:
                            p_uuid = p['recording_event']
                        except KeyError:
                            p_uuid = uuid.uuid4()

                        k = p['key']

                        yield Observation(isotime, k, p['value'], p_uuid, metadata)


    def generate_all(self):
        """Walk subdirs of self.raw_location, yielding Observation instances. Each
        subdir is an observer endpoint. Note METADATA.JSON files while walking;
        build a metadata object to be associated with each datapoint by
        overlaying subdir metadata onto parent-dir metadata. (Eventual rollups
        can coalesce these so they don't burn space, but that's not our problem
        here.)
        """
        metadata_base = self.metadata(self.raw_location)
        if location_is_s3(self.raw_location):
            raise RuntimeError("not yet implemented")

        logger.debug("raw_location:{}".format(self.raw_location))
        with os.scandir(self.raw_location) as s:
            for entry in s:
                if entry.is_dir():
                    child_dirname = os.path.join(self.raw_location, entry.name)
                    yield from self.generate_dir(child_dirname, metadata_base)

if __name__ == '__main__':
  if False:  # saving some old code here
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

    if a.rollup:
        for key in observations.observations.keys():  # like "28-011912588b87/w1_slave"
            kn, _ = key.split('/')
        ncols = 1 + len(observations.observations)
        nrows = max(map(lambda x: len(x), observations.observations.values()))
        print("ncols:{} nrows:{}".format(ncols, nrows))
        rollup = numpy.empty([nrows, ncols], dtype=numpy.float64)
        print(rollup); sys.exit(0)

    return 0
