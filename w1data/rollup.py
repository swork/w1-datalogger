"""rollup.py

Maintain rollup files, monthly collections of data from raw observations,
processed for raw data weirdnesses into rows of (time, value) tuples.

Filename is year-month-measurement.json, where measurement is the name from raw
observation metadata.

"""

import re, os, datetime, json, sys
import dateutil

from .observations import Observations, Observation
from .w1datapoint_linux_w1therm import W1Datapoint_Linux_w1therm
from .common import location_is_s3, datetime_isoformat

import logging
logger = logging.getLogger(__name__)

class RollupMonthly:
    """Lazy tracker for rollup files and contents.

    cls.dbegin, cls.dend: relativedelta instances constructing the first moment
    of a month and the first moment of the month following, when added to a
    datetime instance.

    cls.filename_format: year-month-measurement.json, where "measurement" is
    the metadata .name associated with a sensor HW address, observer-endpoint
    instance. So a raw sensor might be called "office_air_temperature" - that
    string is "measurement". If the sensor is changed out, a new
    observer-endpoint observer with new metadata associates the new HW address
    with the previous "measurement". Metadata changes can reconstruct the time
    of change for later discovery of calibration problems, etc.

    cls.name_re: reverse procedure for year-month-measurement.json to component
    scalar.

    self.dt_begin: The month of this rollup file. All rows are stamped with
    this time or later.

    self.dt_end: The next month following this rollup file. All rows are
    stamped with a time earlier than this.

    self.rollup_location: The top dir for the collection of rollups. Children
    of this directory are named for measurement streams, and contain rollup
    files named for year-month-measurement.json.

    self.filename: The year-month-measurement.json filename for this rollup.
    Excludes any directory component - basename only. Our actual absolute
    filename is os.path.join(rollup_location, measurement_name, self.filename).

    """

    # Add this to a datetime to make it the first possible of the month
    dbegin = dateutil.relativedelta.relativedelta(
        day=1,
        hour=0,
        minute=0,
        second=0,
        microsecond=0)

    # Add this to a first-possible-of-the-month datetime to get a datetime that
    # is the first possible past the end of the month
    dend = dateutil.relativedelta.relativedelta(
        months=1)

    filename_format = "{:04}-{:02}-{}.json"
    name_re = re.compile(r'(?P<year> \d+) - (?P<month> \d+) - (?P<measurement> [^./]+) [.] json', re.X)

    @classmethod
    def dtbegin_for_datetime(dt):
        """datetime for earliest rollup file entry from a datetime object"""
        return datetime_isoformat(dt + self.dbegin)

    def __init__(self, rollup_location, dt_start, measurement_name):
        self.rollup_location = rollup_location
        self.dt_start = dt_start
        self.dt_end = dt_start + self.dend
        self.measurement_name = measurement_name
        self.filename = self.filename_format.format(dt_start.year, dt_start.month, measurement_name)
        self.metadata_series = []  # (earliest, dict), ...
        self._changed = False
        self._content = None

    def __delete__(self):
        self.flush()

    def flush(self):
        if self._changed:
            self.rewrite()

    def read_lazy(self):
        if self._content is None:
            filename = os.path.join(self.rollup_location, self.filename)
            try:
                with open(filename, 'r') as f:
                    try:
                        self._content = json.load(f)
                    except json.decoder.JSONDecodeError as e:
                        logger.error("Bad json in {}: {}".format(filename, sys.exc_info()[1]))
                        sys.exit(65)  # EX_DATAERR
                    except IOError:
                        logger.error("Couldn't read file {}: {}".format(filename, sys.exc_info()[1]))
            except IOError:
                self._content = {}

    def rewrite(self):
        if self._content is not None and self._changed:
            dirname = os.path.join(self.rollup_location, self.measurement_name)
            try:
                os.makedirs(dirname)
            except FileExistsError:
                pass
            # [0]: key: (datetime, uuid).
            # [1]: value: w1datapoint instance.
            # [2]: metadata[2] from METADATA.json parent U child
            content = sorted(map(
                lambda x: [datetime_isoformat(x[0][0]),x[1].value],
                self._content.items()))

            # [0]: earliest datetime at which associated metadata applies
            # [1]: metadata for all items past [0]
            meta = sorted(map(
                lambda x: [datetime_isoformat(x[0]),x[1]],
                self.metadata_series))

            # Rollup files: JSON blobs summarizing raw data
            filename = os.path.join(dirname, self.filename)
            try:
                with open(filename, 'w') as f:
                    try:
                        json.dump({
                            "metadata": meta,
                            "rows": content
                        }, f)
                    except:
                        logger.exception("Couldn't write {}: {}:{}".format(filename, sys.exc_info()[0], sys.exc_info()[1]))
                        raise
            except:
                os.unlink(filename)
                raise

            # GNUPlot data files with column headers
            plotfilename = filename.replace(".json", ".data")
            logger.debug("plotfilename: {}".format(plotfilename))
            try:
                with open(plotfilename, 'w') as pf:
                    pf.write('time "{}"\n'.format(self.measurement_name.replace("_", " ")))
                    for row in content:
                        pf.write("{} {}\n".format(dateutil.parser.isoparse(row[0]).timestamp(), row[1]))
            except:
                logger.warning("Couldn't write {}: {}:{}".format(filename, sys.exc_info()[0], sys.exc_info()[1]))

    def save_metadata(self, dt, metadata):
        for (index, (earliest, m)) in enumerate(self.metadata_series):
            # logger.debug("save_metadata. {} is {}, {}".format(index, earliest, m))
            if m == metadata:
                # logger.debug("save_metadata. equal")
                if dt < earliest:
                    # logger.debug("save_metadata. reassign as {}, {}".format(dt, m))
                    self.metadata_series[index] = (dt, m)
                return
        self.metadata_series.append((dt, metadata))
        self.metadata_series.sort()

    def add_row(self, row_time, row_uuid, row_value, metadata):
        self._changed = True
        if self._content is None:
            self.read_lazy()
        self.save_metadata(row_time, metadata)
        self._content[(row_time, row_uuid)] = row_value

class RollupMonthlyCollection:
    """Maintain a collection of monthly rollups.

    .rollup_location/measurement_name/year-month-measurement_name.json
    """

    def __init__(self, rollup_location):
        self._location = rollup_location
        self.collection = dict()
        if location_is_s3(rollup_location):
            raise RuntimeError("not yet implemented")
        for measurement_entry in os.scandir(rollup_location):
            if measurement_entry.is_dir():
                measurement_dir = os.path.join(rollup_location, measurement_entry.name)
                for entry in os.scandir(measurement_dir):
                    if entry.is_file():
                        mo = RollupMonthly.name_re.match(entry.name)
                        if mo:
                            dtb = datetime.datetime(year=int(mo.group('year')), month=int(mo.group('month')), day=1)
                            self.collection[(dtb.utctimetuple(), measurement_entry.name)] = RollupMonthly(rollup_location, dtb, measurement_entry.name)
                        else:
                            logger.debug('RMC init skipped file {}'.format(entry.name))
                    else:
                        logger.debug('RMC init skipped non-file {}'.format(entry.name))
            else:
                logger.debug('RMC init skipped non-dir {}'.format(measurement_entry.name))
        try:
            # logger.debug("first key: {}".format(list(self.collection.keys())[0]))
            self.most_recent_ymm_init = sorted(self.collection.keys())[-1]
        except IndexError:
            self.most_recent_ymm_init = None
        self.all_ymms_init = set(self.collection.keys())

    def flush(self):
        for _, collection in self.collection.items():
            collection.flush()

    def add_observation(self, observation, ymm):
        try:
            c = self.collection[ymm]
        except KeyError:
            dt = datetime.datetime(year=ymm[0], month=ymm[1], day=1, hour=0, minute=0, second=0)
            measurement = ymm[2]
            c = self.collection[ymm] = RollupMonthly(self._location, dt + RollupMonthly.dbegin, measurement)
        c.add_row(observation.datetime, observation.uuid, observation.datapoint, observation.metadata)

    def save_quick(self, observation):
        """Store this observation into an appropriate new or existing rollup, taking a
        shortcut: if rollup exists and isn't the most recent, assume the
        observation is already in it.
        """
        okey = observation.key
        ymm = observation.year_month_measurement()
        # logger.debug("ymm:{}".format(ymm))
        if ymm == self.most_recent_ymm_init:
            self.add_observation(observation, ymm)
        else:
            if ymm not in self.all_ymms_init:
                self.add_observation(observation, ymm)
            else:
                logger.debug("skipped okey:{} tuple:{}".format(okey, ymm))

def do_rollup(rollup_location, raw_location):
    logger.debug("do_rollup(rollup_location:{}, raw_location:{})".format(rollup_location, raw_location))
    rollup_collection = RollupMonthlyCollection(rollup_location)
    observations = Observations(raw_location)
    for observation in observations.generate_all():
        logger.debug("obs:{}".format(observation))
        rollup_collection.save_quick(observation)
    rollup_collection.flush()
