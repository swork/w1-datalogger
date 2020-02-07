import re, os
import skippers
from observations import Observations, Observation
from w1datapoint_linux_w1therm import W1Datapoint_Linux_w1therm

s3_re = re.compile(r'^s3:// (?P<bucket>[^/]+) /? (?P<key>.*?)$', re.X)

class RollupCollection:
    def __init__(self, rollup_location):
        self._location = rollup_location
        self._rollups = []
        mo = s3_re.match(rollup_location)
        if mo:
            raise RuntimeError("not yet implemented")
        for entry in os.scandir(rollup_location):
            self._rollups.append(os.path.join(rollup_location, entry))

    def get_ranges(self):
        skippers = []
        for entry in self._rollups:
            pre, base = os.path.split(entry)
            mo = rollup_direntry_re.match(base)
            if not mo:
                continue
            begin_dt = datetime(year=int(mo.group('year')), month=int(mo.group('month')), utc_offset=0)
            end_dt = begin_dt + relativedelta(months=1)
            skippers.append([[begin_dt,], [end_dt,]])
        return skippers  # TBD: event IDs

    def rollup_name_for_obs(self, earliest, rollup_location):
        pass

    def write_rollups(self, override_rollup_location=None):
        """Rollups idea is gathering individual observation blobs into a list of
        everything for a calendar month, so reprocessing raw data later is
        faster/cheaper and storage and transfer costs are lower. Have to do it
        to find out if these advantages are actually real.
        """
        if override_rollup_location:
            _rollup_location = override_rollup_location
        else:
            _rollup_location = self.rollup_location
        for sensor_key, sorted_observations in enumerate(observations.observations):
            earliest = sorted_observations.islice(0,1,False)[0]
            latest = sorted_observations.islice(0,1,True)[0]
            while True:
                run = self.rollup_name_for_obs(earliest, _rollup_location)
        mo = s3_re.match(_rollup_location)
        if mo:
            pass
        pass

def do_rollup(rollup_location, raw_location):
    print("do_rollup(rollup_location:{}, raw_location:{})".format(rollup_location, raw_location))
    Observation.register_datapoint_handler("28", W1Datapoint_Linux_w1therm)
    rollup_collection = RollupCollection(rollup_location)
    observations = Observations()
    observations.process_w1logger_dir(raw_location, rollup_collection.get_ranges())
    return rollup_collection.update(observations)

