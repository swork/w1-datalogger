import skippers
from w1data import s3_re

class RollupCollection:
    def __init__(self, rollup_location):
        self._location = rollup_location
        self._rollups = []
        mo = s3_re.match(rollup_location)
        if mo:
            raise RuntimeError("not yet implemented")
        for entry in os.scandir(rollup_location):
            self._rollups.append(os.path.join(rollup_location, entry))

    def get_ranges(self, rollup_location=self.rollup_location):
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

    def write_rollups(self, rollup_location=self.rollup_location):
        """Rollups idea is gathering individual observation blobs into a list of
        everything for a calendar month, so reprocessing raw data later is
        faster/cheaper and storage and transfer costs are lower. Have to do it
        to find out if these advantages are actually real.
        """
        for sensor_key, sorted_observations in enumerate(observations.observations):
            earliest = sorted_observations.islice(0,1,False)[0]
            latest = sorted_observations.islice(0,1,True)[0]
            while True:
                run = self.rollup_name_for_obs(earliest, rollup_location)
        mo = s3_re.match(rollup_location)
        if mo:
            pass
        pass

