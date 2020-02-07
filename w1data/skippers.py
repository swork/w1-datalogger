import dateutil

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

