import re
from .w1datapoint import W1Datapoint
from .observations import Observation

import logging
logger = logging.getLogger(__name__.split('_',1)[0])  # lump with parent class

class W1Datapoint_Linux_w1therm(W1Datapoint):
    """
    String from type-28 (therm sensor) w1slave pseudofile, parsed. If the
    w1therm Linux kernel module is loaded the result includes that line parsed;
    else we calculate the temperature from the raw sensor data (TBD).

    This is from the perspective of the sensor alone: no notion here of semantics.
    """

    sensor_key_re = re.compile(r'''
        (?P<stype> [0-9a-fA-F]{1,2})
        -
        (?P<saddr> [0-9a-fA-F]+)
        /
        w1_slave
    ''', re.X)

    value_re = re.compile(r'''
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
        mo = self.value_re.match(w1_string)
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

    @property
    def value(self):
        return self.temp

    @classmethod
    def key_from_sensor_key(cls, sensor_key):
        mo = cls.sensor_key_re.match(sensor_key)
        # logger.debug("sensor_key:{} mo:{}".format(sensor_key, mo))
        if mo is not None:
            handler_key = "{}/w1_slave".format(mo.group('stype'))
            return handler_key
        return None

Observation.register_datapoint_handler("28/w1_slave", W1Datapoint_Linux_w1therm)
