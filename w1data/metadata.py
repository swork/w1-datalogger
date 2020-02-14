import logging, sys
logger = logging.getLogger(__name__)

def measurement_for_skey(sensor_key, metadata):
    # logger.debug("sensor_key:{} metadata:{}".format(sensor_key, metadata))
    return metadata['collector']['sensors'][sensor_key]['name']

