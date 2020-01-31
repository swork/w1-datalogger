import sys, os, os.path, argparse, json, datetime, subprocess
import requests

def isotime(timespec='seconds'):
    return datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc
    ).isoformat(timespec=timespec)

class W1Logger:
    def __init__(self, config):
        self.config = config

    def LogStartup(self):
        msg = {
            "isotime": isotime(),
            "uptime": subprocess.check_output("uptime")
        }
        requests.post(self.config.endpoint, json=msg, timeout=30)

    def LogStatus(self):
        msg = {
            "isotime": isotime(),
            "uptime": subprocess.check_output("uptime", shell=True),
            "free": subprocess.check_output("free", shell=True),
            "df": subprocess.check_output("df", shell=True),
            "netstat-an": subprocess.check_output("netstat -an", shell=True),
        }
        requests.post(self.config.endpoint, json=msg, timeout=30)

    def LogW1(self):
        msg = dict()
        msg["scan_start"] = isotime('milliseconds')

        devices_links = list()
        devices_link_dir = "/sys/bus/w1/drivers/w1_slave_driver"
        with os.scandir(devices_link_dir) as d:
            for entry in d:
                if entry.is_symlink():
                    devices_links.append(entry_name)

        for link in devices_links:
            datapoints = dict()
            datapoints[os.path.join(link, "w1_slave")] = os.path.join(devices_link_dir, link, "w1_slave")

        for datapoint in sorted(datapoints.keys()):
            with open(os.path.join(devices_links_dir, datapoints[datapoint]), "r") as point:
                msg[datapoint] = {
                    "isotime": isotime('milliseconds'),
                    "value": point.read()
                }

        msg["scan_end"] = isotime('milliseconds')
        requests.post(self.config.endpoint, json=msg, timeout=30)

class Config:
    def __init__(self, config_blob):
        self.config_blob = config_blob
        self.config = None

    @property
    def endpoint(self):
        if self.config is None:
            with open(self.config_blob.config, "r") as f:
                self.config = json.load(f)
        return self.config['Post']

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--status', action='store_true', default=False)
    p.add_argument('--startup', action='store_true', default=False)
    p.add_argument('--config', nargs=1, default=os.path.join(os.path.dirname(__file__), "datalogger.json"))
    a = p.parse_args()

    w1logger = W1Logger(Config(a.config))
    if a.status:
        w1logger.LogStatus()
    elif a.startup:
        w1logger.LogStartup()
    else:
        w1logger.LogW1()
    return 0

if __name__ == '__main__':
    sys.exit(main())
