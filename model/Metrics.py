import threading as th

class Metrics():
    """Container for sensor metrics. Thread-safe.

    Uses an internal lock to protect reads/writes to temperature and battery.
    """
    def __init__(self, temperature=0, battery_pct=100):
        self.temperature = temperature
        self.battery_pct = battery_pct
        self._lock = th.Lock()

    def setTemperature(self, temperature):
        with self._lock:
            self.temperature = temperature

    def setBattery(self, battery_pct):
        with self._lock:
            self.battery_pct = battery_pct

    def getTemperature(self):
        with self._lock:
            return self.temperature

    def getBattery(self):
        with self._lock:
            return self.battery_pct

    def low_battery(self):
        with self._lock:
            if self.battery_pct > 0:
                self.battery_pct -= 1