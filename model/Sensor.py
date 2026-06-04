import threading as th
import datetime
from model.Metrics import Metrics

class Sensor():
    def __init__(self, bay_id, parking_id, level, state, last_event_ts, updated_at, metrics=None):
        # avoid mutable default for Metrics
        if metrics is None:
            metrics = Metrics()

        self.bay_id = bay_id
        self.parking_id = parking_id
        self.level = level
        self.state = state
        self.last_event_ts = last_event_ts
        self.updated_at = updated_at
        self.metrics = metrics
        # lock to protect sensor-level updates (state, timestamps, metrics access if needed)
        self._lock = th.Lock()

    def setBayId(self, bay_id):
        self.bay_id = bay_id
    def setParkingId(self, parking_id):
        self.parking_id = parking_id
    def setLevel(self, level):
        self.level = level
    def setState(self, state):
        self.state = state
    def setLastEventTs(self, last_event_ts):
        self.last_event_ts = last_event_ts
    def setUpdatedAt(self, updated_at):
        self.updated_at = updated_at

    def setMetricsTemperature(self, temperature):
        self.metrics.setTemperature(temperature)
    def setMetricsBattery(self, battery_pct):
        self.metrics.setBattery(battery_pct)
    
    def getBayId(self):
        return self.bay_id
    def getParkingId(self):
        return self.parking_id
    def getLevel(self):
        return self.level
    def getState(self):
        return self.state
    def getLastEventTs(self):
        return self.last_event_ts
    def getUpdatedAt(self):
        return self.updated_at
    
    def getMetricsTemperature(self):
        return self.metrics.getTemperature()
    def getMetricsBattery(self):
        return self.metrics.getBattery()
    

    def change_state(self):
        """Toggle the sensor state in a thread-safe way and update last_event_ts."""
        with self._lock:
            self.state = not self.state
            self.last_event_ts = datetime.datetime.now()

    def update_sensor(self, temperature):
        """Update metrics and updated_at timestamp in a thread-safe way.

        Metrics methods are already thread-safe, but we also protect the updated_at
        assignment with the sensor lock to avoid races with change_state.
        """
        # update metrics (metrics has its own lock)
        self.metrics.setTemperature(temperature)
        self.metrics.low_battery()

        with self._lock:
            self.updated_at = datetime.datetime.now()