import datetime
import random as rd
import threading as th

from model.Sensor import Sensor
from KafkaDAO import KafkaDAO

kfk = KafkaDAO(bootstrap_servers=['192.168.1.130:9092'])

def sensor_simulation(sensor):
    """Single-step simulation call (keeps backwards compatibility)."""
    sensor.update_sensor(rd.randint(20, 40))
    if rd.random() < 0.1:
        sensor.change_state()


def _sensor_worker(sensor, interval, stop_event):
    """Worker loop that updates a sensor periodically until stop_event is set."""
    while not stop_event.is_set():
        sensor.update_sensor(rd.randint(0, 40))
        if rd.random() < 0.1:
            sensor.change_state()
        # wait returns immediately if set, otherwise sleeps up to interval seconds
        sensorData = {
            'bay_id': sensor.getBayId(),
            'parking_id': sensor.getParkingId(),
            'level': sensor.getLevel(),
            'state': sensor.getState(),
            'last_event_ts': sensor.getLastEventTs().isoformat(),
            'updated_at': sensor.getUpdatedAt().isoformat(),
            'temperature': sensor.getMetricsTemperature(),
            'battery_pct': sensor.getMetricsBattery()
        }
        print(f"Sending sensor data: {sensorData}")
        with kfk:
            kfk.send_async('ProyectoUD1', sensorData, key=sensor.getBayId())

        stop_event.wait(interval)


def start_sensor_threads(sensors, interval=5):
    """Start n_threads worker threads to simulate `sensor` every `interval` seconds.

    Returns a tuple (stop_event, threads) where `stop_event.set()` will signal threads to stop.
    Threads are started as daemons.
    """
    stop_event = th.Event()
    threads = []
    for i, sensor in enumerate(sensors):
        t = th.Thread(target=_sensor_worker, args=(sensor, interval, stop_event), daemon=True, name=f"SensorWorker-{i}")
        t.start()
        threads.append(t)
    return stop_event, threads


def stop_sensor_threads(stop_event, threads=None, timeout=None):
    """Signal threads to stop and optionally join them.

    - stop_event: the Event returned by start_sensor_threads
    - threads: optional list of Thread objects to join
    - timeout: optional timeout (seconds) to wait for joins
    """
    stop_event.set()
    if threads:
        for t in threads:
            t.join(timeout)
    kfk.close()

if __name__ == "__main__":
    # simple test
    sensors = [Sensor("A1", "P1", 0, False, datetime.datetime.now(), datetime.datetime.now()), 
               Sensor("A2", "P1", 0, True, datetime.datetime.now(), datetime.datetime.now()),
               Sensor("A3", "P1", 1, False, datetime.datetime.now(), datetime.datetime.now()),
               Sensor("A4", "P1", 1, True, datetime.datetime.now(), datetime.datetime.now()),
               Sensor("A5", "P1", 2, False, datetime.datetime.now(), datetime.datetime.now()),
               Sensor("B1", "P1", 0, False, datetime.datetime.now(), datetime.datetime.now()),
               Sensor("B2", "P1", 0, True, datetime.datetime.now(), datetime.datetime.now()),
               Sensor("B3", "P1", 1, False, datetime.datetime.now(), datetime.datetime.now()),
               Sensor("B4", "P1", 1, True, datetime.datetime.now(), datetime.datetime.now()),
               Sensor("B5", "P1", 2, False, datetime.datetime.now(), datetime.datetime.now())]

    # test threads
    stop_event, threads = start_sensor_threads(sensors, interval=2)
    try:
        # Keep the main thread alive to let the daemon threads run indefinitely.
        # The program will run until it is interrupted (e.g., with Ctrl+C).
        print("Sensor simulation running. Press Ctrl+C to stop.")
        while True:
            # A loop with a short wait is more reliably interrupted by Ctrl+C.
            stop_event.wait(1)
    except KeyboardInterrupt:
        print("\nInterruption received, stopping threads...")
    finally:
        stop_sensor_threads(stop_event, threads, timeout=5)
        print("Threads stopped.")