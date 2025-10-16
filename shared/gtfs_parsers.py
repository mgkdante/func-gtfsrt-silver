from datetime import datetime, timezone
from google.transit import gtfs_realtime_pb2

def _now_utc_iso():
    return datetime.now(timezone.utc).isoformat()

def parse_tripupdates(payload: bytes):
    feed = gtfs_realtime_pb2.FeedMessage(); feed.ParseFromString(payload)
    snap = _now_utc_iso()
    rows = []
    for e in feed.entity:
        if not e.HasField("trip_update"):
            continue
        tu, trip = e.trip_update, e.trip_update.trip
        vid = tu.vehicle.id if tu.HasField("vehicle") else None
        for stu in tu.stop_time_update:
            rows.append({
                "snapshot_utc": snap,
                "entity_id": e.id,
                "route_id": getattr(trip, "route_id", None) or None,
                "trip_id": getattr(trip, "trip_id", None) or None,
                "start_date": getattr(trip, "start_date", None) or None,
                "stop_id": getattr(stu, "stop_id", None) or None,
                "arrival_delay": (stu.arrival.delay 
                                  if stu.HasField("arrival") and stu.arrival.HasField("delay") else None),
                "departure_delay": (stu.departure.delay 
                                    if stu.HasField("departure") and stu.departure.HasField("delay") else None),
                "vehicle_id": vid
            })
    return rows

def parse_vehiclepositions(payload: bytes):
    feed = gtfs_realtime_pb2.FeedMessage(); feed.ParseFromString(payload)
    snap = _now_utc_iso()
    rows = []
    for e in feed.entity:
        if not e.HasField("vehicle"):
            continue
        v = e.vehicle
        pos = v.position if v.HasField("position") else None
        rows.append({
            "snapshot_utc": snap,
            "entity_id": e.id,
            "route_id": (v.trip.route_id if v.HasField("trip") else None),
            "trip_id": (v.trip.trip_id if v.HasField("trip") else None),
            "vehicle_id": (v.vehicle.id if v.HasField("vehicle") else None),
            "latitude": (getattr(pos, "latitude", None) if pos else None),
            "longitude": (getattr(pos, "longitude", None) if pos else None),
            "bearing": (getattr(pos, "bearing", None) if pos else None),
            "speed_mps": (getattr(pos, "speed", None) if pos else None)
        })
    return rows
