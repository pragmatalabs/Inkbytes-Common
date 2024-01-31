from datetime import datetime, timedelta


def generate_threshold_timestamp(n_minutes):
    current_GMT = datetime.utcnow()
    last_n_minutes = current_GMT - timedelta(minutes=n_minutes)
    last_n_minutes_start = datetime(
        last_n_minutes.year, last_n_minutes.month, last_n_minutes.day,
        last_n_minutes.hour, last_n_minutes.minute // n_minutes * n_minutes, 0
    )
    timestamp = int(last_n_minutes_start.timestamp())
    return timestamp


