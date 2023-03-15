from datetime import timedelta


def time_delta_to_string(td: timedelta, accuracy: int = 1, suffix: str = "") -> str:
    parts = []
    if td >= timedelta(seconds=5):
        parts.append(f"{td.seconds%60}s")
    if td >= timedelta(minutes=1):
        parts.append(f"{(td.seconds//60)%60}m")
    if td >= timedelta(hours=1):
        parts.append(f"{(td.seconds//3600)%24}h")
    if td >= timedelta(days=1):
        parts.append(f"{td.days}d")
    if not parts:
        return "Just Now"
    parts.reverse()
    return "".join(parts[:accuracy]) + suffix
