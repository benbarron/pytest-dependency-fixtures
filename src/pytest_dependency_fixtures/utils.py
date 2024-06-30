import threading


def delay(seconds: float = 0.075) -> None:
    """
    Wait for designated number of seconds.
    """
    threading.Event().wait(seconds)
