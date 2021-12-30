"""Print the simulator outputs to stdout if DEBUG_MODE is enabled."""
from .globals import DEBUG_MODE


def debug_print(*text):
    """Print the text if in debug mode."""
    if DEBUG_MODE:
        print(text)
