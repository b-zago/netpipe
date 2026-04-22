import functools

import click
import requests


def handle_errors(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except FileNotFoundError as e:
            raise click.ClickException(str(e))
        except RuntimeError as e:
            raise click.ClickException(str(e))
        except requests.exceptions.HTTPError as e:
            raise click.ClickException(f"Server error: {e}")
        except requests.exceptions.RequestException as e:
            raise click.ClickException(f"Connection error: {e}")
    return wrapper
