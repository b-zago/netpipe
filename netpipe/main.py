#!/usr/bin/env python3
"""Netpipe: send files between friends via AWS."""

import json
import os
import stat
import time
from pathlib import Path

import click

from . import api
from .errors import handle_errors

__version__ = "0.1.0"

CONFIG_FILENAME = "config.json"
CONFIG_KEYS = ("endpoint", "access_key", "default_folder")


def _check_endpoint(endpoint: str) -> None:
    # api.DEV is the single dev-mode switch (also used for host rewriting).
    # When it's off, the endpoint must be https://.
    if api.DEV or endpoint.startswith("https://"):
        return
    raise click.ClickException(
        f"Insecure endpoint {endpoint!r}: must use https:// "
        "(set NETPIPE_DEV=1 to allow http:// for local development)."
    )


def get_config_path() -> Path:
    """Return the path to the config file."""
    return Path(click.get_app_dir("netpipe")) / CONFIG_FILENAME


def load_config() -> dict:
    """Load config from disk. Exits with an error if missing or invalid."""
    path = get_config_path()
    if not path.exists():
        raise click.ClickException(
            f"No config found at {path}. Run `netpipe init` first."
        )
    try:
        with path.open("r", encoding="utf-8") as f:
            config = json.load(f)
    except json.JSONDecodeError as e:
        raise click.ClickException(f"Config at {path} is not valid JSON: {e}")

    missing = [k for k in CONFIG_KEYS if k not in config]
    if missing:
        raise click.ClickException(
            f"Config at {path} is missing keys: {', '.join(missing)}. "
            "Run `netpipe init` to recreate it."
        )
    _check_endpoint(config["endpoint"])
    return config


def save_config(config: dict) -> Path:
    """Write config to disk with restrictive permissions. Returns the path."""
    path = get_config_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    # Write then chmod. On Windows, chmod on 0o600 is a no-op for group/other
    # bits but is harmless.
    with path.open("w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)
    try:
        os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)  # 0o600
    except OSError:
        pass
    return path


@click.group()
@click.version_option(version=__version__)
def cli():
    """Netpipe: send files between friends via AWS."""
    pass


@cli.command()
@click.option("--endpoint", prompt="API endpoint", help="Netpipe API endpoint URL.")
@click.option("--access-key", prompt="Access key", hide_input=True, help="Your access key.")
@click.option("--default-folder", prompt="Default folder", default="default",
              help="Folder used when --folder isn't passed.")
def init(endpoint, access_key, default_folder):
    """First-time setup: configure API endpoint and key."""
    _check_endpoint(endpoint)

    path = get_config_path()
    if path.exists():
        click.confirm(f"Config already exists at {path}. Overwrite?", abort=True)

    config = {
        "endpoint": endpoint,
        "access_key": access_key,
        "default_folder": default_folder,
    }
    saved_to = save_config(config)
    click.echo(f"Saved config to {saved_to}")


@cli.command()
@click.argument("file_path", type=click.Path(exists=True, dir_okay=False, readable=True))
@click.option("--folder", "-f", default=None, help="Shared folder to upload into.")
@click.option("--workers", "-w", default=None, type=click.IntRange(min=1), help="Parallel upload workers (default 8).")
@handle_errors
def send(file_path, folder, workers):
    """Upload a file to a shared folder."""
    config = load_config()
    folder = folder or config["default_folder"]
    kwargs = {"workers": workers} if workers is not None else {}

    t0 = time.monotonic()
    api.upload_file(config["endpoint"], config["access_key"], folder, file_path, **kwargs)
    elapsed = time.monotonic() - t0
    click.echo(f"Uploaded {file_path} to {folder} in {elapsed:.1f}s")

@cli.command()
@click.argument("filename")
@click.option("--folder", "-f", default=None, help="Shared folder to download from.")
@click.option("--output", "-o", type=click.Path(), help="Where to save the file.")
@click.option("--workers", "-w", default=None, type=click.IntRange(min=1), help="Parallel download workers (default 8).")
@handle_errors
def get(filename, folder, output, workers):
    """Download a file."""
    config = load_config()
    folder = folder or config["default_folder"]
    output = output or filename
    kwargs = {"workers": workers} if workers is not None else {}

    t0 = time.monotonic()
    data = api.get_download_parts(config["endpoint"], config["access_key"], folder, filename)
    api.download_file(data, output, **kwargs)
    elapsed = time.monotonic() - t0
    click.echo(f"Downloaded {filename} from {folder} in {elapsed:.1f}s")

@cli.command()
@click.argument("folder", required=False)
@click.option("--long", "-l", "long_", is_flag=True, help="Show detailed info.")
@handle_errors
def ls(folder, long_):
    """List files in a folder."""
    config = load_config()
    folder = folder or config["default_folder"]

    files = api.list_files(config["endpoint"],config["access_key"],folder)

    if long_:
        for f in files:
            click.echo(f"{f['size']:>12}  {f['modified']}  {f['key']}")
    else:
        for f in files:
            click.echo(f["key"])


if __name__ == "__main__":
    cli()