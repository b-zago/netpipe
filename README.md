# netpipe

CLI for uploading to and downloading from a personal netpipe server on AWS. Parallel multipart transfers, per-part retries, resumable downloads.

## Install

```
pipx install git+https://github.com/b-zago/netpipe
```

Requires Python 3.10+ and [pipx](https://pipx.pypa.io/).

## Setup

```
netpipe init
```

Prompts for the API endpoint (https://...), your access key, and a default folder.

## Usage

```
netpipe send path/to/file.iso            # upload to the default folder
netpipe send file.iso -f shared          # upload to a specific folder
netpipe get file.iso                     # download
netpipe get file.iso -o local-name.iso   # download with a different filename
netpipe ls                               # list the default folder
netpipe ls shared -l                     # long listing
```

## Dev mode

Set `NETPIPE_DEV=1` to allow `http://` endpoints and rewrite presigned URL hosts to `localhost` (for LocalStack). Off by default — installed CLI is HTTPS-only.
