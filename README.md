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

## Multipart transfers

Both `send` and `get` default to a single-request transfer. For large files, pass `-m` to split the transfer into 10 MB parts uploaded/downloaded in parallel — faster on high-bandwidth links.

```
netpipe send big.iso -m                  # multipart upload
netpipe get big.iso -m                   # multipart download (resumable)
netpipe send big.iso -m -w 16            # override worker count (default 8)
```

`-w/--workers` only applies with `-m` and sets the number of parallel part transfers.

## Updating config

`netpipe init` overwrites the entire config. To change a single value without re-entering the others, use `config set`:

```
netpipe config set endpoint https://new.example.com
netpipe config set key                       # prompts with hidden input
netpipe config set default shared
```

Omitting the value prompts for it (the key prompt hides input).

## Dev mode

Set `NETPIPE_DEV=1` to allow `http://` endpoints and rewrite presigned URL hosts to `localhost` (for LocalStack). Off by default — installed CLI is HTTPS-only.
