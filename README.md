# smtp-receiver

A small persistent SMTP listener that accepts incoming mail and writes each complete message as a file in an inbox directory.

This first version is intentionally permissive: it accepts any sender and recipient, does not perform spam filtering, and only applies coarse rate limits to avoid overload.

## Behavior

- Listens on `SMTP_LISTEN_ADDR` (default `0.0.0.0:2525`).
- Supports basic SMTP commands: `HELO`/`EHLO`, `MAIL FROM`, `RCPT TO`, `DATA`, `RSET`, `NOOP`, and `QUIT`.
- Applies a global messages-per-minute limit and a per-envelope-sender messages-per-minute limit.
- Writes message data to a temporary directory first, then atomically renames the complete file into the inbox.
- Keeps temporary files outside the inbox by default, so future rsync jobs can read the inbox without seeing partial messages.
- Prepends envelope metadata headers before the received DATA payload.

## Configuration

Environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `SMTP_LISTEN_ADDR` | `0.0.0.0:2525` | TCP listen address. Use `0.0.0.0:25` in a privileged/container deployment. |
| `SMTP_INBOX_DIR` | `inbox` | Directory where complete `.eml` files are delivered. |
| `SMTP_TEMP_DIR` | sibling `.smtp-receiver-tmp` | Temporary write directory used before atomic rename into the inbox. |
| `SMTP_MAX_MESSAGE_BYTES` | `26214400` | Maximum SMTP DATA size. |
| `SMTP_GLOBAL_RATE_PER_MINUTE` | `600` | Global accepted `MAIL FROM` commands per rolling minute. Set `0` to disable. |
| `SMTP_SENDER_RATE_PER_MINUTE` | `60` | Per-sender accepted `MAIL FROM` commands per rolling minute. Set `0` to disable. |

## Local development

```sh
cargo fmt
cargo test
SMTP_LISTEN_ADDR=127.0.0.1:2525 SMTP_INBOX_DIR=tmp/inbox cargo run
```

Example manual smoke test:

```sh
nc 127.0.0.1 2525
```

Then type:

```smtp
EHLO local
MAIL FROM:<sender@example.test>
RCPT TO:<inbox@example.test>
DATA
Subject: hello

body
.
QUIT
```

## Deployment note

The service is intended to run later inside a new container on `k4`. No live container has been created yet.
