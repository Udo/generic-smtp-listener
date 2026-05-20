# generic-smtp-listener

A small persistent SMTP listener that accepts incoming mail and writes each complete message as a file in an inbox directory.

This project is intentionally permissive: it accepts any sender and recipient and does not perform spam filtering. It is intended for private capture/ingestion workflows, not as a full MTA replacement.

## Behavior

- Listens on `SMTP_LISTEN_ADDR` (default `0.0.0.0:2525`).
- Supports basic SMTP commands: `HELO`/`EHLO`, `MAIL FROM`, `RCPT TO`, `DATA`, `RSET`, `NOOP`, and `QUIT`.
- Applies coarse global and per-envelope-sender rate limits.
- Applies a concurrent connection limit and an idle command/DATA timeout.
- Writes message data to a temporary directory first, then atomically renames the complete file into the inbox.
- Also writes a cleaned copy into `SMTP_CLEANED_INBOX_DIR` for LLM ingestion.
- Keeps temporary files outside both inboxes by default, so rsync jobs can read inboxes without seeing partial messages.
- Prepends envelope metadata headers before the received DATA payload.
- Message filenames use `[YYYY]-[MM]-[DD]-[base64_url encoded sha1 content hash].eml`, based on the full stored message content.
- Cleaned copies strip `X-*`, `ARC-*`, and `DKIM*` headers except `X-Received` and `X-SMTP*`, keep plain-text bodies, drop HTML alternatives, and keep attachments with minimal MIME headers.

## Configuration

Environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `SMTP_LISTEN_ADDR` | `0.0.0.0:2525` | TCP listen address. Use `0.0.0.0:25` when deployed with permission to bind port 25. |
| `SMTP_INBOX_DIR` | `inbox` | Directory where complete `.eml` files are delivered. |
| `SMTP_CLEANED_INBOX_DIR` | sibling `inbox-cleaned` | Directory where stripped LLM-ingestion `.eml` files are delivered. |
| `SMTP_TEMP_DIR` | sibling `.smtp-receiver-tmp` | Temporary write directory used before atomic rename into the inboxes. |
| `SMTP_MAX_MESSAGE_BYTES` | `26214400` | Maximum SMTP DATA size. |
| `SMTP_GLOBAL_RATE_PER_MINUTE` | `600` | Global accepted `MAIL FROM` commands per rolling minute. Set `0` to disable. |
| `SMTP_SENDER_RATE_PER_MINUTE` | `60` | Per-sender accepted `MAIL FROM` commands per rolling minute. Set `0` to disable. |
| `SMTP_MAX_CONNECTIONS` | `100` | Maximum concurrent SMTP sessions. Set `0` to use Tokio's maximum semaphore permit count. |
| `SMTP_COMMAND_TIMEOUT_SECONDS` | `300` | Idle timeout for SMTP commands and DATA reads. |

## Local development

```sh
cargo fmt
cargo test
cargo clippy --all-targets -- -D warnings
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

## Security notes

- This listener is deliberately accepting. Put it behind appropriate network controls for your use case.
- The cleaned inbox is for token reduction, not for security sanitization. Treat email contents and attachments as untrusted input.
- The server does not implement authentication, TLS, spam filtering, DKIM/SPF/DMARC validation, mailbox routing, or outbound mail delivery.
