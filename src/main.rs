use std::collections::{HashMap, VecDeque};
use std::env;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::fs;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

static NEXT_ID: AtomicU64 = AtomicU64::new(1);
const MAX_COMMAND_LINE_BYTES: usize = 8 * 1024;

#[derive(Clone, Debug)]
struct Config {
    listen_addr: String,
    inbox_dir: PathBuf,
    temp_dir: PathBuf,
    max_message_bytes: usize,
    global_rate_per_minute: usize,
    sender_rate_per_minute: usize,
}

impl Config {
    fn from_env() -> io::Result<Self> {
        let listen_addr =
            env::var("SMTP_LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:2525".to_string());
        let inbox_dir =
            PathBuf::from(env::var("SMTP_INBOX_DIR").unwrap_or_else(|_| "inbox".to_string()));
        let temp_dir = match env::var("SMTP_TEMP_DIR") {
            Ok(value) => PathBuf::from(value),
            Err(_) => default_temp_dir_for(&inbox_dir),
        };

        Ok(Self {
            listen_addr,
            inbox_dir,
            temp_dir,
            max_message_bytes: parse_env_usize("SMTP_MAX_MESSAGE_BYTES", 25 * 1024 * 1024)?,
            global_rate_per_minute: parse_env_usize("SMTP_GLOBAL_RATE_PER_MINUTE", 600)?,
            sender_rate_per_minute: parse_env_usize("SMTP_SENDER_RATE_PER_MINUTE", 60)?,
        })
    }
}

#[derive(Debug)]
struct AppState {
    config: Config,
    limiter: Mutex<RateLimiter>,
}

impl AppState {
    fn new(config: Config) -> Self {
        let limiter = RateLimiter::new(
            config.global_rate_per_minute,
            config.sender_rate_per_minute,
            Duration::from_secs(60),
        );
        Self {
            config,
            limiter: Mutex::new(limiter),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RateLimitDecision {
    Accepted,
    GlobalLimited,
    SenderLimited,
}

#[derive(Debug)]
struct RateLimiter {
    global_limit: usize,
    sender_limit: usize,
    window: Duration,
    global: VecDeque<Instant>,
    senders: HashMap<String, VecDeque<Instant>>,
}

impl RateLimiter {
    fn new(global_limit: usize, sender_limit: usize, window: Duration) -> Self {
        Self {
            global_limit,
            sender_limit,
            window,
            global: VecDeque::new(),
            senders: HashMap::new(),
        }
    }

    fn check_and_record(&mut self, sender: &str, now: Instant) -> RateLimitDecision {
        prune_old(&mut self.global, now, self.window);
        if self.global_limit > 0 && self.global.len() >= self.global_limit {
            return RateLimitDecision::GlobalLimited;
        }

        let key = normalize_sender(sender);
        let sender_events = self.senders.entry(key).or_default();
        prune_old(sender_events, now, self.window);
        if self.sender_limit > 0 && sender_events.len() >= self.sender_limit {
            return RateLimitDecision::SenderLimited;
        }

        self.global.push_back(now);
        sender_events.push_back(now);
        RateLimitDecision::Accepted
    }
}

fn prune_old(events: &mut VecDeque<Instant>, now: Instant, window: Duration) {
    while events
        .front()
        .is_some_and(|event| now.duration_since(*event) >= window)
    {
        events.pop_front();
    }
}

fn normalize_sender(sender: &str) -> String {
    sender.trim().to_ascii_lowercase()
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let config = Config::from_env()?;
    fs::create_dir_all(&config.inbox_dir).await?;
    fs::create_dir_all(&config.temp_dir).await?;

    let listener = TcpListener::bind(&config.listen_addr).await?;
    eprintln!(
        "smtp-receiver listening on {} and delivering to {}",
        config.listen_addr,
        config.inbox_dir.display()
    );

    let state = Arc::new(AppState::new(config));
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                let state = Arc::clone(&state);
                tokio::spawn(async move {
                    if let Err(error) = handle_client(stream, state).await {
                        eprintln!("connection from {peer} ended with error: {error}");
                    }
                });
            }
            signal = tokio::signal::ctrl_c() => {
                signal?;
                eprintln!("shutdown signal received");
                return Ok(());
            }
        }
    }
}

async fn handle_client(stream: TcpStream, state: Arc<AppState>) -> io::Result<()> {
    let peer = stream.peer_addr().ok();
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = Vec::new();
    let mut sender: Option<String> = None;
    let mut recipients: Vec<String> = Vec::new();

    write_response(&mut writer, "220 smtp-receiver ready\r\n").await?;

    loop {
        line.clear();
        let bytes = match read_line_limited(&mut reader, &mut line, MAX_COMMAND_LINE_BYTES).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::InvalidData => {
                write_response(&mut writer, "500 command line too long\r\n").await?;
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        if bytes == 0 {
            return Ok(());
        }

        let command = String::from_utf8_lossy(trim_line_ending(&line));
        let upper = command.to_ascii_uppercase();

        if upper.starts_with("HELO ") || upper.starts_with("EHLO ") {
            write_response(
                &mut writer,
                "250-smtp-receiver\r\n250-8BITMIME\r\n250 SIZE\r\n",
            )
            .await?;
        } else if upper.starts_with("MAIL FROM:") {
            let proposed_sender = command[10..].trim().to_string();
            let decision = {
                let mut limiter = state.limiter.lock().await;
                limiter.check_and_record(&proposed_sender, Instant::now())
            };
            match decision {
                RateLimitDecision::Accepted => {
                    sender = Some(proposed_sender);
                    recipients.clear();
                    write_response(&mut writer, "250 sender accepted\r\n").await?;
                }
                RateLimitDecision::GlobalLimited => {
                    write_response(
                        &mut writer,
                        "451 global rate limit exceeded; try again later\r\n",
                    )
                    .await?;
                }
                RateLimitDecision::SenderLimited => {
                    write_response(
                        &mut writer,
                        "451 sender rate limit exceeded; try again later\r\n",
                    )
                    .await?;
                }
            }
        } else if upper.starts_with("RCPT TO:") {
            if sender.is_none() {
                write_response(&mut writer, "503 MAIL FROM required first\r\n").await?;
            } else {
                recipients.push(command[8..].trim().to_string());
                write_response(&mut writer, "250 recipient accepted\r\n").await?;
            }
        } else if upper == "DATA" {
            if sender.is_none() || recipients.is_empty() {
                write_response(&mut writer, "503 MAIL FROM and RCPT TO required first\r\n").await?;
                continue;
            }
            write_response(&mut writer, "354 end with <CRLF>.<CRLF>\r\n").await?;
            match read_data(&mut reader, state.config.max_message_bytes).await {
                Ok(data) => {
                    let sender_value = sender.as_deref().unwrap_or("");
                    match persist_message(&state.config, sender_value, &recipients, &data).await {
                        Ok(path) => {
                            eprintln!("accepted message from {:?} into {}", peer, path.display());
                            write_response(&mut writer, "250 message accepted\r\n").await?;
                        }
                        Err(error) => {
                            eprintln!("failed to persist message from {:?}: {error}", peer);
                            write_response(
                                &mut writer,
                                "451 local storage error; try again later\r\n",
                            )
                            .await?;
                        }
                    }
                    sender = None;
                    recipients.clear();
                }
                Err(ReadDataError::TooLarge) => {
                    write_response(&mut writer, "552 message exceeds configured size limit\r\n")
                        .await?;
                    return Ok(());
                }
                Err(ReadDataError::Io(error)) => return Err(error),
            }
        } else if upper == "RSET" {
            sender = None;
            recipients.clear();
            write_response(&mut writer, "250 reset ok\r\n").await?;
        } else if upper == "NOOP" {
            write_response(&mut writer, "250 ok\r\n").await?;
        } else if upper == "QUIT" {
            write_response(&mut writer, "221 bye\r\n").await?;
            return Ok(());
        } else {
            write_response(&mut writer, "502 command not implemented\r\n").await?;
        }
    }
}

async fn write_response<W>(writer: &mut W, response: &str) -> io::Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    writer.write_all(response.as_bytes()).await?;
    writer.flush().await
}

#[derive(Debug)]
enum ReadDataError {
    TooLarge,
    Io(io::Error),
}

impl From<io::Error> for ReadDataError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

async fn read_data<R>(reader: &mut R, max_message_bytes: usize) -> Result<Vec<u8>, ReadDataError>
where
    R: AsyncBufRead + Unpin,
{
    let mut message = Vec::new();
    let mut line = Vec::new();

    loop {
        line.clear();
        let remaining = max_message_bytes.saturating_sub(message.len());
        let bytes = match read_line_limited(reader, &mut line, remaining + 3).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::InvalidData => {
                return Err(ReadDataError::TooLarge);
            }
            Err(error) => return Err(ReadDataError::Io(error)),
        };
        if bytes == 0 {
            return Err(ReadDataError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed during DATA",
            )));
        }

        if is_data_terminator(&line) {
            return Ok(message);
        }

        let line_to_store = if line.starts_with(b"..") {
            &line[1..]
        } else {
            &line[..]
        };
        if message.len() + line_to_store.len() > max_message_bytes {
            return Err(ReadDataError::TooLarge);
        }
        message.extend_from_slice(line_to_store);
    }
}

async fn read_line_limited<R>(
    reader: &mut R,
    out: &mut Vec<u8>,
    max_bytes: usize,
) -> io::Result<usize>
where
    R: AsyncBufRead + Unpin,
{
    let mut total = 0;
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return Ok(total);
        }

        let newline_index = available.iter().position(|byte| *byte == b'\n');
        let take_len = newline_index.map_or(available.len(), |index| index + 1);
        if out.len() + take_len > max_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "line exceeds configured byte limit",
            ));
        }

        out.extend_from_slice(&available[..take_len]);
        reader.consume(take_len);
        total += take_len;

        if newline_index.is_some() {
            return Ok(total);
        }
    }
}

fn is_data_terminator(line: &[u8]) -> bool {
    line == b".\r\n" || line == b".\n" || line == b"."
}

async fn persist_message(
    config: &Config,
    sender: &str,
    recipients: &[String],
    data: &[u8],
) -> io::Result<PathBuf> {
    fs::create_dir_all(&config.inbox_dir).await?;
    fs::create_dir_all(&config.temp_dir).await?;

    let final_path = config.inbox_dir.join(new_message_filename());
    let temp_path = config
        .temp_dir
        .join(format!("{}.tmp", filename_for_path(&final_path)));
    let mut payload = Vec::new();
    payload.extend_from_slice(b"X-SMTP-Receiver-Envelope-From: ");
    payload.extend_from_slice(sender.as_bytes());
    payload.extend_from_slice(b"\r\n");
    for recipient in recipients {
        payload.extend_from_slice(b"X-SMTP-Receiver-Envelope-To: ");
        payload.extend_from_slice(recipient.as_bytes());
        payload.extend_from_slice(b"\r\n");
    }
    payload.extend_from_slice(b"\r\n");
    payload.extend_from_slice(data);

    fs::write(&temp_path, payload).await?;
    fs::rename(&temp_path, &final_path).await?;
    Ok(final_path)
}

fn filename_for_path(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("message.eml")
        .to_string()
}

fn new_message_filename() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0));
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    format!("{}.{:09}-{}.eml", now.as_secs(), now.subsec_nanos(), id)
}

fn trim_line_ending(line: &[u8]) -> &[u8] {
    line.strip_suffix(b"\r\n")
        .or_else(|| line.strip_suffix(b"\n"))
        .unwrap_or(line)
}

fn default_temp_dir_for(inbox_dir: &Path) -> PathBuf {
    let parent = inbox_dir
        .parent()
        .filter(|path| !path.as_os_str().is_empty());
    match parent {
        Some(parent) => parent.join(".smtp-receiver-tmp"),
        None => PathBuf::from(".smtp-receiver-tmp"),
    }
}

fn parse_env_usize(name: &str, default: usize) -> io::Result<usize> {
    match env::var(name) {
        Ok(value) => value.parse::<usize>().map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{name} must be a positive integer: {error}"),
            )
        }),
        Err(_) => Ok(default),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    #[test]
    fn rate_limiter_applies_global_and_sender_limits() {
        let now = Instant::now();
        let mut limiter = RateLimiter::new(2, 1, Duration::from_secs(60));

        assert_eq!(
            limiter.check_and_record("a@example.test", now),
            RateLimitDecision::Accepted
        );
        assert_eq!(
            limiter.check_and_record("a@example.test", now),
            RateLimitDecision::SenderLimited
        );
        assert_eq!(
            limiter.check_and_record("b@example.test", now),
            RateLimitDecision::Accepted
        );
        assert_eq!(
            limiter.check_and_record("c@example.test", now),
            RateLimitDecision::GlobalLimited
        );
    }

    #[test]
    fn rate_limiter_window_expires() {
        let now = Instant::now();
        let mut limiter = RateLimiter::new(1, 1, Duration::from_secs(60));

        assert_eq!(
            limiter.check_and_record("a@example.test", now),
            RateLimitDecision::Accepted
        );
        assert_eq!(
            limiter.check_and_record("a@example.test", now + Duration::from_secs(60)),
            RateLimitDecision::Accepted
        );
    }

    #[tokio::test]
    async fn read_data_unstuffs_dots_and_stops_at_terminator() {
        let input = b"Subject: test\r\n..starts with dot\r\n.\r\nignored\r\n";
        let mut reader = BufReader::new(&input[..]);

        let data = read_data(&mut reader, 1024).await.unwrap();
        assert_eq!(data, b"Subject: test\r\n.starts with dot\r\n");
    }

    #[tokio::test]
    async fn read_data_rejects_oversized_messages() {
        let input = b"123456\r\n.\r\n";
        let mut reader = BufReader::new(&input[..]);

        assert!(matches!(
            read_data(&mut reader, 5).await,
            Err(ReadDataError::TooLarge)
        ));
    }

    #[tokio::test]
    async fn persist_message_renames_into_inbox_only_when_complete() {
        let temp = TempDir::new().unwrap();
        let config = Config {
            listen_addr: "127.0.0.1:0".to_string(),
            inbox_dir: temp.path().join("inbox"),
            temp_dir: temp.path().join("tmp"),
            max_message_bytes: 1024,
            global_rate_per_minute: 10,
            sender_rate_per_minute: 10,
        };

        let path = persist_message(
            &config,
            "<sender@example.test>",
            &["<rcpt@example.test>".to_string()],
            b"Subject: hello\r\n\r\nbody\r\n",
        )
        .await
        .unwrap();

        assert!(path.starts_with(&config.inbox_dir));
        assert!(path.exists());
        let entries: Vec<_> = std::fs::read_dir(&config.inbox_dir).unwrap().collect();
        assert_eq!(entries.len(), 1);
        let content = std::fs::read(path).unwrap();
        assert!(content.starts_with(b"X-SMTP-Receiver-Envelope-From: <sender@example.test>\r\n"));
        assert!(content.ends_with(b"Subject: hello\r\n\r\nbody\r\n"));
    }

    #[tokio::test]
    async fn smtp_session_delivers_complete_message_file() {
        let temp = TempDir::new().unwrap();
        let config = test_config(temp.path(), 4096, 10, 10);
        let mut client = start_test_server(config.clone()).await;

        assert_eq!(
            read_smtp_response(&mut client).await,
            "220 smtp-receiver ready\r\n"
        );
        write_smtp(&mut client, "EHLO local\r\n").await;
        assert_eq!(
            read_smtp_response(&mut client).await,
            "250-smtp-receiver\r\n250-8BITMIME\r\n250 SIZE\r\n"
        );
        write_smtp(&mut client, "MAIL FROM:<sender@example.test>\r\n").await;
        assert_eq!(
            read_smtp_response(&mut client).await,
            "250 sender accepted\r\n"
        );
        write_smtp(&mut client, "RCPT TO:<inbox@example.test>\r\n").await;
        assert_eq!(
            read_smtp_response(&mut client).await,
            "250 recipient accepted\r\n"
        );
        write_smtp(&mut client, "DATA\r\n").await;
        assert_eq!(
            read_smtp_response(&mut client).await,
            "354 end with <CRLF>.<CRLF>\r\n"
        );
        write_smtp(&mut client, "Subject: integration\r\n\r\nhello\r\n.\r\n").await;
        assert_eq!(
            read_smtp_response(&mut client).await,
            "250 message accepted\r\n"
        );

        let files = inbox_files(&config.inbox_dir);
        assert_eq!(files.len(), 1);
        let content = std::fs::read(&files[0]).unwrap();
        assert!(content.starts_with(b"X-SMTP-Receiver-Envelope-From: <sender@example.test>\r\n"));
        assert!(
            content
                .windows(b"Subject: integration".len())
                .any(|window| window == b"Subject: integration")
        );
    }

    #[tokio::test]
    async fn smtp_session_does_not_expose_partial_message_in_inbox() {
        let temp = TempDir::new().unwrap();
        let config = test_config(temp.path(), 4096, 10, 10);
        let mut client = start_test_server(config.clone()).await;

        read_smtp_response(&mut client).await;
        write_smtp(&mut client, "MAIL FROM:<sender@example.test>\r\n").await;
        read_smtp_response(&mut client).await;
        write_smtp(&mut client, "RCPT TO:<inbox@example.test>\r\n").await;
        read_smtp_response(&mut client).await;
        write_smtp(&mut client, "DATA\r\n").await;
        read_smtp_response(&mut client).await;
        write_smtp(
            &mut client,
            "Subject: partial\r\n\r\nbody without terminator\r\n",
        )
        .await;

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(inbox_files(&config.inbox_dir).is_empty());

        write_smtp(&mut client, ".\r\n").await;
        assert_eq!(
            read_smtp_response(&mut client).await,
            "250 message accepted\r\n"
        );
        assert_eq!(inbox_files(&config.inbox_dir).len(), 1);
    }

    #[tokio::test]
    async fn smtp_session_enforces_rate_limits_without_writing_message() {
        let temp = TempDir::new().unwrap();
        let config = test_config(temp.path(), 4096, 1, 10);
        let mut client = start_test_server(config.clone()).await;

        read_smtp_response(&mut client).await;
        write_smtp(&mut client, "MAIL FROM:<one@example.test>\r\n").await;
        assert_eq!(
            read_smtp_response(&mut client).await,
            "250 sender accepted\r\n"
        );
        write_smtp(&mut client, "MAIL FROM:<two@example.test>\r\n").await;
        assert_eq!(
            read_smtp_response(&mut client).await,
            "451 global rate limit exceeded; try again later\r\n"
        );
        write_smtp(&mut client, "DATA\r\n").await;
        assert_eq!(
            read_smtp_response(&mut client).await,
            "503 MAIL FROM and RCPT TO required first\r\n"
        );
        assert!(inbox_files(&config.inbox_dir).is_empty());
    }

    #[tokio::test]
    async fn smtp_session_enforces_sender_rate_limit() {
        let temp = TempDir::new().unwrap();
        let config = test_config(temp.path(), 4096, 10, 1);
        let mut client = start_test_server(config).await;

        read_smtp_response(&mut client).await;
        write_smtp(&mut client, "MAIL FROM:<sender@example.test>\r\n").await;
        assert_eq!(
            read_smtp_response(&mut client).await,
            "250 sender accepted\r\n"
        );
        write_smtp(&mut client, "MAIL FROM:<sender@example.test>\r\n").await;
        assert_eq!(
            read_smtp_response(&mut client).await,
            "451 sender rate limit exceeded; try again later\r\n"
        );
    }

    fn test_config(root: &Path, max_message_bytes: usize, global: usize, sender: usize) -> Config {
        Config {
            listen_addr: "127.0.0.1:0".to_string(),
            inbox_dir: root.join("inbox"),
            temp_dir: root.join("tmp"),
            max_message_bytes,
            global_rate_per_minute: global,
            sender_rate_per_minute: sender,
        }
    }

    async fn start_test_server(config: Config) -> BufReader<TcpStream> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(AppState::new(config));
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_client(stream, state).await.unwrap();
        });
        BufReader::new(TcpStream::connect(addr).await.unwrap())
    }

    async fn write_smtp(client: &mut BufReader<TcpStream>, text: &str) {
        client.get_mut().write_all(text.as_bytes()).await.unwrap();
        client.get_mut().flush().await.unwrap();
    }

    async fn read_smtp_response(client: &mut BufReader<TcpStream>) -> String {
        let mut response = String::new();
        loop {
            let mut line = String::new();
            let bytes = client.read_line(&mut line).await.unwrap();
            assert_ne!(bytes, 0, "server closed connection before response");
            let is_last = line
                .as_bytes()
                .get(3)
                .is_none_or(|separator| *separator != b'-');
            response.push_str(&line);
            if is_last {
                return response;
            }
        }
    }

    fn inbox_files(inbox_dir: &Path) -> Vec<PathBuf> {
        if !inbox_dir.exists() {
            return Vec::new();
        }
        let mut files = std::fs::read_dir(inbox_dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        files.sort();
        files
    }
}
