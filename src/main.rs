use std::collections::{BTreeSet, HashMap, VecDeque};
use std::env;
use std::fs as stdfs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use base64::Engine;
use base64::engine::general_purpose::{STANDARD as BASE64_STANDARD, URL_SAFE_NO_PAD};
use chrono::Utc;
use sha1::{Digest, Sha1};
use tokio::fs;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Semaphore};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);
const MAX_COMMAND_LINE_BYTES: usize = 8 * 1024;

#[derive(Clone, Debug)]
struct Config {
    listen_addr: String,
    inbox_dir: PathBuf,
    cleaned_inbox_dir: PathBuf,
    temp_dir: PathBuf,
    max_message_bytes: usize,
    global_rate_per_minute: usize,
    sender_rate_per_minute: usize,
    max_connections: usize,
    command_timeout_seconds: u64,
    recipient_domains: Vec<String>,
    auth_results: AuthResultsConfig,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AuthResultsConfig {
    mode: AuthResultsMode,
    trusted_servers: Vec<String>,
    required_results: Vec<String>,
    match_mode: AuthResultsMatchMode,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum AuthResultsMode {
    Off,
    Require,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum AuthResultsMatchMode {
    Any,
    All,
}

impl Config {
    fn from_env() -> io::Result<Self> {
        Self::from_values(load_config_values()?)
    }

    fn from_values(values: ConfigValues) -> io::Result<Self> {
        let listen_addr = config_string(&values, "SMTP_LISTEN_ADDR", "0.0.0.0:2525");
        let inbox_dir = PathBuf::from(config_string(&values, "SMTP_INBOX_DIR", "inbox"));
        let cleaned_inbox_dir = PathBuf::from(
            config_optional(&values, "SMTP_CLEANED_INBOX_DIR")
                .unwrap_or_else(|| default_cleaned_dir_for(&inbox_dir)),
        );
        let temp_dir = config_optional(&values, "SMTP_TEMP_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| default_temp_dir_for(&inbox_dir));

        Ok(Self {
            listen_addr,
            inbox_dir,
            cleaned_inbox_dir,
            temp_dir,
            max_message_bytes: parse_config_usize(
                &values,
                "SMTP_MAX_MESSAGE_BYTES",
                25 * 1024 * 1024,
            )?,
            global_rate_per_minute: parse_config_usize(
                &values,
                "SMTP_GLOBAL_RATE_PER_MINUTE",
                600,
            )?,
            sender_rate_per_minute: parse_config_usize(&values, "SMTP_SENDER_RATE_PER_MINUTE", 60)?,
            max_connections: parse_config_usize(&values, "SMTP_MAX_CONNECTIONS", 100)?,
            command_timeout_seconds: parse_config_u64(
                &values,
                "SMTP_COMMAND_TIMEOUT_SECONDS",
                300,
            )?,
            recipient_domains: parse_recipient_domains(config_optional(
                &values,
                "SMTP_RECIPIENT_DOMAINS",
            )),
            auth_results: parse_auth_results_config(&values)?,
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
        self.senders.retain(|_, events| {
            prune_old(events, now, self.window);
            !events.is_empty()
        });
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

type ConfigValues = HashMap<String, String>;

fn load_config_values() -> io::Result<ConfigValues> {
    let mut values = match config_file_path()? {
        Some(path) => parse_config_file(&path)?,
        None => ConfigValues::new(),
    };

    for key in CONFIG_KEYS {
        if let Ok(value) = env::var(key) {
            values.insert((*key).to_string(), value);
        }
    }
    Ok(values)
}

const CONFIG_KEYS: &[&str] = &[
    "SMTP_LISTEN_ADDR",
    "SMTP_INBOX_DIR",
    "SMTP_CLEANED_INBOX_DIR",
    "SMTP_TEMP_DIR",
    "SMTP_MAX_MESSAGE_BYTES",
    "SMTP_GLOBAL_RATE_PER_MINUTE",
    "SMTP_SENDER_RATE_PER_MINUTE",
    "SMTP_MAX_CONNECTIONS",
    "SMTP_COMMAND_TIMEOUT_SECONDS",
    "SMTP_RECIPIENT_DOMAINS",
    "SMTP_AUTH_RESULTS_MODE",
    "SMTP_AUTH_RESULTS_TRUSTED_SERVERS",
    "SMTP_AUTH_RESULTS_REQUIRED",
    "SMTP_AUTH_RESULTS_MATCH",
];

fn config_file_path() -> io::Result<Option<PathBuf>> {
    if let Ok(value) = env::var("SMTP_CONFIG_FILE") {
        let path = PathBuf::from(value);
        if path.as_os_str().is_empty() {
            return Ok(None);
        }
        return Ok(Some(path));
    }

    let default_path = PathBuf::from("/etc/smtp-receiver/config");
    match stdfs::metadata(&default_path) {
        Ok(metadata) if metadata.is_file() => Ok(Some(default_path)),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} is not a regular file", default_path.display()),
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn parse_config_file(path: &Path) -> io::Result<ConfigValues> {
    let content = stdfs::read_to_string(path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("failed to read config file {}: {error}", path.display()),
        )
    })?;
    parse_config_text(&content)
}

fn parse_config_text(content: &str) -> io::Result<ConfigValues> {
    let mut values = ConfigValues::new();
    for (index, raw_line) in content.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("config line {} must use key=value syntax", index + 1),
            ));
        };
        let key = normalize_config_key(key.trim());
        if key.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("config line {} has an empty key", index + 1),
            ));
        }
        values.insert(key, trim_config_value(value.trim()).to_string());
    }
    Ok(values)
}

fn normalize_config_key(key: &str) -> String {
    let normalized = key.trim().replace('-', "_").to_ascii_uppercase();
    if normalized.starts_with("SMTP_") {
        normalized
    } else {
        format!("SMTP_{normalized}")
    }
}

fn trim_config_value(value: &str) -> &str {
    value
        .strip_prefix('"')
        .and_then(|without_prefix| without_prefix.strip_suffix('"'))
        .or_else(|| {
            value
                .strip_prefix('\'')
                .and_then(|without_prefix| without_prefix.strip_suffix('\''))
        })
        .unwrap_or(value)
}

fn config_optional(values: &ConfigValues, key: &str) -> Option<String> {
    values.get(key).cloned().filter(|value| !value.is_empty())
}

fn config_string(values: &ConfigValues, key: &str, default: &str) -> String {
    config_optional(values, key).unwrap_or_else(|| default.to_string())
}

fn parse_recipient_domains(value: Option<String>) -> Vec<String> {
    value
        .unwrap_or_default()
        .split(',')
        .map(|domain| domain.trim().trim_start_matches('@').to_ascii_lowercase())
        .filter(|domain| !domain.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn parse_auth_results_config(values: &ConfigValues) -> io::Result<AuthResultsConfig> {
    let mode = match config_string(values, "SMTP_AUTH_RESULTS_MODE", "off")
        .to_ascii_lowercase()
        .as_str()
    {
        "off" | "none" | "disabled" => AuthResultsMode::Off,
        "require" | "required" => AuthResultsMode::Require,
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("SMTP_AUTH_RESULTS_MODE must be off or require, got {other}"),
            ));
        }
    };
    let match_mode = match config_string(values, "SMTP_AUTH_RESULTS_MATCH", "any")
        .to_ascii_lowercase()
        .as_str()
    {
        "any" => AuthResultsMatchMode::Any,
        "all" => AuthResultsMatchMode::All,
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("SMTP_AUTH_RESULTS_MATCH must be any or all, got {other}"),
            ));
        }
    };

    Ok(AuthResultsConfig {
        mode,
        trusted_servers: parse_csv_lowercase(config_optional(
            values,
            "SMTP_AUTH_RESULTS_TRUSTED_SERVERS",
        )),
        required_results: parse_csv_lowercase(config_optional(
            values,
            "SMTP_AUTH_RESULTS_REQUIRED",
        )),
        match_mode,
    })
}

fn parse_csv_lowercase(value: Option<String>) -> Vec<String> {
    value
        .unwrap_or_default()
        .split(',')
        .map(|item| item.trim().to_ascii_lowercase())
        .filter(|item| !item.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let config = Config::from_env()?;
    fs::create_dir_all(&config.inbox_dir).await?;
    fs::create_dir_all(&config.cleaned_inbox_dir).await?;
    fs::create_dir_all(&config.temp_dir).await?;

    let listener = TcpListener::bind(&config.listen_addr).await?;
    eprintln!(
        "smtp-receiver listening on {} and delivering to {}",
        config.listen_addr,
        config.inbox_dir.display()
    );

    let semaphore = Arc::new(Semaphore::new(connection_limit(config.max_connections)));
    let state = Arc::new(AppState::new(config));
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (mut stream, peer) = accepted?;
                let permit = match Arc::clone(&semaphore).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        tokio::spawn(async move {
                            let _ = stream.write_all(b"421 too many concurrent connections; try again later\r\n").await;
                            let _ = stream.shutdown().await;
                        });
                        continue;
                    }
                };
                let state = Arc::clone(&state);
                tokio::spawn(async move {
                    let _permit = permit;
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
        let timeout_duration = Duration::from_secs(state.config.command_timeout_seconds);
        let bytes = match tokio::time::timeout(
            timeout_duration,
            read_line_limited(&mut reader, &mut line, MAX_COMMAND_LINE_BYTES),
        )
        .await
        {
            Ok(Ok(bytes)) => bytes,
            Ok(Err(error)) if error.kind() == io::ErrorKind::InvalidData => {
                write_response(&mut writer, "500 command line too long\r\n").await?;
                return Ok(());
            }
            Ok(Err(error)) => return Err(error),
            Err(_) => {
                write_response(&mut writer, "421 idle timeout\r\n").await?;
                return Ok(());
            }
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
            match tokio::time::timeout(
                Duration::from_secs(state.config.command_timeout_seconds),
                read_data(&mut reader, state.config.max_message_bytes),
            )
            .await
            {
                Ok(Ok(data)) => {
                    let sender_value = sender.as_deref().unwrap_or("");
                    match persist_message(&state.config, sender_value, &recipients, &data).await {
                        Ok(DeliveryOutcome::Stored(paths)) => {
                            eprintln!(
                                "accepted message from {:?} into {} recipient folder(s)",
                                peer,
                                paths.len()
                            );
                            write_response(&mut writer, "250 message accepted\r\n").await?;
                        }
                        Ok(DeliveryOutcome::IgnoredNoMatchingRecipient) => {
                            eprintln!(
                                "ignored message from {:?}: no configured recipient domain",
                                peer
                            );
                            write_response(&mut writer, "250 message accepted\r\n").await?;
                        }
                        Ok(DeliveryOutcome::IgnoredAuthResults) => {
                            eprintln!(
                                "ignored message from {:?}: authentication-results policy not satisfied",
                                peer
                            );
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
                Ok(Err(ReadDataError::TooLarge)) => {
                    write_response(&mut writer, "552 message exceeds configured size limit\r\n")
                        .await?;
                    return Ok(());
                }
                Ok(Err(ReadDataError::Io(error))) => return Err(error),
                Err(_) => {
                    write_response(&mut writer, "421 DATA timeout\r\n").await?;
                    return Ok(());
                }
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

#[derive(Debug, PartialEq, Eq)]
enum DeliveryOutcome {
    Stored(Vec<PathBuf>),
    IgnoredNoMatchingRecipient,
    IgnoredAuthResults,
}

async fn persist_message(
    config: &Config,
    sender: &str,
    recipients: &[String],
    data: &[u8],
) -> io::Result<DeliveryOutcome> {
    fs::create_dir_all(&config.inbox_dir).await?;
    fs::create_dir_all(&config.cleaned_inbox_dir).await?;
    fs::create_dir_all(&config.temp_dir).await?;

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

    if !message_satisfies_auth_results_policy(&config.auth_results, data) {
        return Ok(DeliveryOutcome::IgnoredAuthResults);
    }

    let recipient_folders =
        recipient_folders_for_message(&config.recipient_domains, recipients, data);
    if !config.recipient_domains.is_empty() && recipient_folders.is_empty() {
        return Ok(DeliveryOutcome::IgnoredNoMatchingRecipient);
    }

    let filename = message_filename(&payload);
    let cleaned_payload = clean_message_for_llm(&payload);
    let target_dirs = if config.recipient_domains.is_empty() {
        vec![(config.inbox_dir.clone(), config.cleaned_inbox_dir.clone())]
    } else {
        recipient_folders
            .into_iter()
            .map(|folder| {
                (
                    config.inbox_dir.join(&folder),
                    config.cleaned_inbox_dir.join(&folder),
                )
            })
            .collect()
    };

    let delivery_id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let mut written_paths = Vec::new();
    let mut written_cleaned_paths = Vec::new();
    for (index, (inbox_dir, cleaned_inbox_dir)) in target_dirs.into_iter().enumerate() {
        fs::create_dir_all(&inbox_dir).await?;
        fs::create_dir_all(&cleaned_inbox_dir).await?;
        let final_path = inbox_dir.join(&filename);
        let cleaned_final_path = cleaned_inbox_dir.join(&filename);
        let temp_path = config
            .temp_dir
            .join(format!("{filename}.{delivery_id}.{index}.tmp"));
        let cleaned_temp_path = config
            .temp_dir
            .join(format!("{filename}.{delivery_id}.{index}.cleaned.tmp"));

        fs::write(&temp_path, &payload).await?;
        fs::write(&cleaned_temp_path, &cleaned_payload).await?;
        if let Err(error) = fs::rename(&temp_path, &final_path).await {
            let _ = fs::remove_file(&temp_path).await;
            let _ = fs::remove_file(&cleaned_temp_path).await;
            cleanup_written_files(&written_paths, &written_cleaned_paths).await;
            return Err(error);
        }
        if let Err(error) = fs::rename(&cleaned_temp_path, &cleaned_final_path).await {
            let _ = fs::remove_file(&cleaned_temp_path).await;
            let _ = fs::remove_file(&final_path).await;
            cleanup_written_files(&written_paths, &written_cleaned_paths).await;
            return Err(error);
        }
        written_paths.push(final_path);
        written_cleaned_paths.push(cleaned_final_path);
    }
    Ok(DeliveryOutcome::Stored(written_paths))
}

async fn cleanup_written_files(paths: &[PathBuf], cleaned_paths: &[PathBuf]) {
    for path in paths {
        let _ = fs::remove_file(path).await;
    }
    for path in cleaned_paths {
        let _ = fs::remove_file(path).await;
    }
}

fn message_satisfies_auth_results_policy(config: &AuthResultsConfig, data: &[u8]) -> bool {
    if config.mode == AuthResultsMode::Off {
        return true;
    }
    if config.trusted_servers.is_empty() || config.required_results.is_empty() {
        return false;
    }

    let (header_bytes, _) = split_headers_body(data);
    parse_headers(header_bytes)
        .into_iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("authentication-results"))
        .any(|(_, value)| auth_results_header_satisfies(config, &value))
}

fn auth_results_header_satisfies(config: &AuthResultsConfig, value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    let authserv_id = lower
        .split(';')
        .next()
        .unwrap_or("")
        .split_whitespace()
        .next()
        .unwrap_or("")
        .trim_end_matches('.');
    if !config
        .trusted_servers
        .iter()
        .any(|trusted| trusted.trim_end_matches('.') == authserv_id)
    {
        return false;
    }

    match config.match_mode {
        AuthResultsMatchMode::Any => config
            .required_results
            .iter()
            .any(|required| lower.contains(required)),
        AuthResultsMatchMode::All => config
            .required_results
            .iter()
            .all(|required| lower.contains(required)),
    }
}

fn recipient_folders_for_message(
    recipient_domains: &[String],
    envelope_recipients: &[String],
    data: &[u8],
) -> Vec<String> {
    if recipient_domains.is_empty() {
        return Vec::new();
    }

    let mut folders = BTreeSet::new();
    for recipient in envelope_recipients {
        collect_recipient_folders(recipient, recipient_domains, &mut folders);
    }

    let (header_bytes, _) = split_headers_body(data);
    for (name, value) in parse_headers(header_bytes) {
        if name.eq_ignore_ascii_case("to")
            || name.eq_ignore_ascii_case("cc")
            || name.eq_ignore_ascii_case("bcc")
        {
            collect_recipient_folders(&value, recipient_domains, &mut folders);
        }
    }

    folders.into_iter().collect()
}

fn collect_recipient_folders(
    text: &str,
    recipient_domains: &[String],
    folders: &mut BTreeSet<String>,
) {
    for at_index in text.match_indices('@').map(|(index, _)| index) {
        let local_start = local_part_start(text, at_index);
        let domain_end = domain_end(text, at_index + 1);
        if local_start == at_index || domain_end == at_index + 1 {
            continue;
        }
        let domain = text[at_index + 1..domain_end].to_ascii_lowercase();
        if !recipient_domains.iter().any(|wanted| wanted == &domain) {
            continue;
        }
        let local = &text[local_start..at_index];
        let username = local
            .split_once('+')
            .or_else(|| local.split_once('#'))
            .map_or(local, |(base, _)| base);
        if is_safe_folder_name(username) {
            folders.insert(username.to_ascii_lowercase());
        }
    }
}

fn local_part_start(text: &str, at_index: usize) -> usize {
    let bytes = text.as_bytes();
    let mut index = at_index;
    while index > 0 && is_local_part_byte(bytes[index - 1]) {
        index -= 1;
    }
    index
}

fn domain_end(text: &str, start: usize) -> usize {
    let bytes = text.as_bytes();
    let mut index = start;
    while index < bytes.len() && is_domain_byte(bytes[index]) {
        index += 1;
    }
    index
}

fn is_local_part_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'%' | b'+' | b'#' | b'-')
}

fn is_domain_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-')
}

fn is_safe_folder_name(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

#[derive(Debug, Clone)]
struct MessagePart<'a> {
    headers: Vec<(String, String)>,
    body: &'a [u8],
}

#[derive(Debug, Clone)]
struct KeptPart {
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    is_attachment: bool,
}

fn clean_message_for_llm(message: &[u8]) -> Vec<u8> {
    let root = parse_message_part(message);
    let original_part;
    let content_part = if has_smtp_receiver_envelope(&root.headers) {
        original_part = parse_message_part(root.body);
        &original_part
    } else {
        &root
    };

    let mut top_headers = filter_headers(&root.headers);
    if has_smtp_receiver_envelope(&root.headers) {
        top_headers.extend(filter_headers(&content_part.headers));
    }
    remove_header(&mut top_headers, "content-type");
    remove_header(&mut top_headers, "content-transfer-encoding");
    remove_header(&mut top_headers, "mime-version");

    let kept_parts = collect_llm_parts(content_part);
    let mut output = Vec::new();
    write_headers(&mut output, &top_headers);

    if kept_parts.len() <= 1 && !kept_parts.first().is_some_and(|part| part.is_attachment) {
        output.extend_from_slice(b"Content-Type: text/plain; charset=utf-8\r\n\r\n");
        if let Some(part) = kept_parts.first() {
            output.extend_from_slice(&part.body);
        }
        return output;
    }

    let boundary = format!(
        "smtp-receiver-cleaned-{}",
        NEXT_ID.fetch_add(1, Ordering::Relaxed)
    );
    output.extend_from_slice(b"MIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary=\"");
    output.extend_from_slice(boundary.as_bytes());
    output.extend_from_slice(b"\"\r\n\r\n");

    for part in kept_parts {
        output.extend_from_slice(b"--");
        output.extend_from_slice(boundary.as_bytes());
        output.extend_from_slice(b"\r\n");
        write_headers(&mut output, &part.headers);
        output.extend_from_slice(b"\r\n");
        output.extend_from_slice(&part.body);
        if !part.body.ends_with(b"\n") {
            output.extend_from_slice(b"\r\n");
        }
    }
    output.extend_from_slice(b"--");
    output.extend_from_slice(boundary.as_bytes());
    output.extend_from_slice(b"--\r\n");
    output
}

fn has_smtp_receiver_envelope(headers: &[(String, String)]) -> bool {
    headers
        .iter()
        .any(|(name, _)| name.to_ascii_lowercase().starts_with("x-smtp-receiver-"))
}

fn collect_llm_parts(part: &MessagePart<'_>) -> Vec<KeptPart> {
    let content_type = header_value(&part.headers, "content-type").unwrap_or("text/plain");
    let media_type = media_type(content_type);
    if media_type.starts_with("multipart/")
        && let Some(boundary) = boundary_parameter(content_type)
    {
        let mut kept = Vec::new();
        for child in split_multipart_body(part.body, &boundary) {
            kept.extend(collect_llm_parts(&parse_message_part(child)));
        }
        return kept;
    }

    if is_attachment_like(&part.headers, &media_type) {
        return vec![KeptPart {
            headers: minimal_attachment_headers(&part.headers),
            body: part.body.to_vec(),
            is_attachment: true,
        }];
    }

    if media_type == "text/plain" {
        return vec![KeptPart {
            headers: vec![(
                "Content-Type".to_string(),
                "text/plain; charset=utf-8".to_string(),
            )],
            body: decode_text_body(&part.headers, part.body),
            is_attachment: false,
        }];
    }

    Vec::new()
}

fn parse_message_part(message: &[u8]) -> MessagePart<'_> {
    let (header_bytes, body) = split_headers_body(message);
    MessagePart {
        headers: parse_headers(header_bytes),
        body,
    }
}

fn split_headers_body(message: &[u8]) -> (&[u8], &[u8]) {
    if let Some(index) = find_bytes(message, b"\r\n\r\n") {
        return (&message[..index], &message[index + 4..]);
    }
    if let Some(index) = find_bytes(message, b"\n\n") {
        return (&message[..index], &message[index + 2..]);
    }
    (message, b"")
}

fn parse_headers(header_bytes: &[u8]) -> Vec<(String, String)> {
    let text = String::from_utf8_lossy(header_bytes);
    let mut headers: Vec<(String, String)> = Vec::new();
    for line in text.lines() {
        if line.starts_with(' ') || line.starts_with('\t') {
            if let Some((_, value)) = headers.last_mut() {
                value.push(' ');
                value.push_str(line.trim());
            }
            continue;
        }
        if let Some(colon_index) = line.find(':') {
            let name = line[..colon_index].trim().to_string();
            let value = line[colon_index + 1..].trim().to_string();
            if !name.is_empty() {
                headers.push((name, value));
            }
        }
    }
    headers
}

fn filter_headers(headers: &[(String, String)]) -> Vec<(String, String)> {
    headers
        .iter()
        .filter(|(name, _)| should_keep_header(name))
        .cloned()
        .collect()
}

fn should_keep_header(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    if lower.starts_with("arc-") || lower.starts_with("dkim") {
        return false;
    }
    if lower.starts_with("x-") {
        return lower == "x-received" || lower.starts_with("x-smtp");
    }
    true
}

fn remove_header(headers: &mut Vec<(String, String)>, name: &str) {
    headers.retain(|(header_name, _)| !header_name.eq_ignore_ascii_case(name));
}

fn write_headers(output: &mut Vec<u8>, headers: &[(String, String)]) {
    for (name, value) in headers {
        if should_keep_header(name) {
            output.extend_from_slice(name.as_bytes());
            output.extend_from_slice(b": ");
            output.extend_from_slice(value.as_bytes());
            output.extend_from_slice(b"\r\n");
        }
    }
}

fn header_value<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(header_name, _)| header_name.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

fn media_type(content_type: &str) -> String {
    content_type
        .split(';')
        .next()
        .unwrap_or("text/plain")
        .trim()
        .to_ascii_lowercase()
}

fn boundary_parameter(content_type: &str) -> Option<String> {
    parameter_value(content_type, "boundary")
}

fn parameter_value(header: &str, wanted_name: &str) -> Option<String> {
    for parameter in header.split(';').skip(1) {
        let Some((name, value)) = parameter.split_once('=') else {
            continue;
        };
        if name.trim().eq_ignore_ascii_case(wanted_name) {
            return Some(trim_quotes(value.trim()).to_string());
        }
    }
    None
}

fn trim_quotes(value: &str) -> &str {
    value
        .strip_prefix('"')
        .and_then(|without_prefix| without_prefix.strip_suffix('"'))
        .unwrap_or(value)
}

fn is_attachment_like(headers: &[(String, String)], media_type: &str) -> bool {
    if header_value(headers, "content-disposition")
        .map(|value| {
            let lower = value.to_ascii_lowercase();
            lower.starts_with("attachment") || lower.contains("filename=")
        })
        .unwrap_or(false)
    {
        return true;
    }

    if header_value(headers, "content-type")
        .map(|value| value.to_ascii_lowercase().contains("name="))
        .unwrap_or(false)
    {
        return true;
    }

    !media_type.starts_with("text/") && media_type != "message/rfc822"
}

fn minimal_attachment_headers(headers: &[(String, String)]) -> Vec<(String, String)> {
    let mut kept = Vec::new();
    for wanted in [
        "Content-Type",
        "Content-Disposition",
        "Content-Transfer-Encoding",
        "Content-ID",
    ] {
        if let Some(value) = header_value(headers, wanted) {
            kept.push((wanted.to_string(), value.to_string()));
        }
    }
    kept
}

fn decode_text_body(headers: &[(String, String)], body: &[u8]) -> Vec<u8> {
    let transfer_encoding = header_value(headers, "content-transfer-encoding")
        .unwrap_or("")
        .to_ascii_lowercase();
    let decoded = match transfer_encoding.as_str() {
        "base64" => BASE64_STANDARD
            .decode(without_ascii_whitespace(body))
            .unwrap_or_else(|_| body.to_vec()),
        "quoted-printable" => quoted_printable::decode(body, quoted_printable::ParseMode::Robust)
            .unwrap_or_else(|_| body.to_vec()),
        _ => body.to_vec(),
    };
    String::from_utf8_lossy(&decoded).into_owned().into_bytes()
}

fn without_ascii_whitespace(bytes: &[u8]) -> Vec<u8> {
    bytes
        .iter()
        .copied()
        .filter(|byte| !byte.is_ascii_whitespace())
        .collect()
}

fn split_multipart_body<'a>(body: &'a [u8], boundary: &str) -> Vec<&'a [u8]> {
    let marker = format!("--{boundary}");
    let closing_marker = format!("--{boundary}--");
    let mut parts = Vec::new();
    let mut current_start: Option<usize> = None;
    let mut offset: usize = 0;

    for raw_line in body.split_inclusive(|byte| *byte == b'\n') {
        let line_without_ending = trim_line_ending(raw_line);
        if line_without_ending == marker.as_bytes()
            || line_without_ending == closing_marker.as_bytes()
        {
            if let Some(start) = current_start.take() {
                let end = offset.saturating_sub(previous_line_ending_len(body, offset));
                if end >= start {
                    parts.push(&body[start..end]);
                }
            }
            if line_without_ending == closing_marker.as_bytes() {
                break;
            }
            current_start = Some(offset + raw_line.len());
        }
        offset += raw_line.len();
    }

    parts
}

fn previous_line_ending_len(body: &[u8], offset: usize) -> usize {
    if offset >= 2 && &body[offset - 2..offset] == b"\r\n" {
        2
    } else if offset >= 1 && body[offset - 1] == b'\n' {
        1
    } else {
        0
    }
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn message_filename(content: &[u8]) -> String {
    let date = Utc::now().format("%Y-%m-%d");
    let digest = Sha1::digest(content);
    let encoded_hash = URL_SAFE_NO_PAD.encode(digest);
    format!("{date}-{encoded_hash}.eml")
}

fn trim_line_ending(line: &[u8]) -> &[u8] {
    line.strip_suffix(b"\r\n")
        .or_else(|| line.strip_suffix(b"\n"))
        .unwrap_or(line)
}

fn default_cleaned_dir_for(inbox_dir: &Path) -> String {
    let file_name = inbox_dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("inbox");
    let cleaned_name = format!("{file_name}-cleaned");
    match inbox_dir
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
    {
        Some(parent) => parent.join(cleaned_name).to_string_lossy().into_owned(),
        None => cleaned_name,
    }
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

fn connection_limit(configured: usize) -> usize {
    if configured == 0 {
        Semaphore::MAX_PERMITS
    } else {
        configured.min(Semaphore::MAX_PERMITS)
    }
}

fn parse_config_usize(values: &ConfigValues, name: &str, default: usize) -> io::Result<usize> {
    match config_optional(values, name) {
        Some(value) => value.parse::<usize>().map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{name} must be a non-negative integer: {error}"),
            )
        }),
        None => Ok(default),
    }
}

fn parse_config_u64(values: &ConfigValues, name: &str, default: u64) -> io::Result<u64> {
    match config_optional(values, name) {
        Some(value) => value.parse::<u64>().map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{name} must be a non-negative integer: {error}"),
            )
        }),
        None => Ok(default),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    #[test]
    fn config_file_values_define_listener_storage_limits_and_routing_domains() {
        let values = parse_config_text(
            r#"
            # bare keys and SMTP_* keys are both accepted
            listen_addr = 127.0.0.1:2525
            inbox_dir = /mail/dropoff
            cleaned_inbox_dir = /mail/cleaned
            temp_dir = /mail/tmp
            max_message_bytes = 1024
            global_rate_per_minute = 12
            SMTP_SENDER_RATE_PER_MINUTE = 3
            max_connections = 4
            command_timeout_seconds = 5
            recipient_domains = kautschuk.com, @undenheim.kautschuk.com, kautschuk.com
            auth_results_mode = require
            auth_results_trusted_servers = mx.google.com
            auth_results_required = dkim=pass, dmarc=pass
            auth_results_match = any
            "#,
        )
        .unwrap();

        let config = Config::from_values(values).unwrap();

        assert_eq!(config.listen_addr, "127.0.0.1:2525");
        assert_eq!(config.inbox_dir, PathBuf::from("/mail/dropoff"));
        assert_eq!(config.cleaned_inbox_dir, PathBuf::from("/mail/cleaned"));
        assert_eq!(config.temp_dir, PathBuf::from("/mail/tmp"));
        assert_eq!(config.max_message_bytes, 1024);
        assert_eq!(config.global_rate_per_minute, 12);
        assert_eq!(config.sender_rate_per_minute, 3);
        assert_eq!(config.max_connections, 4);
        assert_eq!(config.command_timeout_seconds, 5);
        assert_eq!(
            config.recipient_domains,
            vec!["kautschuk.com", "undenheim.kautschuk.com"]
        );
        assert_eq!(config.auth_results.mode, AuthResultsMode::Require);
        assert_eq!(config.auth_results.trusted_servers, vec!["mx.google.com"]);
        assert_eq!(
            config.auth_results.required_results,
            vec!["dkim=pass", "dmarc=pass"]
        );
        assert_eq!(config.auth_results.match_mode, AuthResultsMatchMode::Any);
    }

    #[test]
    fn config_file_rejects_lines_without_key_value_syntax() {
        let error = parse_config_text("listen_addr 127.0.0.1:2525").unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

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

    #[test]
    fn message_filename_uses_utc_date_and_url_safe_sha1() {
        let filename = message_filename(b"abc");
        let today = Utc::now().format("%Y-%m-%d").to_string();

        assert_eq!(filename, format!("{today}-qZk-NkcGgWq6PiVxeFDCbJzQ2J0.eml"));
    }

    #[tokio::test]
    async fn persist_message_renames_into_inbox_only_when_complete() {
        let temp = TempDir::new().unwrap();
        let config = Config {
            listen_addr: "127.0.0.1:0".to_string(),
            inbox_dir: temp.path().join("inbox"),
            cleaned_inbox_dir: temp.path().join("inbox-cleaned"),
            temp_dir: temp.path().join("tmp"),
            max_message_bytes: 1024,
            global_rate_per_minute: 10,
            sender_rate_per_minute: 10,
            max_connections: 100,
            command_timeout_seconds: 300,
            recipient_domains: Vec::new(),
            auth_results: auth_results_off(),
        };

        let outcome = persist_message(
            &config,
            "<sender@example.test>",
            &["<rcpt@example.test>".to_string()],
            b"Subject: hello\r\n\r\nbody\r\n",
        )
        .await
        .unwrap();

        let paths = stored_paths(outcome);
        assert_eq!(paths.len(), 1);
        let path = &paths[0];
        assert!(path.starts_with(&config.inbox_dir));
        assert!(path.exists());
        let filename = path.file_name().unwrap().to_string_lossy();
        let today = Utc::now().format("%Y-%m-%d").to_string();
        assert!(filename.starts_with(&format!("{today}-")));
        assert!(filename.ends_with(".eml"));
        let entries: Vec<_> = std::fs::read_dir(&config.inbox_dir).unwrap().collect();
        assert_eq!(entries.len(), 1);
        let content = std::fs::read(path).unwrap();
        assert!(content.starts_with(b"X-SMTP-Receiver-Envelope-From: <sender@example.test>\r\n"));
        assert!(content.ends_with(b"Subject: hello\r\n\r\nbody\r\n"));
        let cleaned_entries: Vec<_> = std::fs::read_dir(&config.cleaned_inbox_dir)
            .unwrap()
            .collect();
        assert_eq!(cleaned_entries.len(), 1);
    }

    #[tokio::test]
    async fn persist_message_routes_to_configured_recipient_username_folders() {
        let temp = TempDir::new().unwrap();
        let mut config = test_config(temp.path(), 4096, 10, 10);
        config.recipient_domains = vec![
            "kautschuk.com".to_string(),
            "undenheim.kautschuk.com".to_string(),
        ];

        let outcome = persist_message(
            &config,
            "<sender@example.test>",
            &["<udo@kautschuk.com>".to_string()],
            b"To: Other <other@example.test>\r\nCc: Alice <alice@undenheim.kautschuk.com>, udo+tag@kautschuk.com\r\n\r\nbody\r\n",
        )
        .await
        .unwrap();

        let paths = stored_paths(outcome);
        assert_eq!(paths.len(), 2);
        assert!(
            paths
                .iter()
                .any(|path| path.starts_with(config.inbox_dir.join("udo")))
        );
        assert!(
            paths
                .iter()
                .any(|path| path.starts_with(config.inbox_dir.join("alice")))
        );
        assert_eq!(inbox_files(&config.inbox_dir.join("udo")).len(), 1);
        assert_eq!(inbox_files(&config.inbox_dir.join("alice")).len(), 1);
        assert_eq!(inbox_files(&config.cleaned_inbox_dir.join("udo")).len(), 1);
        assert_eq!(
            inbox_files(&config.cleaned_inbox_dir.join("alice")).len(),
            1
        );
    }

    #[tokio::test]
    async fn persist_message_ignores_when_no_configured_recipient_domain_matches() {
        let temp = TempDir::new().unwrap();
        let mut config = test_config(temp.path(), 4096, 10, 10);
        config.recipient_domains = vec!["kautschuk.com".to_string()];

        let outcome = persist_message(
            &config,
            "<sender@example.test>",
            &["<outside@example.test>".to_string()],
            b"To: outside@example.test\r\n\r\nbody\r\n",
        )
        .await
        .unwrap();

        assert_eq!(outcome, DeliveryOutcome::IgnoredNoMatchingRecipient);
        assert!(inbox_files(&config.inbox_dir).is_empty());
        assert!(inbox_files(&config.cleaned_inbox_dir).is_empty());
    }

    #[tokio::test]
    async fn persist_message_ignores_when_auth_results_policy_fails() {
        let temp = TempDir::new().unwrap();
        let mut config = test_config(temp.path(), 4096, 10, 10);
        config.auth_results = AuthResultsConfig {
            mode: AuthResultsMode::Require,
            trusted_servers: vec!["mx.google.com".to_string()],
            required_results: vec!["dmarc=pass".to_string()],
            match_mode: AuthResultsMatchMode::Any,
        };

        let outcome = persist_message(
            &config,
            "<sender@example.test>",
            &["<rcpt@example.test>".to_string()],
            b"Authentication-Results: mx.google.com; dkim=pass header.i=@example.test\r\nSubject: no dmarc\r\n\r\nbody\r\n",
        )
        .await
        .unwrap();

        assert_eq!(outcome, DeliveryOutcome::IgnoredAuthResults);
        assert!(inbox_files(&config.inbox_dir).is_empty());
    }

    #[tokio::test]
    async fn persist_message_stores_when_auth_results_policy_passes() {
        let temp = TempDir::new().unwrap();
        let mut config = test_config(temp.path(), 4096, 10, 10);
        config.auth_results = AuthResultsConfig {
            mode: AuthResultsMode::Require,
            trusted_servers: vec!["mx.google.com".to_string()],
            required_results: vec!["dkim=pass".to_string(), "dmarc=pass".to_string()],
            match_mode: AuthResultsMatchMode::Any,
        };

        let outcome = persist_message(
            &config,
            "<sender@example.test>",
            &["<rcpt@example.test>".to_string()],
            b"Authentication-Results: mx.google.com; spf=pass; dkim=pass header.i=@example.test\r\nSubject: pass\r\n\r\nbody\r\n",
        )
        .await
        .unwrap();

        assert_eq!(stored_paths(outcome).len(), 1);
    }

    #[test]
    fn recipient_folder_detection_uses_to_cc_bcc_and_exact_domains() {
        let domains = vec![
            "kautschuk.com".to_string(),
            "undenheim.kautschuk.com".to_string(),
        ];
        let folders = recipient_folders_for_message(
            &domains,
            &["<bcc@kautschuk.com>".to_string()],
            b"To: Udo <UDO@kautschuk.com>, bad@evil-kautschuk.com\r\nCc: team@undenheim.kautschuk.com\r\nBcc: hidden@kautschuk.com\r\n\r\nbody\r\n",
        );

        assert_eq!(folders, vec!["bcc", "hidden", "team", "udo"]);
    }

    #[test]
    fn recipient_folder_detection_strips_plus_and_hash_detail_suffixes() {
        let domains = vec!["undenheim.kautschuk.com".to_string()];
        let folders = recipient_folders_for_message(
            &domains,
            &[
                "<orders#protocol@undenheim.kautschuk.com>".to_string(),
                "<udo+scanner@undenheim.kautschuk.com>".to_string(),
            ],
            b"To: orders#other@undenheim.kautschuk.com\r\n\r\nbody\r\n",
        );

        assert_eq!(folders, vec!["orders", "udo"]);
    }

    #[test]
    fn clean_message_strips_tracking_auth_headers_html_and_keeps_plain_text() {
        let message = b"X-SMTP-Receiver-Envelope-From: <sender@example.test>\r\n\r\nX-Received: ok\r\nX-Spam-Score: 100\r\nARC-Seal: secret\r\nDKIM-Signature: signature\r\nFrom: Sender <sender@example.test>\r\nSubject: Clean me\r\nMIME-Version: 1.0\r\nContent-Type: multipart/alternative; boundary=alt\r\n\r\n--alt\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Transfer-Encoding: quoted-printable\r\n\r\nHello=20plain=20text.\r\n--alt\r\nContent-Type: text/html; charset=utf-8\r\n\r\n<html><body>expensive html</body></html>\r\n--alt--\r\n";

        let cleaned = String::from_utf8(clean_message_for_llm(message)).unwrap();

        assert!(cleaned.contains("X-SMTP-Receiver-Envelope-From: <sender@example.test>"));
        assert!(cleaned.contains("X-Received: ok"));
        assert!(cleaned.contains("From: Sender <sender@example.test>"));
        assert!(cleaned.contains("Subject: Clean me"));
        assert!(cleaned.contains("Hello plain text."));
        assert!(!cleaned.contains("X-Spam-Score"));
        assert!(!cleaned.contains("ARC-Seal"));
        assert!(!cleaned.contains("DKIM-Signature"));
        assert!(!cleaned.contains("expensive html"));
        assert!(!cleaned.contains("multipart/alternative"));
    }

    #[test]
    fn clean_message_keeps_attachments_with_minimal_headers() {
        let message = b"Subject: Attachment\r\nMIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary=mix\r\n\r\n--mix\r\nContent-Type: text/plain\r\n\r\nSee attached.\r\n--mix\r\nContent-Type: application/pdf; name=doc.pdf\r\nContent-Disposition: attachment; filename=doc.pdf\r\nContent-Transfer-Encoding: base64\r\nX-Attachment-Tracker: remove-me\r\n\r\nJVBERi0xLjQK\r\n--mix\r\nContent-Type: application/octet-stream\r\nContent-Transfer-Encoding: base64\r\n\r\nQUJDRA==\r\n--mix--\r\n";

        let cleaned = String::from_utf8(clean_message_for_llm(message)).unwrap();

        assert!(cleaned.contains("Content-Type: multipart/mixed"));
        assert!(cleaned.contains("See attached."));
        assert!(cleaned.contains("Content-Type: application/pdf; name=doc.pdf"));
        assert!(cleaned.contains("Content-Disposition: attachment; filename=doc.pdf"));
        assert!(cleaned.contains("Content-Transfer-Encoding: base64"));
        assert!(cleaned.contains("JVBERi0xLjQK"));
        assert!(cleaned.contains("Content-Type: application/octet-stream"));
        assert!(cleaned.contains("QUJDRA=="));
        assert!(!cleaned.contains("X-Attachment-Tracker"));
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
        let cleaned_files = inbox_files(&config.cleaned_inbox_dir);
        assert_eq!(cleaned_files.len(), 1);
        let content = std::fs::read(&files[0]).unwrap();
        assert!(content.starts_with(b"X-SMTP-Receiver-Envelope-From: <sender@example.test>\r\n"));
        assert!(
            content
                .windows(b"Subject: integration".len())
                .any(|window| window == b"Subject: integration")
        );
        let cleaned_content = std::fs::read_to_string(&cleaned_files[0]).unwrap();
        assert!(cleaned_content.contains("Subject: integration"));
        assert!(cleaned_content.contains("hello"));
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
        assert!(inbox_files(&config.cleaned_inbox_dir).is_empty());

        write_smtp(&mut client, ".\r\n").await;
        assert_eq!(
            read_smtp_response(&mut client).await,
            "250 message accepted\r\n"
        );
        assert_eq!(inbox_files(&config.inbox_dir).len(), 1);
        assert_eq!(inbox_files(&config.cleaned_inbox_dir).len(), 1);
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
            cleaned_inbox_dir: root.join("inbox-cleaned"),
            temp_dir: root.join("tmp"),
            max_message_bytes,
            global_rate_per_minute: global,
            sender_rate_per_minute: sender,
            max_connections: 100,
            command_timeout_seconds: 300,
            recipient_domains: Vec::new(),
            auth_results: auth_results_off(),
        }
    }

    fn auth_results_off() -> AuthResultsConfig {
        AuthResultsConfig {
            mode: AuthResultsMode::Off,
            trusted_servers: Vec::new(),
            required_results: Vec::new(),
            match_mode: AuthResultsMatchMode::Any,
        }
    }

    fn stored_paths(outcome: DeliveryOutcome) -> Vec<PathBuf> {
        match outcome {
            DeliveryOutcome::Stored(paths) => paths,
            other => panic!("expected stored delivery, got {other:?}"),
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
