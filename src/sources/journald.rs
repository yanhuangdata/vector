use std::{
    collections::{HashMap, HashSet},
    io::SeekFrom,
    iter::FromIterator,
    path::{Path, PathBuf},
    process::Stdio,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
use chrono::TimeZone;
use codecs::{decoding::BoxedFramingError, CharacterDelimitedDecoder};
use futures::{future, stream::BoxStream, StreamExt};
use nix::{
    sys::signal::{kill, Signal},
    unistd::Pid,
};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::{Error as JsonError, Value as JsonValue};
use snafu::{ResultExt, Snafu};
use tokio::{
    fs::{File, OpenOptions},
    io::{self, AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    process::Command,
    sync::{Mutex, MutexGuard},
    time::sleep,
};
use tokio_util::codec::FramedRead;
use vector_core::ByteSizeOf;

use crate::{
    config::{
        log_schema, AcknowledgementsConfig, DataType, Output, SourceConfig, SourceContext,
        SourceDescription,
    },
    event::{BatchNotifier, BatchStatus, BatchStatusReceiver, LogEvent, Value},
    internal_events::{BytesReceived, JournaldInvalidRecordError, OldEventsReceived},
    serde::bool_or_struct,
    shutdown::ShutdownSignal,
    sources::util::finalizer::OrderedFinalizer,
    SourceSender,
};

const DEFAULT_BATCH_SIZE: usize = 16;
const BATCH_TIMEOUT: Duration = Duration::from_millis(10);

const CHECKPOINT_FILENAME: &str = "checkpoint.txt";
const CURSOR: &str = "__CURSOR";
const HOSTNAME: &str = "_HOSTNAME";
const MESSAGE: &str = "MESSAGE";
const SYSTEMD_UNIT: &str = "_SYSTEMD_UNIT";
const SOURCE_TIMESTAMP: &str = "_SOURCE_REALTIME_TIMESTAMP";
const RECEIVED_TIMESTAMP: &str = "__REALTIME_TIMESTAMP";

const BACKOFF_DURATION: Duration = Duration::from_secs(1);

static JOURNALCTL: Lazy<PathBuf> = Lazy::new(|| "journalctl".into());

#[derive(Debug, Snafu)]
enum BuildError {
    #[snafu(display("journalctl failed to execute: {}", source))]
    JournalctlSpawn { source: io::Error },
    #[snafu(display("Cannot use both `units` and `include_units`"))]
    BothUnitsAndIncludeUnits,
    #[snafu(display(
        "The unit {:?} is duplicated in both include_units and exclude_units",
        unit
    ))]
    DuplicatedUnit { unit: String },
    #[snafu(display(
        "The Journal field/value pair {:?}:{:?} is duplicated in both include_matches and exclude_matches.",
        field,
        value,
    ))]
    DuplicatedMatches { field: String, value: String },
}

#[derive(Deserialize, Serialize, Debug, Default)]
#[serde(deny_unknown_fields, default)]
pub struct JournaldConfig {
    pub since_now: Option<bool>,
    pub current_boot_only: Option<bool>,
    pub units: Vec<String>,
    pub include_units: Vec<String>,
    pub exclude_units: Vec<String>,
    pub include_matches: HashMap<String, HashSet<String>>,
    pub exclude_matches: HashMap<String, HashSet<String>>,
    pub data_dir: Option<PathBuf>,
    pub batch_size: Option<usize>,
    pub journalctl_path: Option<PathBuf>,
    pub journal_directory: Option<PathBuf>,
    #[serde(default, deserialize_with = "bool_or_struct")]
    acknowledgements: AcknowledgementsConfig,
    /// Deprecated
    #[serde(default)]
    remap_priority: bool,
}

impl JournaldConfig {
    fn merged_include_matches(&self) -> crate::Result<Matches> {
        let include_units = match (!self.units.is_empty(), !self.include_units.is_empty()) {
            (true, true) => return Err(BuildError::BothUnitsAndIncludeUnits.into()),
            (true, false) => {
                warn!("The `units` setting is deprecated, use `include_units` instead.");
                &self.units
            }
            (false, _) => &self.include_units,
        };

        Ok(Self::merge_units(&self.include_matches, include_units))
    }

    fn merged_exclude_matches(&self) -> Matches {
        Self::merge_units(&self.exclude_matches, &self.exclude_units)
    }

    fn merge_units(matches: &Matches, units: &[String]) -> Matches {
        let mut matches = matches.clone();
        for unit in units {
            let entry = matches.entry(String::from(SYSTEMD_UNIT));
            entry.or_default().insert(fixup_unit(unit));
        }
        matches
    }
}

inventory::submit! {
    SourceDescription::new::<JournaldConfig>("journald")
}

impl_generate_config_from_default!(JournaldConfig);

type Record = HashMap<String, String>;
type Matches = HashMap<String, HashSet<String>>;

#[async_trait::async_trait]
#[typetag::serde(name = "journald")]
impl SourceConfig for JournaldConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<super::Source> {
        if self.remap_priority {
            warn!("Option `remap_priority` has been deprecated. Please use the `remap` transform and function `to_syslog_level` instead.");
        }

        let data_dir = cx
            .globals
            // source are only global, name can be used for subdir
            .resolve_and_make_data_subdir(self.data_dir.as_ref(), cx.key.id())?;

        if let Some(unit) = self
            .include_units
            .iter()
            .find(|unit| self.exclude_units.contains(unit))
        {
            let unit = unit.into();
            return Err(BuildError::DuplicatedUnit { unit }.into());
        }

        let include_matches = self.merged_include_matches()?;
        let exclude_matches = self.merged_exclude_matches();

        if let Some((field, value)) = find_duplicate_match(&include_matches, &exclude_matches) {
            return Err(BuildError::DuplicatedMatches { field, value }.into());
        }

        let mut checkpoint_path = data_dir;
        checkpoint_path.push(CHECKPOINT_FILENAME);

        let journalctl_path = self
            .journalctl_path
            .clone()
            .unwrap_or_else(|| JOURNALCTL.clone());

        let batch_size = self.batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
        let current_boot_only = self.current_boot_only.unwrap_or(true);
        let since_now = self.since_now.unwrap_or(false);
        let journal_dir = self.journal_directory.clone();
        let acknowledgements = cx.do_acknowledgements(&self.acknowledgements);

        let start: StartJournalctlFn = Box::new(move |cursor| {
            let mut command = create_command(
                &journalctl_path,
                journal_dir.as_ref(),
                current_boot_only,
                since_now,
                cursor,
            );
            start_journalctl(&mut command)
        });

        Ok(Box::pin(
            JournaldSource {
                include_matches,
                exclude_matches,
                checkpoint_path,
                batch_size,
                remap_priority: self.remap_priority,
                out: cx.out,
                acknowledgements,
            }
            .run_shutdown(cx.shutdown, start),
        ))
    }

    fn outputs(&self) -> Vec<Output> {
        vec![Output::default(DataType::Log)]
    }

    fn source_type(&self) -> &'static str {
        "journald"
    }

    fn can_acknowledge(&self) -> bool {
        true
    }
}

struct JournaldSource {
    include_matches: Matches,
    exclude_matches: Matches,
    checkpoint_path: PathBuf,
    batch_size: usize,
    remap_priority: bool,
    out: SourceSender,
    acknowledgements: bool,
}

impl JournaldSource {
    async fn run_shutdown(
        self,
        shutdown: ShutdownSignal,
        start_journalctl: StartJournalctlFn,
    ) -> Result<(), ()> {
        let checkpointer = StatefulCheckpointer::new(self.checkpoint_path.clone())
            .await
            .map_err(|error| {
                error!(
                    message = "Unable to open checkpoint file.",
                    path = ?self.checkpoint_path,
                    %error,
                );
            })?;

        let checkpointer = SharedCheckpointer::new(checkpointer);
        let finalizer = Finalizer::new(
            self.acknowledgements,
            checkpointer.clone(),
            shutdown.clone(),
        );

        let run = Box::pin(self.run(checkpointer, finalizer, start_journalctl));
        future::select(run, shutdown).await;

        Ok(())
    }

    async fn run(
        mut self,
        checkpointer: SharedCheckpointer,
        finalizer: Finalizer,
        start_journalctl: StartJournalctlFn,
    ) {
        loop {
            info!("Starting journalctl.");
            match start_journalctl(&checkpointer.lock().await.cursor) {
                Ok(RunningJournal(stream, stop)) => {
                    let should_restart = self.run_stream(stream, &finalizer).await;
                    stop();
                    if !should_restart {
                        return;
                    }
                }
                Err(error) => {
                    error!(message = "Error starting journalctl process.", %error);
                }
            };

            // journalctl process should never stop,
            // so it is an error if we reach here.
            sleep(BACKOFF_DURATION).await;
        }
    }

    /// Process `journalctl` output until some error occurs.
    /// Return `true` if should restart `journalctl`.
    async fn run_stream<'a>(
        &'a mut self,
        mut stream: BoxStream<'static, Result<Bytes, BoxedFramingError>>,
        finalizer: &'a Finalizer,
    ) -> bool {
        let batch_size = self.batch_size;
        loop {
            let mut batch = Batch::new(self);

            if !batch.handle_next(stream.next().await) {
                break true;
            }

            let timeout = tokio::time::sleep(BATCH_TIMEOUT);
            tokio::pin!(timeout);

            for _ in 1..batch_size {
                tokio::select! {
                    _ = &mut timeout => break,
                    result = stream.next() => if !batch.handle_next(result) {
                        break;
                    }
                }
            }
            if let Some(x) = batch.finish(finalizer).await {
                break x;
            }
        }
    }
}

struct Batch<'a> {
    events: Vec<LogEvent>,
    record_size: usize,
    exiting: Option<bool>,
    batch: Option<Arc<BatchNotifier>>,
    receiver: Option<BatchStatusReceiver>,
    source: &'a mut JournaldSource,
    cursor: Option<String>,
}

impl<'a> Batch<'a> {
    fn new(source: &'a mut JournaldSource) -> Self {
        let (batch, receiver) = BatchNotifier::maybe_new_with_receiver(source.acknowledgements);
        Self {
            events: Vec::new(),
            record_size: 0,
            exiting: None,
            batch,
            receiver,
            source,
            cursor: None,
        }
    }

    fn handle_next(&mut self, result: Option<Result<Bytes, BoxedFramingError>>) -> bool {
        match result {
            None => {
                warn!("Journalctl process stopped.");
                self.exiting = Some(true);
                false
            }
            Some(Err(error)) => {
                error!(
                    message = "Could not read from journald source.",
                    %error,
                );
                false
            }
            Some(Ok(bytes)) => {
                match decode_record(&bytes, self.source.remap_priority) {
                    Ok(mut record) => {
                        if let Some(tmp) = record.remove(&*CURSOR) {
                            self.cursor = Some(tmp);
                        }

                        if !filter_matches(
                            &record,
                            &self.source.include_matches,
                            &self.source.exclude_matches,
                        ) {
                            self.record_size += bytes.len();
                            let event = create_event(record, &self.batch);
                            self.events.push(event);
                        }
                    }
                    Err(error) => {
                        emit!(JournaldInvalidRecordError {
                            error,
                            text: String::from_utf8_lossy(&bytes).into_owned()
                        });
                    }
                }
                true
            }
        }
    }

    async fn finish(mut self, finalizer: &Finalizer) -> Option<bool> {
        drop(self.batch);

        if self.record_size > 0 {
            emit!(BytesReceived {
                byte_size: self.record_size,
                protocol: "journald",
            });
        }

        if !self.events.is_empty() {
            emit!(OldEventsReceived {
                count: self.events.len(),
                byte_size: self.events.size_of(),
            });

            match self.source.out.send_batch(self.events).await {
                Ok(_) => {
                    if let Some(cursor) = self.cursor {
                        finalizer.finalize(cursor, self.receiver).await;
                    }
                }
                Err(error) => {
                    error!(message = "Could not send journald log.", %error);
                    // `out` channel is closed, don't restart journalctl.
                    self.exiting = Some(false);
                }
            }
        }
        self.exiting
    }
}

/// A function that starts journalctl process.
/// Return a stream of output split by '\n', and a `StopJournalctlFn`.
///
/// Code uses `start_journalctl` below,
/// but we need this type to implement fake journald source in testing.
type StartJournalctlFn =
    Box<dyn Fn(&Option<String>) -> crate::Result<RunningJournal> + Send + Sync>;

type StopJournalctlFn = Box<dyn FnOnce() + Send>;

struct RunningJournal(
    BoxStream<'static, Result<Bytes, BoxedFramingError>>,
    StopJournalctlFn,
);

fn start_journalctl(command: &mut Command) -> crate::Result<RunningJournal> {
    let mut child = command.spawn().context(JournalctlSpawnSnafu)?;

    let stream = FramedRead::new(
        child.stdout.take().unwrap(),
        CharacterDelimitedDecoder::new(b'\n'),
    )
    .boxed();

    let pid = Pid::from_raw(child.id().unwrap() as _);
    let stop = Box::new(move || {
        let _ = kill(pid, Signal::SIGTERM);
    });

    Ok(RunningJournal(stream, stop))
}

fn create_command(
    path: &Path,
    journal_dir: Option<&PathBuf>,
    current_boot_only: bool,
    since_now: bool,
    cursor: &Option<String>,
) -> Command {
    let mut command = Command::new(path);
    command.stdout(Stdio::piped());
    command.arg("--follow");
    command.arg("--all");
    command.arg("--show-cursor");
    command.arg("--output=json");

    if let Some(dir) = journal_dir {
        command.arg(format!("--directory={}", dir.display()));
    }

    if current_boot_only {
        command.arg("--boot");
    }

    if let Some(cursor) = cursor {
        command.arg(format!("--after-cursor={}", cursor));
    } else if since_now {
        command.arg("--since=now");
    } else {
        // journalctl --follow only outputs a few lines without a starting point
        command.arg("--since=2000-01-01");
    }

    command
}

fn create_event(record: Record, batch: &Option<Arc<BatchNotifier>>) -> LogEvent {
    let mut log = LogEvent::from_iter(record).with_batch_notifier_option(batch);

    // Convert some journald-specific field names into Vector standard ones.
    if let Some(message) = log.remove(MESSAGE) {
        log.insert(log_schema().message_key(), message);
    }
    if let Some(host) = log.remove(HOSTNAME) {
        log.insert(log_schema().host_key(), host);
    }
    // Translate the timestamp, and so leave both old and new names.
    if let Some(Value::Bytes(timestamp)) = log
        .get(&*SOURCE_TIMESTAMP)
        .or_else(|| log.get(RECEIVED_TIMESTAMP))
    {
        if let Ok(timestamp) = String::from_utf8_lossy(timestamp).parse::<u64>() {
            let timestamp = chrono::Utc.timestamp(
                (timestamp / 1_000_000) as i64,
                (timestamp % 1_000_000) as u32 * 1_000,
            );
            log.insert(log_schema().timestamp_key(), Value::Timestamp(timestamp));
        }
    }
    // Add source type
    log.try_insert(log_schema().source_type_key(), Bytes::from("journald"));

    log
}

/// Map the given unit name into a valid systemd unit
/// by appending ".service" if no extension is present.
fn fixup_unit(unit: &str) -> String {
    if unit.contains('.') {
        unit.into()
    } else {
        format!("{}.service", unit)
    }
}

fn decode_record(line: &[u8], remap: bool) -> Result<Record, JsonError> {
    let mut record = serde_json::from_str::<JsonValue>(&String::from_utf8_lossy(line))?;
    // journalctl will output non-ASCII values using an array
    // of integers. Look for those values and re-parse them.
    if let Some(record) = record.as_object_mut() {
        for (_, value) in record.iter_mut().filter(|(_, v)| v.is_array()) {
            *value = decode_array(value.as_array().expect("already validated"));
        }
    }
    if remap {
        record.get_mut("PRIORITY").map(remap_priority);
    }
    serde_json::from_value(record)
}

fn decode_array(array: &[JsonValue]) -> JsonValue {
    decode_array_as_bytes(array).unwrap_or_else(|| {
        let ser = serde_json::to_string(array).expect("already deserialized");
        JsonValue::String(ser)
    })
}

fn decode_array_as_bytes(array: &[JsonValue]) -> Option<JsonValue> {
    // From the array of values, turn all the numbers into bytes, and
    // then the bytes into a string, but return None if any value in the
    // array was not a valid byte.
    array
        .iter()
        .map(|item| {
            item.as_u64().and_then(|num| match num {
                num if num <= u8::max_value() as u64 => Some(num as u8),
                _ => None,
            })
        })
        .collect::<Option<Vec<u8>>>()
        .map(|array| String::from_utf8_lossy(&array).into())
}

fn remap_priority(priority: &mut JsonValue) {
    if let Some(num) = priority.as_str().and_then(|s| usize::from_str(s).ok()) {
        let text = match num {
            0 => "EMERG",
            1 => "ALERT",
            2 => "CRIT",
            3 => "ERR",
            4 => "WARNING",
            5 => "NOTICE",
            6 => "INFO",
            7 => "DEBUG",
            _ => "UNKNOWN",
        };
        *priority = JsonValue::String(text.into());
    }
}

fn filter_matches(record: &Record, includes: &Matches, excludes: &Matches) -> bool {
    match (includes.is_empty(), excludes.is_empty()) {
        (true, true) => false,
        (false, true) => !contains_match(record, includes),
        (true, false) => contains_match(record, excludes),
        (false, false) => !contains_match(record, includes) || contains_match(record, excludes),
    }
}

fn contains_match(record: &Record, matches: &Matches) -> bool {
    let f = move |(field, value)| {
        matches
            .get(field)
            .map(|x| x.contains(value))
            .unwrap_or(false)
    };
    record.iter().any(f)
}

fn find_duplicate_match(a_matches: &Matches, b_matches: &Matches) -> Option<(String, String)> {
    for (a_key, a_values) in a_matches {
        if let Some(b_values) = b_matches.get(a_key.as_str()) {
            for (a, b) in a_values
                .iter()
                .flat_map(|x| std::iter::repeat(x).zip(b_values.iter()))
            {
                if a == b {
                    return Some((a_key.into(), b.into()));
                }
            }
        }
    }
    None
}

enum Finalizer {
    Sync(SharedCheckpointer),
    Async(OrderedFinalizer<String>),
}

impl Finalizer {
    fn new(
        acknowledgements: bool,
        checkpointer: SharedCheckpointer,
        shutdown: ShutdownSignal,
    ) -> Self {
        if acknowledgements {
            let (finalizer, mut ack_stream) = OrderedFinalizer::new(shutdown);
            tokio::spawn(async move {
                while let Some((status, cursor)) = ack_stream.next().await {
                    if status == BatchStatus::Delivered {
                        checkpointer.lock().await.set(cursor).await;
                    }
                }
            });
            Self::Async(finalizer)
        } else {
            Self::Sync(checkpointer)
        }
    }

    async fn finalize(&self, cursor: String, receiver: Option<BatchStatusReceiver>) {
        match (self, receiver) {
            (Self::Sync(checkpointer), None) => checkpointer.lock().await.set(cursor).await,
            (Self::Async(finalizer), Some(receiver)) => finalizer.add(cursor, receiver),
            _ => {
                unreachable!("Cannot have async finalization without a receiver in journald source")
            }
        }
    }
}

struct Checkpointer {
    file: File,
    filename: PathBuf,
}

impl Checkpointer {
    async fn new(filename: PathBuf) -> Result<Self, io::Error> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&filename)
            .await?;
        Ok(Checkpointer { file, filename })
    }

    async fn set(&mut self, token: &str) -> Result<(), io::Error> {
        self.file.seek(SeekFrom::Start(0)).await?;
        self.file
            .write_all(format!("{}\n", token).as_bytes())
            .await?;
        Ok(())
    }

    async fn get(&mut self) -> Result<Option<String>, io::Error> {
        let mut buf = Vec::<u8>::new();
        self.file.seek(SeekFrom::Start(0)).await?;
        self.file.read_to_end(&mut buf).await?;
        match buf.len() {
            0 => Ok(None),
            _ => {
                let text = String::from_utf8_lossy(&buf);
                match text.find('\n') {
                    Some(nl) => Ok(Some(String::from(&text[..nl]))),
                    None => Ok(None), // Maybe return an error?
                }
            }
        }
    }
}

struct StatefulCheckpointer {
    checkpointer: Checkpointer,
    cursor: Option<String>,
}

impl StatefulCheckpointer {
    async fn new(filename: PathBuf) -> Result<Self, io::Error> {
        let mut checkpointer = Checkpointer::new(filename).await?;
        let cursor = checkpointer.get().await?;
        Ok(Self {
            checkpointer,
            cursor,
        })
    }

    async fn set(&mut self, token: impl Into<String>) {
        let token = token.into();
        if let Err(error) = self.checkpointer.set(&token).await {
            error!(
                message = "Could not set journald checkpoint.",
                %error,
                filename = ?self.checkpointer.filename,
            );
        }
        self.cursor = Some(token);
    }
}

#[derive(Clone)]
struct SharedCheckpointer(Arc<Mutex<StatefulCheckpointer>>);

impl SharedCheckpointer {
    fn new(c: StatefulCheckpointer) -> Self {
        Self(Arc::new(Mutex::new(c)))
    }

    async fn lock(&self) -> MutexGuard<'_, StatefulCheckpointer> {
        self.0.lock().await
    }
}

#[cfg(test)]
mod checkpointer_tests {
    use tempfile::tempdir;
    use tokio::fs::read_to_string;

    use super::*;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<JournaldConfig>();
    }

    #[tokio::test]
    async fn journald_checkpointer_works() {
        let tempdir = tempdir().unwrap();
        let mut filename = tempdir.path().to_path_buf();
        filename.push(CHECKPOINT_FILENAME);
        let mut checkpointer = Checkpointer::new(filename.clone())
            .await
            .expect("Creating checkpointer failed!");

        assert!(checkpointer.get().await.unwrap().is_none());

        checkpointer
            .set("first test")
            .await
            .expect("Setting checkpoint failed");
        assert_eq!(checkpointer.get().await.unwrap().unwrap(), "first test");
        let contents = read_to_string(filename.clone())
            .await
            .unwrap_or_else(|_| panic!("Failed to read: {:?}", filename));
        assert!(contents.starts_with("first test\n"));

        checkpointer
            .set("second")
            .await
            .expect("Setting checkpoint failed");
        assert_eq!(checkpointer.get().await.unwrap().unwrap(), "second");
        let contents = read_to_string(filename.clone())
            .await
            .unwrap_or_else(|_| panic!("Failed to read: {:?}", filename));
        assert!(contents.starts_with("second\n"));
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{BufRead, BufReader, Cursor},
        pin::Pin,
        task::{Context, Poll},
    };

    use futures::Stream;
    use tempfile::tempdir;
    use tokio::time::{sleep, timeout, Duration};

    use super::*;
    use crate::{
        event::Event, event::EventStatus, test_util::components::assert_source_compliance,
    };

    const FAKE_JOURNAL: &str = r#"{"_SYSTEMD_UNIT":"sysinit.target","MESSAGE":"System Initialization","__CURSOR":"1","_SOURCE_REALTIME_TIMESTAMP":"1578529839140001","PRIORITY":"6"}
{"_SYSTEMD_UNIT":"unit.service","MESSAGE":"unit message","__CURSOR":"2","_SOURCE_REALTIME_TIMESTAMP":"1578529839140002","PRIORITY":"7"}
{"_SYSTEMD_UNIT":"badunit.service","MESSAGE":[194,191,72,101,108,108,111,63],"__CURSOR":"2","_SOURCE_REALTIME_TIMESTAMP":"1578529839140003","PRIORITY":"5"}
{"_SYSTEMD_UNIT":"stdout","MESSAGE":"Missing timestamp","__CURSOR":"3","__REALTIME_TIMESTAMP":"1578529839140004","PRIORITY":"2"}
{"_SYSTEMD_UNIT":"stdout","MESSAGE":"Different timestamps","__CURSOR":"4","_SOURCE_REALTIME_TIMESTAMP":"1578529839140005","__REALTIME_TIMESTAMP":"1578529839140004","PRIORITY":"3"}
{"_SYSTEMD_UNIT":"syslog.service","MESSAGE":"Non-ASCII in other field","__CURSOR":"5","_SOURCE_REALTIME_TIMESTAMP":"1578529839140005","__REALTIME_TIMESTAMP":"1578529839140004","PRIORITY":"3","SYSLOG_RAW":[194,191,87,111,114,108,100,63]}
{"_SYSTEMD_UNIT":"NetworkManager.service","MESSAGE":"<info>  [1608278027.6016] dhcp-init: Using DHCP client 'dhclient'","__CURSOR":"6","_SOURCE_REALTIME_TIMESTAMP":"1578529839140005","__REALTIME_TIMESTAMP":"1578529839140004","PRIORITY":"6","SYSLOG_FACILITY":["DHCP4","DHCP6"]}
{"PRIORITY":"5","SYSLOG_FACILITY":"0","SYSLOG_IDENTIFIER":"kernel","_TRANSPORT":"kernel","__REALTIME_TIMESTAMP":"1578529839140006","MESSAGE":"audit log"}
"#;

    struct FakeJournal {
        reader: BufReader<Cursor<&'static str>>,
    }

    impl FakeJournal {
        fn next(&mut self) -> Option<Result<Bytes, BoxedFramingError>> {
            let mut line = String::new();
            match self.reader.read_line(&mut line) {
                Ok(0) => None,
                Ok(_) => {
                    line.pop();
                    Some(Ok(Bytes::from(line)))
                }
                Err(err) => Some(Err(err.into())),
            }
        }
    }

    impl Stream for FakeJournal {
        type Item = Result<Bytes, BoxedFramingError>;

        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Option<Self::Item>> {
            Poll::Ready(Pin::into_inner(self).next())
        }
    }

    impl FakeJournal {
        fn new(checkpoint: &Option<String>) -> RunningJournal {
            let cursor = Cursor::new(FAKE_JOURNAL);
            let reader = BufReader::new(cursor);
            let mut journal = FakeJournal { reader };

            // The cursors are simply line numbers
            if let Some(cursor) = checkpoint {
                let cursor = cursor.parse::<usize>().expect("Invalid cursor");
                for _ in 0..cursor {
                    journal.next();
                }
            }

            RunningJournal(Box::pin(journal), Box::new(|| ()))
        }
    }

    async fn run_with_units(iunits: &[&str], xunits: &[&str], cursor: Option<&str>) -> Vec<Event> {
        let include_matches = create_unit_matches(iunits.to_vec());
        let exclude_matches = create_unit_matches(xunits.to_vec());
        run_journal(include_matches, exclude_matches, cursor).await
    }

    async fn run_journal(
        include_matches: Matches,
        exclude_matches: Matches,
        cursor: Option<&str>,
    ) -> Vec<Event> {
        assert_source_compliance(&["protocol"], async move {
            let (tx, rx) = SourceSender::new_test_finalize(EventStatus::Delivered);
            let (trigger, shutdown, _) = ShutdownSignal::new_wired();

            let tempdir = tempdir().unwrap();
            let mut checkpoint_path = tempdir.path().to_path_buf();
            checkpoint_path.push(CHECKPOINT_FILENAME);

            let mut checkpointer = Checkpointer::new(checkpoint_path.clone())
                .await
                .expect("Creating checkpointer failed!");

            if let Some(cursor) = cursor {
                checkpointer
                    .set(cursor)
                    .await
                    .expect("Could not set checkpoint");
            }

            let source = JournaldSource {
                include_matches,
                exclude_matches,
                checkpoint_path,
                batch_size: DEFAULT_BATCH_SIZE,
                remap_priority: true,
                out: tx,
                acknowledgements: false,
            }
            .run_shutdown(
                shutdown,
                Box::new(|checkpoint| Ok(FakeJournal::new(checkpoint))),
            );
            tokio::spawn(source);

            sleep(Duration::from_millis(100)).await;
            drop(trigger);

            timeout(Duration::from_secs(1), rx.collect()).await.unwrap()
        })
        .await
    }

    fn create_unit_matches<S: Into<String>>(units: Vec<S>) -> Matches {
        let units: HashSet<String> = units.into_iter().map(Into::into).collect();
        let mut map = HashMap::new();
        if !units.is_empty() {
            map.insert(String::from(SYSTEMD_UNIT), units);
        }
        map
    }

    fn create_matches<S: Into<String>>(conditions: Vec<(S, S)>) -> Matches {
        let mut matches: Matches = HashMap::new();
        for (field, value) in conditions {
            matches
                .entry(field.into())
                .or_default()
                .insert(value.into());
        }
        matches
    }

    #[tokio::test]
    async fn reads_journal() {
        let received = run_with_units(&[], &[], None).await;
        assert_eq!(received.len(), 8);
        assert_eq!(
            message(&received[0]),
            Value::Bytes("System Initialization".into())
        );
        assert_eq!(
            received[0].as_log()[log_schema().source_type_key()],
            "journald".into()
        );
        assert_eq!(timestamp(&received[0]), value_ts(1578529839, 140001000));
        assert_eq!(priority(&received[0]), Value::Bytes("INFO".into()));
        assert_eq!(message(&received[1]), Value::Bytes("unit message".into()));
        assert_eq!(timestamp(&received[1]), value_ts(1578529839, 140002000));
        assert_eq!(priority(&received[1]), Value::Bytes("DEBUG".into()));
    }

    #[tokio::test]
    async fn includes_units() {
        let received = run_with_units(&["unit.service"], &[], None).await;
        assert_eq!(received.len(), 1);
        assert_eq!(message(&received[0]), Value::Bytes("unit message".into()));
    }

    #[tokio::test]
    async fn excludes_units() {
        let received = run_with_units(&[], &["unit.service", "badunit.service"], None).await;
        assert_eq!(received.len(), 6);
        assert_eq!(
            message(&received[0]),
            Value::Bytes("System Initialization".into())
        );
        assert_eq!(
            message(&received[1]),
            Value::Bytes("Missing timestamp".into())
        );
        assert_eq!(
            message(&received[2]),
            Value::Bytes("Different timestamps".into())
        );
    }

    #[tokio::test]
    async fn includes_matches() {
        let matches = create_matches(vec![("PRIORITY", "ERR")]);
        let received = run_journal(matches, HashMap::new(), None).await;
        assert_eq!(received.len(), 2);
        assert_eq!(
            message(&received[0]),
            Value::Bytes("Different timestamps".into())
        );
        assert_eq!(timestamp(&received[0]), value_ts(1578529839, 140005000));
        assert_eq!(
            message(&received[1]),
            Value::Bytes("Non-ASCII in other field".into())
        );
        assert_eq!(timestamp(&received[1]), value_ts(1578529839, 140005000));
    }

    #[tokio::test]
    async fn includes_kernel() {
        let matches = create_matches(vec![("_TRANSPORT", "kernel")]);
        let received = run_journal(matches, HashMap::new(), None).await;
        assert_eq!(received.len(), 1);
        assert_eq!(timestamp(&received[0]), value_ts(1578529839, 140006000));
        assert_eq!(message(&received[0]), Value::Bytes("audit log".into()));
    }

    #[tokio::test]
    async fn excludes_matches() {
        let matches = create_matches(vec![("PRIORITY", "INFO"), ("PRIORITY", "DEBUG")]);
        let received = run_journal(HashMap::new(), matches, None).await;
        assert_eq!(received.len(), 5);
        assert_eq!(timestamp(&received[0]), value_ts(1578529839, 140003000));
        assert_eq!(timestamp(&received[1]), value_ts(1578529839, 140004000));
        assert_eq!(timestamp(&received[2]), value_ts(1578529839, 140005000));
        assert_eq!(timestamp(&received[3]), value_ts(1578529839, 140005000));
        assert_eq!(timestamp(&received[4]), value_ts(1578529839, 140006000));
    }

    #[tokio::test]
    async fn handles_checkpoint() {
        let received = run_with_units(&[], &[], Some("1")).await;
        assert_eq!(received.len(), 7);
        assert_eq!(message(&received[0]), Value::Bytes("unit message".into()));
        assert_eq!(timestamp(&received[0]), value_ts(1578529839, 140002000));
    }

    #[tokio::test]
    async fn parses_array_messages() {
        let received = run_with_units(&["badunit.service"], &[], None).await;
        assert_eq!(received.len(), 1);
        assert_eq!(message(&received[0]), Value::Bytes("¿Hello?".into()));
    }

    #[tokio::test]
    async fn parses_array_fields() {
        let received = run_with_units(&["syslog.service"], &[], None).await;
        assert_eq!(received.len(), 1);
        assert_eq!(
            received[0].as_log()["SYSLOG_RAW"],
            Value::Bytes("¿World?".into())
        );
    }

    #[tokio::test]
    async fn parses_string_sequences() {
        let received = run_with_units(&["NetworkManager.service"], &[], None).await;
        assert_eq!(received.len(), 1);
        assert_eq!(
            received[0].as_log()["SYSLOG_FACILITY"],
            Value::Bytes(r#"["DHCP4","DHCP6"]"#.into())
        );
    }

    #[tokio::test]
    async fn handles_missing_timestamp() {
        let received = run_with_units(&["stdout"], &[], None).await;
        assert_eq!(received.len(), 2);
        assert_eq!(timestamp(&received[0]), value_ts(1578529839, 140004000));
        assert_eq!(timestamp(&received[1]), value_ts(1578529839, 140005000));
    }

    #[tokio::test]
    async fn handles_acknowledgements() {
        let (tx, mut rx) = SourceSender::new_test();

        let tempdir = tempdir().unwrap();
        let mut checkpoint_path = tempdir.path().to_path_buf();
        checkpoint_path.push(CHECKPOINT_FILENAME);

        let checkpointer = StatefulCheckpointer::new(checkpoint_path.clone())
            .await
            .expect("Creating checkpointer failed!");

        let mut source = JournaldSource {
            include_matches: Default::default(),
            exclude_matches: Default::default(),
            checkpoint_path,
            batch_size: DEFAULT_BATCH_SIZE,
            remap_priority: true,
            out: tx,
            acknowledgements: true,
        };
        let RunningJournal(stream, _stop) = FakeJournal::new(&checkpointer.cursor);
        let checkpointer = SharedCheckpointer::new(checkpointer);
        let (_trigger, shutdown, _tripwire) = ShutdownSignal::new_wired();
        let finalizer = Finalizer::new(true, checkpointer.clone(), shutdown);

        // The source runs to completion without setting the checkpoint
        source.run_stream(stream, &finalizer).await;

        // Make sure the checkpointer cursor is empty
        assert_eq!(checkpointer.lock().await.cursor, None);

        // Acknowledge all the received events.
        let mut count = 0;
        while let Poll::Ready(Some(event)) = futures::poll!(rx.next()) {
            // The checkpointer shouldn't set the cursor until the end of the batch.
            assert_eq!(checkpointer.lock().await.cursor, None);
            event.metadata().update_status(EventStatus::Delivered);
            count += 1;
        }
        assert_eq!(count, 8);

        // Let the async finalizer update the cursor
        tokio::task::yield_now().await;
        assert_eq!(checkpointer.lock().await.cursor.as_deref(), Some("6"));
    }

    #[test]
    fn filter_matches_works_correctly() {
        let empty: Matches = HashMap::new();
        let includes = create_unit_matches(vec!["one", "two"]);
        let excludes = create_unit_matches(vec!["foo", "bar"]);

        let zero = HashMap::new();
        assert!(!filter_matches(&zero, &empty, &empty));
        assert!(filter_matches(&zero, &includes, &empty));
        assert!(!filter_matches(&zero, &empty, &excludes));
        assert!(filter_matches(&zero, &includes, &excludes));
        let mut one = HashMap::new();
        one.insert(String::from(SYSTEMD_UNIT), String::from("one"));
        assert!(!filter_matches(&one, &empty, &empty));
        assert!(!filter_matches(&one, &includes, &empty));
        assert!(!filter_matches(&one, &empty, &excludes));
        assert!(!filter_matches(&one, &includes, &excludes));
        let mut two = HashMap::new();
        two.insert(String::from(SYSTEMD_UNIT), String::from("bar"));
        assert!(!filter_matches(&two, &empty, &empty));
        assert!(filter_matches(&two, &includes, &empty));
        assert!(filter_matches(&two, &empty, &excludes));
        assert!(filter_matches(&two, &includes, &excludes));
    }

    #[test]
    fn merges_units_and_matches_option() {
        let include_units = vec!["one", "two"].into_iter().map(String::from).collect();
        let include_matches = create_matches(vec![
            ("_SYSTEMD_UNIT", "three.service"),
            ("_TRANSPORT", "kernel"),
        ]);

        let exclude_units = vec!["foo", "bar"].into_iter().map(String::from).collect();
        let exclude_matches = create_matches(vec![
            ("_SYSTEMD_UNIT", "baz.service"),
            ("PRIORITY", "DEBUG"),
        ]);

        let journald_config = JournaldConfig {
            include_units,
            include_matches,
            exclude_units,
            exclude_matches,
            ..Default::default()
        };

        let hashset =
            |v: &[&str]| -> HashSet<String> { v.iter().copied().map(String::from).collect() };

        let matches = journald_config.merged_include_matches().unwrap();
        let units = matches.get("_SYSTEMD_UNIT").unwrap();
        assert_eq!(
            units,
            &hashset(&["one.service", "two.service", "three.service"])
        );
        let units = matches.get("_TRANSPORT").unwrap();
        assert_eq!(units, &hashset(&["kernel"]));

        let matches = journald_config.merged_exclude_matches();
        let units = matches.get("_SYSTEMD_UNIT").unwrap();
        assert_eq!(
            units,
            &hashset(&["foo.service", "bar.service", "baz.service"])
        );
        let units = matches.get("PRIORITY").unwrap();
        assert_eq!(units, &hashset(&["DEBUG"]));
    }

    #[test]
    fn find_duplicate_match_works_correctly() {
        let include_matches = create_matches(vec![("_TRANSPORT", "kernel")]);
        let exclude_matches = create_matches(vec![("_TRANSPORT", "kernel")]);
        let (field, value) = find_duplicate_match(&include_matches, &exclude_matches).unwrap();
        assert_eq!(field, "_TRANSPORT");
        assert_eq!(value, "kernel");

        let empty = HashMap::new();
        let actual = find_duplicate_match(&empty, &empty);
        assert!(actual.is_none());

        let actual = find_duplicate_match(&include_matches, &empty);
        assert!(actual.is_none());

        let actual = find_duplicate_match(&empty, &exclude_matches);
        assert!(actual.is_none());
    }

    #[test]
    fn command_options() {
        let path = PathBuf::from("jornalctl");

        let journal_dir = None;
        let current_boot_only = false;
        let cursor = None;
        let since_now = false;

        let command = create_command(&path, journal_dir, current_boot_only, since_now, &cursor);
        let cmd_line = format!("{:?}", command);
        assert!(!cmd_line.contains("--directory="));
        assert!(!cmd_line.contains("--boot"));
        assert!(cmd_line.contains("--since=2000-01-01"));

        let since_now = true;

        let command = create_command(&path, journal_dir, current_boot_only, since_now, &cursor);
        let cmd_line = format!("{:?}", command);
        assert!(cmd_line.contains("--since=now"));

        let journal_dir = Some(PathBuf::from("/tmp/journal-dir"));
        let current_boot_only = true;
        let cursor = Some(String::from("2021-01-01"));

        let command = create_command(
            &path,
            journal_dir.as_ref(),
            current_boot_only,
            since_now,
            &cursor,
        );
        let cmd_line = format!("{:?}", command);
        assert!(cmd_line.contains("--directory=/tmp/journal-dir"));
        assert!(cmd_line.contains("--boot"));
        assert!(cmd_line.contains("--after-cursor="));
    }

    fn message(event: &Event) -> Value {
        event.as_log()[log_schema().message_key()].clone()
    }

    fn timestamp(event: &Event) -> Value {
        event.as_log()[log_schema().timestamp_key()].clone()
    }

    fn value_ts(secs: i64, usecs: u32) -> Value {
        Value::Timestamp(chrono::Utc.timestamp(secs, usecs))
    }

    fn priority(event: &Event) -> Value {
        event.as_log()["PRIORITY"].clone()
    }
}
