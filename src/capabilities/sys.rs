//! Cross-platform process execution.
//!
//! This capability owns argv-safe spawning, bounded buffered output, process
//! handles, and cancellation without exposing platform-specific lifecycle code.

use std::io;
use std::process::{ExitStatus, Stdio};

use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::{Child, Command};

use crate::runtime::cancellation::CancellationToken;
use crate::runtime::error::{RuntimeError, RuntimeResult};
use crate::runtime::router::{RouteFuture, RouteHandler};

/// One argv-safe process execution request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessRequest {
    program: String,
    arguments: Vec<String>,
    environment: Vec<(String, String)>,
}

impl ProcessRequest {
    /// Creates a request for `program` with no arguments or environment entries.
    pub fn new(program: impl Into<String>) -> Self {
        Self {
            program: program.into(),
            arguments: Vec::new(),
            environment: Vec::new(),
        }
    }

    /// Appends one argument without shell interpretation.
    pub fn arg(mut self, argument: impl Into<String>) -> Self {
        self.arguments.push(argument.into());
        self
    }

    /// Appends an explicit environment entry for the child process.
    pub fn env(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.environment.push((name.into(), value.into()));
        self
    }
}

/// Buffered status and output from a completed process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessOutput {
    success: bool,
    exit_code: Option<i32>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

impl ProcessOutput {
    /// Returns whether the process reported successful termination.
    pub fn success(&self) -> bool {
        self.success
    }

    /// Returns the process exit code, when the platform supplied one.
    pub fn exit_code(&self) -> Option<i32> {
        self.exit_code
    }

    /// Returns the captured standard output bytes.
    pub fn stdout(&self) -> &[u8] {
        &self.stdout
    }

    /// Returns the captured standard error bytes.
    pub fn stderr(&self) -> &[u8] {
        &self.stderr
    }
}

/// A process-execution grant with a fixed output limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProcessCapability {
    max_output_bytes_per_stream: usize,
}

impl ProcessCapability {
    /// Creates a process capability with a separate byte limit for each output stream.
    pub fn new(max_output_bytes_per_stream: usize) -> Self {
        Self {
            max_output_bytes_per_stream,
        }
    }

    /// Returns the byte limit applied independently to stdout and stderr.
    pub fn max_output_bytes_per_stream(&self) -> usize {
        self.max_output_bytes_per_stream
    }
}

impl RouteHandler<ProcessRequest, ProcessOutput> for ProcessCapability {
    fn handle(
        &self,
        cancellation: CancellationToken,
        request: ProcessRequest,
    ) -> RouteFuture<ProcessOutput> {
        Box::pin(execute_process(
            cancellation,
            request,
            self.max_output_bytes_per_stream,
        ))
    }
}

async fn execute_process(
    cancellation: CancellationToken,
    request: ProcessRequest,
    max_output_bytes_per_stream: usize,
) -> RuntimeResult<ProcessOutput> {
    if cancellation.is_cancelled() {
        return Err(RuntimeError::cancelled());
    }

    let mut command = Command::new(&request.program);
    command
        .args(&request.arguments)
        .env_clear()
        .envs(request.environment)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let mut child = command.spawn().map_err(|error| {
        RuntimeError::capability_failed(format!("failed to spawn process: {error}"))
    })?;
    let stdout = child
        .stdout
        .take()
        .expect("piped process stdout was not available");
    let stderr = child
        .stderr
        .take()
        .expect("piped process stderr was not available");

    let mut completion = Box::pin(async {
        let (status, stdout, stderr) = tokio::try_join!(
            async { child.wait().await.map_err(ProcessFailure::WaitFailed) },
            read_bounded(stdout, max_output_bytes_per_stream, OutputStream::Stdout),
            read_bounded(stderr, max_output_bytes_per_stream, OutputStream::Stderr),
        )?;
        Ok::<_, ProcessFailure>((status, stdout, stderr))
    });

    let result = tokio::select! {
        biased;
        _ = cancellation.cancelled() => {
            drop(completion);
            terminate_and_reap(&mut child).await.map_err(cleanup_error)?;
            return Err(RuntimeError::cancelled());
        }
        result = &mut completion => result,
    };
    drop(completion);

    match result {
        Ok((status, stdout, stderr)) => Ok(process_output(status, stdout, stderr)),
        Err(failure) => {
            terminate_and_reap(&mut child)
                .await
                .map_err(cleanup_error)?;
            Err(failure.into_runtime_error())
        }
    }
}

fn process_output(status: ExitStatus, stdout: Vec<u8>, stderr: Vec<u8>) -> ProcessOutput {
    ProcessOutput {
        success: status.success(),
        exit_code: status.code(),
        stdout,
        stderr,
    }
}

async fn read_bounded<Reader>(
    mut reader: Reader,
    limit: usize,
    stream: OutputStream,
) -> Result<Vec<u8>, ProcessFailure>
where
    Reader: AsyncRead + Unpin,
{
    let mut output = Vec::with_capacity(limit.min(8 * 1024));
    let mut buffer = [0; 8 * 1024];

    loop {
        let count = reader
            .read(&mut buffer)
            .await
            .map_err(|source| ProcessFailure::ReadFailed { stream, source })?;
        if count == 0 {
            return Ok(output);
        }
        if output.len().saturating_add(count) > limit {
            return Err(ProcessFailure::OutputLimitExceeded { stream, limit });
        }
        output.extend_from_slice(&buffer[..count]);
    }
}

async fn terminate_and_reap(child: &mut Child) -> io::Result<()> {
    if child.try_wait()?.is_some() {
        return Ok(());
    }

    if let Err(kill_error) = child.start_kill() {
        return match child.try_wait() {
            Ok(Some(_)) => Ok(()),
            _ => Err(kill_error),
        };
    }

    child.wait().await.map(|_| ())
}

fn cleanup_error(error: io::Error) -> RuntimeError {
    RuntimeError::capability_failed(format!("failed to terminate and reap process: {error}"))
}

#[derive(Clone, Copy)]
enum OutputStream {
    Stdout,
    Stderr,
}

impl OutputStream {
    fn name(self) -> &'static str {
        match self {
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
        }
    }
}

enum ProcessFailure {
    WaitFailed(io::Error),
    ReadFailed {
        stream: OutputStream,
        source: io::Error,
    },
    OutputLimitExceeded {
        stream: OutputStream,
        limit: usize,
    },
}

impl ProcessFailure {
    fn into_runtime_error(self) -> RuntimeError {
        match self {
            Self::WaitFailed(error) => {
                RuntimeError::capability_failed(format!("failed to wait for process: {error}"))
            }
            Self::ReadFailed { stream, source } => RuntimeError::capability_failed(format!(
                "failed to read process {}: {source}",
                stream.name()
            )),
            Self::OutputLimitExceeded { stream, limit } => {
                RuntimeError::resource_limit_exceeded(format!(
                    "process {} exceeded {limit} byte output limit",
                    stream.name()
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;
    use std::time::Duration;

    use tokio::time::{sleep, timeout};

    use super::*;
    use crate::runtime::error::ErrorCode;
    use crate::runtime::router::Router;
    use crate::runtime::task::RequestContext;

    const FIXTURE_PATH_ENV: &str = "HOSTKIT_SYS_CANCELLATION_FIXTURE_PATH";
    const FIXTURE_VALUE_ENV: &str = "HOSTKIT_SYS_EXPLICIT_FIXTURE_VALUE";
    const FIXTURE_TEST: &str = "capabilities::sys::tests::cancellation_process_fixture";
    const ENVIRONMENT_FIXTURE_TEST: &str =
        "capabilities::sys::tests::explicit_environment_process_fixture";
    static NEXT_FIXTURE_ID: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn cancellation_process_fixture() {
        let Some(path) = std::env::var_os(FIXTURE_PATH_ENV) else {
            return;
        };

        fs::write(path, process::id().to_string()).unwrap();
        loop {
            thread::park_timeout(Duration::from_secs(60));
        }
    }

    #[test]
    fn explicit_environment_process_fixture() {
        let Some(value) = std::env::var_os(FIXTURE_VALUE_ENV) else {
            return;
        };

        assert!(std::env::var_os("PATH").is_none());
        print!("{}", value.to_string_lossy());
    }

    #[tokio::test]
    async fn reports_spawn_failures_through_capability_boundary() {
        let capability = ProcessCapability::new(1024);
        let error = capability
            .handle(
                CancellationToken::new(),
                ProcessRequest::new("hostkit-program-that-does-not-exist"),
            )
            .await
            .unwrap_err();

        assert_eq!(error.code(), ErrorCode::CapabilityFailed);
        assert!(error.message().starts_with("failed to spawn process:"));
    }

    #[tokio::test]
    async fn rejects_cancellation_before_spawning_process() {
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let capability = ProcessCapability::new(1024);
        let error = capability
            .handle(
                cancellation,
                ProcessRequest::new("hostkit-program-that-does-not-exist"),
            )
            .await
            .unwrap_err();

        assert_eq!(error.code(), ErrorCode::Cancelled);
    }

    #[tokio::test]
    async fn returns_success_status_and_buffered_output() {
        let capability = ProcessCapability::new(64 * 1024);
        let output = capability
            .handle(
                CancellationToken::new(),
                ProcessRequest::new(current_executable())
                    .arg("--exact")
                    .arg(ENVIRONMENT_FIXTURE_TEST)
                    .arg("--nocapture")
                    .env(FIXTURE_VALUE_ENV, "explicit-environment-value"),
            )
            .await
            .unwrap();

        assert!(output.success());
        assert_eq!(output.exit_code(), Some(0));
        assert!(String::from_utf8_lossy(output.stdout()).contains("explicit-environment-value"));
        assert!(String::from_utf8_lossy(output.stdout()).contains("1 passed"));
        assert!(output.stderr().is_empty());
    }

    #[tokio::test]
    async fn enforces_bounded_output_and_reaps_process() {
        let executable = current_executable();
        let capability = ProcessCapability::new(0);
        let error = capability
            .handle(
                CancellationToken::new(),
                ProcessRequest::new(executable).arg("--list"),
            )
            .await
            .unwrap_err();

        assert_eq!(error.code(), ErrorCode::ResourceLimitExceeded);
        assert!(
            error
                .message()
                .contains("stdout exceeded 0 byte output limit")
        );
    }

    #[tokio::test]
    async fn request_cancellation_terminates_and_reaps_real_process() {
        let fixture_path = fixture_path();
        let executable = current_executable();
        let request = ProcessRequest::new(executable)
            .arg("--exact")
            .arg(FIXTURE_TEST)
            .arg("--nocapture")
            .env(FIXTURE_PATH_ENV, fixture_path.to_string_lossy());
        let mut router = Router::new();
        assert!(router.grant((), ProcessCapability::new(64 * 1024)));
        let context = Arc::new(RequestContext::new());
        let task = router.dispatch(&context, &(), request).unwrap();
        let pid = wait_for_fixture_pid(&fixture_path).await;
        let process = ProcessProbe::open(pid);
        assert!(!process.has_exited());

        timeout(Duration::from_secs(5), context.cancel())
            .await
            .expect("request cancellation did not terminate the process");

        let error = task.await.unwrap_err();
        assert_eq!(error.code(), ErrorCode::Cancelled);
        assert!(process.has_exited());
        assert_eq!(context.task_count(), 0);
        fs::remove_file(fixture_path).unwrap();
    }

    fn current_executable() -> String {
        std::env::current_exe()
            .unwrap()
            .to_str()
            .expect("test executable path is not UTF-8")
            .to_owned()
    }

    fn fixture_path() -> PathBuf {
        let fixture_directory = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(".scratch")
            .join("sys-tests");
        fs::create_dir_all(&fixture_directory).unwrap();
        let fixture_id = NEXT_FIXTURE_ID.fetch_add(1, Ordering::Relaxed);
        fixture_directory.join(format!("pid-{}-{fixture_id}.txt", process::id()))
    }

    async fn wait_for_fixture_pid(path: &Path) -> u32 {
        timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(pid) = fs::read_to_string(path) {
                    return pid.trim().parse().unwrap();
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("process fixture did not report its PID")
    }

    #[cfg(windows)]
    struct ProcessProbe(windows_sys::Win32::Foundation::HANDLE);

    #[cfg(windows)]
    impl ProcessProbe {
        fn open(pid: u32) -> Self {
            use windows_sys::Win32::System::Threading::{
                OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
            };

            let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
            assert!(!handle.is_null(), "failed to open process {pid}");
            Self(handle)
        }

        fn has_exited(&self) -> bool {
            use windows_sys::Win32::Foundation::STILL_ACTIVE;
            use windows_sys::Win32::System::Threading::GetExitCodeProcess;

            let mut exit_code = 0;
            assert_ne!(unsafe { GetExitCodeProcess(self.0, &mut exit_code) }, 0);
            exit_code != STILL_ACTIVE as u32
        }
    }

    #[cfg(windows)]
    impl Drop for ProcessProbe {
        fn drop(&mut self) {
            unsafe {
                windows_sys::Win32::Foundation::CloseHandle(self.0);
            }
        }
    }

    #[cfg(unix)]
    struct ProcessProbe(libc::pid_t);

    #[cfg(unix)]
    impl ProcessProbe {
        fn open(pid: u32) -> Self {
            let pid = libc::pid_t::try_from(pid).unwrap();
            assert_eq!(unsafe { libc::kill(pid, 0) }, 0, "failed to probe process");
            Self(pid)
        }

        fn has_exited(&self) -> bool {
            if unsafe { libc::kill(self.0, 0) } == 0 {
                return false;
            }

            io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
        }
    }
}
