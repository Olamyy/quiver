/// Server initialization and cleanup for benchmarks
/// Ensures isolated test environments with proper resource cleanup via Drop trait.
use std::process::{Child, Command};
use std::thread;
use std::time::Duration;

/// Spawned Quiver server with hardened cleanup to prevent TCP port exhaustion.
/// Implements Drop to forcefully terminate the process and wait for port release.
pub struct QuiverTestServer {
    process: Child,
    pub port: u16,
}

impl QuiverTestServer {
    /// Spawns a Quiver server on the specified port with the given configuration.
    ///
    /// # Arguments
    /// * `config_path` - Relative path to config YAML (from project root)
    /// * `port` - Port to bind server on
    ///
    /// # Panics
    /// If the server fails to spawn or readiness check fails within timeout.
    pub async fn spawn(config_path: &str, port: u16) -> Self {
        let child = Command::new("cargo")
            .args(&["run", "--release", "--"])
            .arg("--config")
            .arg(config_path)
            .env("RUST_LOG", "warn")
            .spawn()
            .expect("Failed to spawn Quiver server");

        let mut server = QuiverTestServer {
            process: child,
            port,
        };

        server.wait_for_readiness(Duration::from_secs(10)).await;
        server
    }

    /// Polls server readiness until it responds on the port or timeout occurs.
    /// Panics if server fails to become ready within the specified timeout.
    async fn wait_for_readiness(&mut self, timeout: Duration) {
        let start = std::time::Instant::now();
        loop {
            if self.is_ready() {
                return;
            }
            if start.elapsed() > timeout {
                panic!("Server failed to become ready within {:?}", timeout);
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Checks if server is responding by attempting a TCP connection to the bound port.
    fn is_ready(&self) -> bool {
        std::net::TcpStream::connect(format!("127.0.0.1:{}", self.port)).is_ok()
    }
}

impl Drop for QuiverTestServer {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();

        thread::sleep(Duration::from_millis(100));
    }
}
