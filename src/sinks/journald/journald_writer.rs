use std::{io, os::unix::net::UnixDatagram};
use std::path::{Path, PathBuf};

/// A writer for journald that sends log messages over a Unix domain socket.
/// For protocol details, see [this link](https://systemd.io/JOURNAL_NATIVE_PROTOCOL/)
pub struct JournaldWriter {
    journald_path: PathBuf,
    socket: UnixDatagram,
    buf: Vec<u8>,
}

impl JournaldWriter {
    pub fn new(journald_path: impl AsRef<Path>) -> io::Result<Self> {
        let socket = UnixDatagram::unbound()?;
        let writer = Self {
            journald_path: journald_path.as_ref().to_path_buf(),
            socket,
            buf: vec![],
        };
        // Send an empty payload to ensure the socket is ready for use.
        // This will be ignored by journald.
        writer.send_payload(&[])?;
        Ok(writer)
    }

    /// Add a string field to the buffer.
    pub fn add_str(&mut self, key: &str, value: &str) {
        self.write_with_length(key, |w| {
            w.buf.extend_from_slice(value.as_bytes());
        });
    }

    /// Add a field with arbitrary bytes to the buffer.
    // pub fn add_bytes(&mut self, key: &str, value: &[u8]) {
    //     self.write_with_length(key, |w| {
    //         w.buf.extend_from_slice(value);
    //     });
    // }

    pub fn flush(&mut self) -> io::Result<usize> {
        if !self.buf.is_empty() {
            let bytes_sent = self.send_payload(&self.buf)?;
            // Clear the buffer after sending
            // We could also keep the buffer for reuse, but by doing this we ensure that
            // we don't allocate too much memory for long time in case of rare large payloads.
            self.buf = vec![];
            Ok(bytes_sent)
        } else {
            Ok(0)
        }
    }


    fn send_payload(&self, payload: &[u8]) -> io::Result<usize> {
        self.socket
            .send_to(payload, self.journald_path.as_path())
            .or_else(|error| {
                if Some(nix::libc::EMSGSIZE) == error.raw_os_error() {
                    // If the payload is too large, we should try to send it via a memfd, currently
                    // this is not implemented.
                    Err(error)
                } else {
                    Err(error)
                }
            })
    }

    /// Append a sanitized and length-encoded field into the buffer.
    fn write_with_length(&mut self, key: &str, write_cb: impl FnOnce(&mut Self)) {
        self.sanitize_key(key);
        self.buf.push(b'\n');
        self.buf.extend_from_slice(&[0; 8]); // Length tag, to be populated after writing the value
        let start = self.buf.len();
        write_cb(self);
        let end = self.buf.len();
        self.buf[start - 8..start].copy_from_slice(&((end - start) as u64).to_le_bytes());
        self.buf.push(b'\n');
    }

    /// Sanitize a key and convert it to uppercase.
    fn sanitize_key(&mut self, key: &str) {
        self.buf.extend(
            key.bytes()
                .map(|c| match c {
                    // As per journald protocol, '=' and '\n' are illegal in keys so we replace them with '_'.
                    b'=' | b'\n' => b'_',
                    _ => c,
                })
                .filter(|&c| c == b'_' || char::from(c).is_ascii_alphanumeric())
                .map(|c| char::from(c).to_ascii_uppercase() as u8),
        );
    }
}
