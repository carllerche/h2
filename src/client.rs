use {frame, ConnectionError, StreamId};
use proto::{self, Connection};
use error::Reason::*;

use http::{self, Request, Response};
use futures::{Future, Poll, Sink, AsyncSink};
use tokio_io::{AsyncRead, AsyncWrite};
use bytes::{Bytes, IntoBuf};

use std::fmt;

/// In progress H2 connection binding
pub struct Handshake<T, B: IntoBuf = Bytes> {
    // TODO: unbox
    inner: Box<Future<Item = Client<T, B>, Error = ConnectionError>>,
}

#[derive(Debug)]
pub(crate) struct Peer;

/// Marker type indicating a client peer
pub struct Client<T, B: IntoBuf> {
    connection: Connection<T, Peer, B>,
}

/// Client half of an active HTTP/2.0 stream.
#[derive(Debug)]
pub struct Stream<B: IntoBuf> {
    inner: proto::StreamRef<Peer, B::Buf>,
}

impl<T> Client<T, Bytes>
    where T: AsyncRead + AsyncWrite + 'static,
{
    pub fn handshake(io: T) -> Handshake<T, Bytes> {
        Client::handshake2(io)
    }
}

impl<T, B> Client<T, B>
    // TODO: Get rid of 'static
    where T: AsyncRead + AsyncWrite + 'static,
          B: IntoBuf + 'static,
{
    /// Bind an H2 client connection.
    ///
    /// Returns a future which resolves to the connection value once the H2
    /// handshake has been completed.
    pub fn handshake2(io: T) -> Handshake<T, B> {
        use tokio_io::io;

        debug!("binding client connection");

        let handshake = io::write_all(io, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
            .map_err(ConnectionError::from)
            .and_then(|(io, _)| {
                debug!("client connection bound");

                let mut framed_write = proto::framed_write(io);
                let settings = frame::Settings::default();

                // Send initial settings frame
                match framed_write.start_send(settings.into()) {
                    Ok(AsyncSink::Ready) => {
                        let conn = proto::from_framed_write(framed_write);
                        Ok(Client { connection: conn })
                    }
                    Ok(_) => unreachable!(),
                    Err(e) => Err(ConnectionError::from(e)),
                }
            });

        Handshake { inner: Box::new(handshake) }
    }

    /// Returns `Ready` when the connection can initialize a new HTTP 2.0
    /// stream.
    pub fn poll_ready(&mut self) -> Poll<(), ConnectionError> {
        self.connection.poll_ready()
    }

    /// Send a request on a new HTTP 2.0 stream
    pub fn request(&mut self, request: Request<()>, end_of_stream: bool)
        -> Result<Stream<B>, ConnectionError>
    {
        self.connection.send_request(request, end_of_stream)
            .map(|stream| Stream {
                inner: stream,
            })
    }
}

impl<T, B> Future for Client<T, B>
    // TODO: Get rid of 'static
    where T: AsyncRead + AsyncWrite + 'static,
          B: IntoBuf + 'static,
{
    type Item = ();
    type Error = ConnectionError;

    fn poll(&mut self) -> Poll<(), ConnectionError> {
        self.connection.poll()
    }
}

impl<T, B> fmt::Debug for Client<T, B>
    where T: fmt::Debug,
          B: fmt::Debug + IntoBuf,
          B::Buf: fmt::Debug + IntoBuf,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Client")
            .field("connection", &self.connection)
            .finish()
    }
}

// ===== impl Handshake =====

impl<T, B: IntoBuf> Future for Handshake<T, B> {
    type Item = Client<T, B>;
    type Error = ConnectionError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.inner.poll()
    }
}

impl<T, B> fmt::Debug for Handshake<T, B>
    where T: fmt::Debug,
          B: fmt::Debug + IntoBuf,
          B::Buf: fmt::Debug + IntoBuf,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "client::Handshake")
    }
}

// ===== impl Stream =====

impl<B: IntoBuf> Stream<B> {
    /// Receive the HTTP/2.0 response, if it is ready.
    pub fn poll_response(&mut self) -> Poll<Response<()>, ConnectionError> {
        self.inner.poll_response()
    }

    /// Send data
    pub fn send_data(&mut self, data: B, end_of_stream: bool)
        -> Result<(), ConnectionError>
    {
        unimplemented!();
    }

    /// Send trailers
    pub fn send_trailers(&mut self, trailers: ())
        -> Result<(), ConnectionError>
    {
        unimplemented!();
    }
}

impl<B: IntoBuf> Future for Stream<B> {
    type Item = Response<()>;
    type Error = ConnectionError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.poll_response()
    }
}

// ===== impl Peer =====

impl proto::Peer for Peer {
    type Send = Request<()>;
    type Poll = Response<()>;

    fn is_server() -> bool {
        false
    }

    fn convert_send_message(
        id: StreamId,
        request: Self::Send,
        end_of_stream: bool) -> frame::Headers
    {
        use http::request::Parts;

        let (Parts { method, uri, headers, .. }, _) = request.into_parts();

        // Build the set pseudo header set. All requests will include `method`
        // and `path`.
        let pseudo = frame::Pseudo::request(method, uri);

        // Create the HEADERS frame
        let mut frame = frame::Headers::new(id, pseudo, headers);

        if end_of_stream {
            frame.set_end_stream()
        }

        frame
    }

    fn convert_poll_message(headers: frame::Headers) -> Result<Self::Poll, ConnectionError> {
        headers.into_response()
            // TODO: Is this always a protocol error?
            .map_err(|_| ProtocolError.into())
    }
}
