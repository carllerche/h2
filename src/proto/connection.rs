use {client, frame, server, proto};
use frame::Reason;
use codec::SendError;

use proto::*;

use http::Request;
use futures::{Sink, Stream};
use bytes::{Bytes, IntoBuf};
use tokio_io::{AsyncRead, AsyncWrite};

use std::marker::PhantomData;

/// An H2 connection
#[derive(Debug)]
pub(crate) struct Connection<T, P, B: IntoBuf = Bytes>
    where P: Peer,
{
    /// Tracks the connection level state transitions.
    state: State,

    /// Read / write frame values
    codec: Codec<T, Prioritized<B::Buf>>,

    /// Ping/pong handler
    ping_pong: PingPong<Prioritized<B::Buf>>,

    /// Connection settings
    settings: Settings,

    /// Stream state handler
    streams: Streams<B::Buf, P>,

    /// Client or server
    _phantom: PhantomData<P>,
}

#[derive(Debug)]
enum State {
    /// Currently open in a sane state
    Open,

    /// Waiting to send a GO_AWAY frame
    GoAway(frame::GoAway),

    /// The codec must be flushed
    Flush(Reason),

    /// In an errored state
    Error(Reason),
}

impl<T, P, B> Connection<T, P, B>
    where T: AsyncRead + AsyncWrite,
          P: Peer,
          B: IntoBuf,
{
    pub fn new(codec: Codec<T, Prioritized<B::Buf>>) -> Connection<T, P, B> {
        // TODO: Actually configure
        let streams = Streams::new(streams::Config {
            max_remote_initiated: None,
            init_remote_window_sz: DEFAULT_INITIAL_WINDOW_SIZE,
            max_local_initiated: None,
            init_local_window_sz: DEFAULT_INITIAL_WINDOW_SIZE,
        });

        Connection {
            state: State::Open,
            codec: codec,
            ping_pong: PingPong::new(),
            settings: Settings::new(),
            streams: streams,
            _phantom: PhantomData,
        }
    }

    /// Returns `Ready` when the connection is ready to receive a frame.
    fn poll_ready(&mut self) -> Poll<(), SendError> {
        // The order of these calls don't really matter too much as only one
        // should have pending work.
        try_ready!(self.ping_pong.send_pending_pong(&mut self.codec));
        try_ready!(self.settings.send_pending_ack(&mut self.codec, &mut self.streams));
        try_ready!(self.streams.send_pending_refusal(&mut self.codec));

        Ok(().into())
    }

    /// Advances the internal state of the connection.
    pub fn poll(&mut self) -> Poll<(), proto::Error> {
        use proto::Error::*;

        loop {
            match self.state {
                // When open, continue to poll a frame
                State::Open => {},
                // In an error state
                _ => {
                    try_ready!(self.poll_complete());

                    // GO_AWAY frame has been sent, return the error
                    return Err(self.state.error().unwrap().into());
                }
            }

            match self.poll2() {
                Err(Proto(e)) => {
                    debug!("Connection::poll; err={:?}", e);
                    let last_processed_id = self.streams.recv_err(&e.into());
                    let frame = frame::GoAway::new(last_processed_id, e);

                    self.state = State::GoAway(frame);
                }
                Err(e) => {
                    // TODO: Are I/O errors recoverable?
                    self.streams.recv_err(&e);
                    return Err(e);
                }
                ret => return ret,
            }
        }
    }

    fn poll2(&mut self) -> Poll<(), proto::Error> {
        use frame::Frame::*;
        use codec::RecvError::*;

        loop {
            // First, ensure that the `Connection` is able to receive a frame
            try_ready!(self.poll_ready());

            trace!("polling codec");

            // Poll a frame from the codec.
            let frame = match self.codec.poll() {
                // Received a frame
                Ok(Async::Ready(frame)) => frame,
                // Socket not ready, try to flush any pending data
                Ok(Async::NotReady) => {
                    // Flush any pending writes
                    let _ = self.poll_complete()?;

                    // The codec is not read
                    return Ok(Async::NotReady);
                }
                // Connection level error, set GO_AWAY and close connection
                Err(Connection(reason)) => {
                    return Err(reason.into());
                }
                // Stream level error, reset the stream and try to poll again
                Err(Stream { id, reason }) => {
                    trace!("stream level error; id={:?}; reason={:?}", id, reason);
                    self.streams.send_reset(id, reason);
                    continue;
                }
                // I/O error, nothing more can be done
                Err(Io(err)) => {
                    return Err(err.into());
                }
            };

            debug!("recv; frame={:?}", frame);

            match frame {
                Some(Headers(frame)) => {
                    trace!("recv HEADERS; frame={:?}", frame);
                    try!(self.streams.recv_headers(frame));
                }
                Some(Data(frame)) => {
                    trace!("recv DATA; frame={:?}", frame);
                    try!(self.streams.recv_data(frame));
                }
                Some(Reset(frame)) => {
                    trace!("recv RST_STREAM; frame={:?}", frame);
                    try!(self.streams.recv_reset(frame));
                }
                Some(PushPromise(frame)) => {
                    trace!("recv PUSH_PROMISE; frame={:?}", frame);
                    self.streams.recv_push_promise(frame)?;
                }
                Some(Settings(frame)) => {
                    trace!("recv SETTINGS; frame={:?}", frame);
                    self.settings.recv_settings(frame);
                }
                Some(GoAway(_)) => {
                    // TODO: handle the last_processed_id. Also, should this be
                    // handled as an error?
                    // let _ = RecvError::Proto(frame.reason());
                    return Ok(().into());
                }
                Some(Ping(frame)) => {
                    trace!("recv PING; frame={:?}", frame);
                    self.ping_pong.recv_ping(frame);
                }
                Some(WindowUpdate(frame)) => {
                    trace!("recv WINDOW_UPDATE; frame={:?}", frame);
                    self.streams.recv_window_update(frame)?;
                }
                Some(Priority(frame)) => {
                    trace!("recv PRIORITY; frame={:?}", frame);
                    // TODO: handle
                }
                None => {
                    // TODO: Is this correct?
                    trace!("codec closed");
                    return Ok(Async::Ready(()));
                }
            }
        }
    }

    fn poll_complete(&mut self) -> Poll<(), SendError> {
        loop {
            match self.state {
                State::Open => {
                    try_ready!(self.poll_ready());

                    // Ensure all window updates have been sent.
                    try_ready!(self.streams.poll_complete(&mut self.codec));

                    return Ok(().into());
                }
                State::GoAway(frame) => {
                    if !self.codec.start_send(frame.into())?.is_ready() {
                        // Not ready to send the frame... try again later.
                        return Ok(Async::NotReady);
                    }

                    // GO_AWAY sent, transition the connection to an errored state
                    self.state = State::Flush(frame.reason());
                }
                State::Flush(reason) => {
                    try_ready!(self.codec.poll_complete());
                    self.state = State::Error(reason);
                }
                State::Error(..) => {
                    return Ok(().into());
                }
            }
        }
    }
}

impl<T, B> Connection<T, client::Peer, B>
    where T: AsyncRead + AsyncWrite,
          B: IntoBuf,
{
    /// Returns `Ready` when new the connection is able to support a new request stream.
    pub fn poll_send_request_ready(&mut self) -> Async<()> {
        self.streams.poll_send_request_ready()
    }

    /// Initialize a new HTTP/2.0 stream and send the message.
    pub fn send_request(&mut self, request: Request<()>, end_of_stream: bool)
        -> Result<StreamRef<B::Buf, client::Peer>, SendError>
    {
        self.streams.send_request(request, end_of_stream)
    }
}

impl<T, B> Connection<T, server::Peer, B>
    where T: AsyncRead + AsyncWrite,
          B: IntoBuf,
{
    pub fn next_incoming(&mut self) -> Option<StreamRef<B::Buf, server::Peer>> {
        self.streams.next_incoming()
    }
}

// ====== impl State =====

impl State {
    fn error(&self) -> Option<Reason> {
        match *self {
            State::Error(reason) => Some(reason),
            _ => None,
        }
    }
}
