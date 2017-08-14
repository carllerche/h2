use {client, server, ConnectionError, Frame};
use HeaderMap;
use frame::{self, StreamId};

use proto::*;

use http::{Request, Response};
use bytes::{Bytes, IntoBuf};
use tokio_io::{AsyncRead, AsyncWrite};

use std::marker::PhantomData;

/// An H2 connection
#[derive(Debug)]
pub(crate) struct Connection<T, P, B: IntoBuf = Bytes> {
    // Codec
    codec: Codec<T, Prioritized<B::Buf>>,
    ping_pong: PingPong<Prioritized<B::Buf>>,
    settings: Settings,
    streams: Streams<B::Buf>,
    _phantom: PhantomData<P>,
}

impl<T, P, B> Connection<T, P, B>
    where T: AsyncRead + AsyncWrite,
          P: Peer,
          B: IntoBuf,
{
    pub fn new(codec: Codec<T, Prioritized<B::Buf>>) -> Connection<T, P, B> {
        // TODO: Actually configure
        let streams = Streams::new::<P>(streams::Config {
            max_remote_initiated: None,
            init_remote_window_sz: DEFAULT_INITIAL_WINDOW_SIZE,
            max_local_initiated: None,
            init_local_window_sz: DEFAULT_INITIAL_WINDOW_SIZE,
        });

        Connection {
            codec: codec,
            ping_pong: PingPong::new(),
            settings: Settings::new(),
            streams: streams,
            _phantom: PhantomData,
        }
    }

    /// Returns `Ready` when the connection is ready to receive a frame.
    pub fn poll_ready(&mut self) -> Poll<(), ConnectionError> {
        // The order of these calls don't really matter too much as only one
        // should have pending work.
        try_ready!(self.ping_pong.send_pending_pong(&mut self.codec));
        try_ready!(self.settings.send_pending_ack(&mut self.codec, &mut self.streams));
        try_ready!(self.streams.send_pending_refusal(&mut self.codec));

        Ok(().into())
    }

    /// Returns `Ready` when new the connection is able to support a new request stream.
    pub fn poll_send_request_ready(&mut self) -> Poll<(), ConnectionError> {
        self.streams.poll_send_request_ready()
    }

    /// Advances the internal state of the connection.
    pub fn poll(&mut self) -> Poll<(), ConnectionError> {
        match self.poll2() {
            Err(e) => {
                debug!("Connection::poll; err={:?}", e);
                self.streams.recv_err(&e);
                Err(e)
            }
            ret => ret,
        }
    }

    fn poll2(&mut self) -> Poll<(), ConnectionError> {
        use frame::Frame::*;

        loop {
            // First, ensure that the `Connection` is able to receive a frame
            try_ready!(self.poll_ready());

            trace!("polling codec");

            let frame = match try!(self.codec.poll()) {
                Async::Ready(frame) => frame,
                Async::NotReady => {
                    // Flush any pending writes
                    let _ = try!(self.poll_complete());
                    return Ok(Async::NotReady);
                }
            };

            debug!("recv; frame={:?}", frame);

            match frame {
                Some(Headers(frame)) => {
                    trace!("recv HEADERS; frame={:?}", frame);
                    try!(self.streams.recv_headers::<P>(frame));
                }
                Some(Data(frame)) => {
                    trace!("recv DATA; frame={:?}", frame);
                    try!(self.streams.recv_data::<P>(frame));
                }
                Some(Reset(frame)) => {
                    trace!("recv RST_STREAM; frame={:?}", frame);
                    try!(self.streams.recv_reset::<P>(frame));
                }
                Some(PushPromise(frame)) => {
                    trace!("recv PUSH_PROMISE; frame={:?}", frame);
                    self.streams.recv_push_promise::<P>(frame)?;
                }
                Some(Settings(frame)) => {
                    trace!("recv SETTINGS; frame={:?}", frame);
                    self.settings.recv_settings(frame);
                }
                Some(GoAway(frame)) => {
                    // TODO: handle the last_stream_id. Also, should this be
                    // handled as an error?
                    let e = ConnectionError::Proto(frame.reason());
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

    fn poll_complete(&mut self) -> Poll<(), ConnectionError> {
        try_ready!(self.poll_ready());

        // Ensure all window updates have been sent.
        try_ready!(self.streams.poll_complete(&mut self.codec));

        Ok(().into())
    }

    fn convert_poll_message(frame: frame::Headers) -> Result<Frame<P::Poll>, ConnectionError> {
        if frame.is_trailers() {
            Ok(Frame::Trailers {
                id: frame.stream_id(),
                headers: frame.into_fields()
            })
        } else {
            Ok(Frame::Headers {
                id: frame.stream_id(),
                end_of_stream: frame.is_end_stream(),
                headers: P::convert_poll_message(frame)?,
            })
        }
    }
}

impl<T, B> Connection<T, client::Peer, B>
    where T: AsyncRead + AsyncWrite,
          B: IntoBuf,
{
    /// Initialize a new HTTP/2.0 stream and send the message.
    pub fn send_request(&mut self, request: Request<()>, end_of_stream: bool)
        -> Result<StreamRef<B::Buf>, ConnectionError>
    {
        self.streams.send_request(request, end_of_stream)
    }
}

impl<T, B> Connection<T, server::Peer, B>
    where T: AsyncRead + AsyncWrite,
          B: IntoBuf,
{
    pub fn next_incoming(&mut self) -> Option<StreamRef<B::Buf>> {
        self.streams.next_incoming()
    }
}
