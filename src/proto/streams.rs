use {frame, Peer, StreamId, ConnectionError};
use proto::*;
use error::Reason::*;
use error::User::*;

use ordermap::{OrderMap, Entry};

use std::collections::VecDeque;

// TODO: All the VecDeques should become linked lists using the state::Stream
// values.
#[derive(Debug)]
pub struct Streams {
    /// State related to managing the set of streams.
    inner: Inner,

    /// Streams
    streams: StreamMap,
}

type StreamMap = OrderMap<StreamId, state::Stream>;

/// Fields needed to manage state related to managing the set of streams. This
/// is mostly split out to make ownership happy.
///
/// TODO: better name
#[derive(Debug)]
struct Inner {
    /// True when running in context of an H2 server
    is_server: bool,

    /// Maximum number of remote initiated streams
    max_remote_initiated: Option<usize>,

    /// Current number of remote initiated streams
    num_remote_initiated: usize,

    /// Initial window size of remote initiated streams
    init_remote_window_sz: WindowSize,

    /// Maximum number of locally initiated streams
    max_local_initiated: Option<usize>,

    /// Current number of locally initiated streams
    num_local_initiated: usize,

    /// Initial window size of locally initiated streams
    init_local_window_sz: WindowSize,

    /// Connection level flow control governing received data
    recv_flow_control: state::FlowControl,

    /// Connection level flow control governing sent data
    send_flow_control: state::FlowControl,

    /// Holds the list of streams on which local window updates may be sent.
    // XXX It would be cool if this didn't exist.
    pending_recv_window_updates: VecDeque<StreamId>,

    /// Holds the list of streams on which local window updates may be sent.
    // XXX It would be cool if this didn't exist.
    pending_send_window_updates: VecDeque<StreamId>,

    /// When `poll_window_update` is not ready, then the calling task is saved to
    /// be notified later. Access to poll_window_update must not be shared across tasks,
    /// as we only track a single task (and *not* i.e. a task per stream id).
    blocked: Option<task::Task>,

    /// Refused StreamId, this represents a frame that must be sent out.
    refused: Option<StreamId>,
}

#[derive(Debug)]
pub struct Config {
    /// Maximum number of remote initiated streams
    pub max_remote_initiated: Option<usize>,

    /// Initial window size of remote initiated streams
    pub init_remote_window_sz: WindowSize,

    /// Maximum number of locally initiated streams
    pub max_local_initiated: Option<usize>,

    /// Initial window size of locally initiated streams
    pub init_local_window_sz: WindowSize,
}

impl Streams {
    pub fn new<P: Peer>(config: Config) -> Self {
        Streams {
            inner: Inner {
                is_server: P::is_server(),
                max_remote_initiated: config.max_remote_initiated,
                num_remote_initiated: 0,
                init_remote_window_sz: config.init_remote_window_sz,
                max_local_initiated: config.max_local_initiated,
                num_local_initiated: 0,
                init_local_window_sz: config.init_local_window_sz,
                recv_flow_control: state::FlowControl::new(config.init_remote_window_sz),
                send_flow_control: state::FlowControl::new(config.init_local_window_sz),
                pending_recv_window_updates: VecDeque::new(),
                pending_send_window_updates: VecDeque::new(),
                blocked: None,
                refused: None,
            },
            streams: OrderMap::default(),
        }
    }

    pub fn recv_headers(&mut self, frame: frame::Headers)
        -> Result<Option<frame::Headers>, ConnectionError>
    {
        let id = frame.stream_id();

        try!(validate_stream_id(id, ProtocolError));

        let state = match self.streams.entry(id) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => {
                // Trailers cannot open a stream. Trailers are header frames
                // that do not contain pseudo headers. Requests MUST contain a
                // method and responses MUST contain a status. If they do not,t
                // hey are considered to be malformed.
                if frame.is_trailers() {
                    return Err(ProtocolError.into());
                }

                match try!(self.inner.remote_open(id)) {
                    Some(state) => e.insert(state),
                    None => return Ok(None),
                }
            }
        };

        if frame.is_trailers() {
            try!(self.inner.recv_trailers(id, state, frame.is_end_stream()));
        } else {
            try!(self.inner.recv_headers(id, state, frame.is_end_stream()));
        }

        Ok(Some(frame))
    }

    pub fn recv_data(&mut self, frame: &frame::Data)
        -> Result<(), ConnectionError>
    {
        let id = frame.stream_id();

        let sz = frame.payload().len();

        if sz > MAX_WINDOW_SIZE as usize {
            unimplemented!();
        }

        let sz = sz as WindowSize;

        let state = match self.streams.get_mut(&id) {
            Some(state) => state,
            None => return Err(ProtocolError.into()),
        };

        // Ensure there's enough capacity on the connection before acting on the
        // stream.
        self.inner.recv_data(id, state, sz, frame.is_end_stream())
    }

    pub fn recv_reset(&mut self, _frame: &frame::Reset)
        -> Result<(), ConnectionError>
    {
        unimplemented!();
    }

    pub fn recv_window_update(&mut self, frame: frame::WindowUpdate) {
        let id = frame.stream_id();
        let sz = frame.size_increment();

        if id.is_zero() {
            self.inner.expand_connection_window(sz);
        } else {
            // The remote may send window updates for streams that the local now
            // considers closed. It's ok...
            if let Some(state) = self.streams.get_mut(&id) {
                self.inner.expand_stream_window(sz, state);
            }
        }

        if let Some(task) = self.inner.blocked.take() {
            task.notify();
        }
    }

    pub fn recv_push_promise(&mut self, _frame: frame::PushPromise)
        -> Result<(), ConnectionError>
    {
        unimplemented!();
    }

    pub fn send_headers(&mut self, frame: &frame::Headers)
        -> Result<(), ConnectionError>
    {
        let id = frame.stream_id();

        trace!("send_headers; id={:?}", id);

        try!(validate_stream_id(id, InvalidStreamId));

        let state = match self.streams.entry(id) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => {
                // Trailers cannot open a stream. Trailers are header frames
                // that do not contain pseudo headers. Requests MUST contain a
                // method and responses MUST contain a status. If they do not,t
                // hey are considered to be malformed.
                if frame.is_trailers() {
                    // TODO: Should this be a different error?
                    return Err(UnexpectedFrameType.into());
                }

                let state = try!(self.inner.local_open(id));
                e.insert(state)
            }
        };

        if frame.is_trailers() {
            try!(self.inner.send_trailers(id, state, frame.is_end_stream()));
        } else {
            try!(self.inner.send_headers(id, state, frame.is_end_stream()));
        }

        Ok(())
    }

    pub fn send_data<B: Buf>(&mut self, frame: &frame::Data<B>)
        -> Result<(), ConnectionError>
    {
        let id = frame.stream_id();
        let sz = frame.payload().remaining();

        if sz > MAX_WINDOW_SIZE as usize {
            // TODO: handle overflow
            unimplemented!();
        }

        let sz = sz as WindowSize;

        let state = match self.streams.get_mut(&id) {
            Some(state) => state,
            None => return Err(UnexpectedFrameType.into()),
        };

        // Ensure there's enough capacity on the connection before acting on the
        // stream.
        self.inner.send_data(id, state, sz, frame.is_end_stream())
    }

    pub fn poll_window_update(&mut self)
        -> Poll<WindowUpdate, ConnectionError>
    {
        self.inner.poll_window_update(&mut self.streams)
    }

    pub fn expand_window(&mut self, id: StreamId, sz: WindowSize) {
        if id.is_zero() {
            self.inner.expand_connection_window(sz);
        } else {
            if let Some(state) = self.streams.get_mut(&id) {
                self.inner.expand_stream_window(sz, state);
            }
        }
    }

    pub fn send_pending_refusal<T, B>(&mut self, dst: &mut Codec<T, B>)
        -> Poll<(), ConnectionError>
        where T: AsyncWrite,
              B: Buf,
    {
        self.inner.send_pending_refusal(dst)
    }

    pub fn send_pending_window_updates<T, B>(&mut self, dst: &mut Codec<T, B>)
        -> Poll<(), ConnectionError>
        where T: AsyncWrite,
              B: Buf,
    {
        try_ready!(self.inner.send_connection_window_update(dst));
        try_ready!(self.inner.send_stream_window_update(&mut self.streams, dst));

        Ok(().into())
    }
}

impl Inner {
    /// Update state reflecting a new, remotely opened stream
    ///
    /// Returns the stream state if successful. `None` if refused
    fn remote_open(&mut self, id: StreamId) -> Result<Option<state::Stream>, ConnectionError> {
        assert!(self.refused.is_none());

        if !self.can_remote_open(id) {
            return Err(ProtocolError.into());
        }

        if let Some(max) = self.max_remote_initiated {
            if max <= self.num_remote_initiated {
                self.refused = Some(id);
                return Ok(None);
            }
        }

        // Increment the number of remote initiated streams
        self.num_remote_initiated += 1;

        Ok(Some(state::Stream::default()))
    }

    /// Transition the stream state based on receiving headers
    fn recv_headers(&mut self, id: StreamId, state: &mut state::Stream, eos: bool)
        -> Result<(), ConnectionError>
    {
        try!(state.recv_open(self.init_remote_window_sz, eos));

        if state.is_closed() {
            self.stream_closed(id);
        }

        Ok(())
    }

    fn recv_trailers(&mut self, _id: StreamId, _state: &mut state::Stream, _eos: bool)
        -> Result<(), ConnectionError>
    {
        unimplemented!();
    }

    fn recv_data(&mut self,
                 id: StreamId,
                 state: &mut state::Stream,
                 sz: WindowSize,
                 eos: bool)
        -> Result<(), ConnectionError>
    {
        match state.recv_flow_control() {
            Some(flow) => {
                // Ensure there's enough capacity on the connection before
                // acting on the stream.
                try!(self.recv_flow_control.ensure_window(sz, FlowControlError));

                // Claim the window on the stream
                try!(flow.claim_window(sz, FlowControlError));

                // Claim the window on the connection.
                self.recv_flow_control.claim_window(sz, FlowControlError)
                    .expect("local connection flow control error");
            }
            None => return Err(ProtocolError.into()),
        }

        if eos {
            try!(state.recv_close());

            if state.is_closed() {
                self.stream_closed(id)
            }
        }

        Ok(())
    }

    /// Update state reflecting a new, locally opened stream
    ///
    /// Returns the stream state if successful. `None` if refused
    fn local_open(&mut self, id: StreamId) -> Result<state::Stream, ConnectionError> {
        if !self.can_local_open(id) {
            return Err(UnexpectedFrameType.into());
        }

        if let Some(max) = self.max_local_initiated {
            if max <= self.num_local_initiated {
                return Err(Rejected.into());
            }
        }

        // Increment the number of locally initiated streams
        self.num_local_initiated += 1;

        Ok(state::Stream::default())
    }

    fn send_headers(&mut self, id: StreamId, state: &mut state::Stream, eos: bool)
        -> Result<(), ConnectionError>
    {
        try!(state.send_open(self.init_local_window_sz, eos));

        if state.is_closed() {
            self.stream_closed(id);
        }

        Ok(())
    }

    fn send_trailers(&mut self, _id: StreamId, _state: &mut state::Stream, _eos: bool)
        -> Result<(), ConnectionError>
    {
        unimplemented!();
    }

    fn send_data(&mut self,
                 id: StreamId,
                 state: &mut state::Stream,
                 sz: WindowSize,
                 eos: bool)
        -> Result<(), ConnectionError>
    {
        match state.send_flow_control() {
            Some(flow) => {
                try!(self.send_flow_control.ensure_window(sz, FlowControlViolation));

                // Claim the window on the stream
                try!(flow.claim_window(sz, FlowControlViolation));

                // Claim the window on the connection
                self.send_flow_control.claim_window(sz, FlowControlViolation)
                    .expect("local connection flow control error");
            }
            None => return Err(UnexpectedFrameType.into()),
        }

        if eos {
            try!(state.send_close());

            if state.is_closed() {
                self.stream_closed(id)
            }
        }

        Ok(())
    }

    fn stream_closed(&mut self, id: StreamId) {
        if self.is_local_init(id) {
            self.num_local_initiated -= 1;
        } else {
            self.num_remote_initiated -= 1;
        }
    }

    fn is_local_init(&self, id: StreamId) -> bool {
        assert!(!id.is_zero());
        self.is_server == id.is_server_initiated()
    }

    /// Returns true if the remote peer can initiate a stream with the given ID.
    fn can_remote_open(&self, id: StreamId) -> bool {
        if self.is_server {
            // Remote is a client and cannot open streams
            return false;
        }

        // Ensure that the ID is a valid server initiated ID
        id.is_server_initiated()
    }

    /// Returns true if the local actor can initiate a stream with the given ID.
    fn can_local_open(&self, id: StreamId) -> bool {
        if !self.is_server {
            // Clients cannot open streams
            return false;
        }

        id.is_server_initiated()
    }

    /// Get pending window updates
    fn poll_window_update(&mut self, streams: &mut StreamMap)
        -> Poll<WindowUpdate, ConnectionError>
    {
        // This biases connection window updates, which probably makes sense.
        //
        // TODO: We probably don't want to expose connection level updates
        if let Some(incr) = self.send_flow_control.apply_window_update() {
            return Ok(Async::Ready(WindowUpdate::new(StreamId::zero(), incr)));
        }

        // TODO this should probably account for stream priority?
        let update = self.pending_recv_window_updates.pop_front()
            .and_then(|id| {
                streams.get_mut(&id)
                    .and_then(|state| state.send_flow_control())
                    .and_then(|flow| flow.apply_window_update())
                    .map(|incr| WindowUpdate::new(id, incr))
            });

        if let Some(update) = update {
            return Ok(Async::Ready(update));
        }

        // Update the task.
        //
        // TODO: Extract this "gate" logic
        self.blocked = Some(task::current());

        return Ok(Async::NotReady);
    }

    fn expand_connection_window(&mut self, sz: WindowSize) {
        self.send_flow_control.expand_window(sz);
    }

    fn expand_stream_window(&mut self, sz: WindowSize, state: &mut state::Stream) {
        // It's fine for this to be None and silently ignored.
        if let Some(flow) = state.send_flow_control() {
            flow.expand_window(sz);
        }
    }

    /// Send any pending refusals.
    fn send_pending_refusal<T, B>(&mut self, dst: &mut Codec<T, B>) -> Poll<(), ConnectionError>
        where T: AsyncWrite,
              B: Buf,
    {
        if let Some(stream_id) = self.refused.take() {
            let frame = frame::Reset::new(stream_id, RefusedStream);

            match dst.start_send(frame.into())? {
                AsyncSink::Ready => {
                    self.reset(stream_id, RefusedStream);
                    return Ok(Async::Ready(()));
                }
                AsyncSink::NotReady(_) => {
                    self.refused = Some(stream_id);
                    return Ok(Async::NotReady);
                }
            }
        }

        Ok(Async::Ready(()))
    }

    /// Send connection level window update
    fn send_connection_window_update<T, B>(&mut self, dst: &mut Codec<T, B>)
        -> Poll<(), ConnectionError>
        where T: AsyncWrite,
              B: Buf,
    {
        if let Some(incr) = self.recv_flow_control.peek_window_update() {
            let frame = frame::WindowUpdate::new(StreamId::zero(), incr);

            if dst.start_send(frame.into())?.is_ready() {
                assert_eq!(Some(incr), self.recv_flow_control.apply_window_update());
            } else {
                return Ok(Async::NotReady);
            }
        }

        Ok(().into())
    }

    /// Send stream level window update
    fn send_stream_window_update<T, B>(&mut self,
                                       streams: &mut StreamMap,
                                       dst: &mut Codec<T, B>)
        -> Poll<(), ConnectionError>
        where T: AsyncWrite,
              B: Buf,
    {
        while let Some(id) = self.pending_send_window_updates.pop_front() {
            let flow = streams.get_mut(&id)
                .and_then(|state| state.recv_flow_control());


            if let Some(flow) = flow {
                if let Some(incr) = flow.peek_window_update() {
                    let frame = frame::WindowUpdate::new(id, incr);

                    if dst.start_send(frame.into())?.is_ready() {
                        assert_eq!(Some(incr), flow.apply_window_update());
                    } else {
                        self.pending_send_window_updates.push_front(id);
                        return Ok(Async::NotReady);
                    }
                }
            }
        }

        Ok(().into())
    }

    fn reset(&mut self, _stream_id: StreamId, _reason: Reason) {
        unimplemented!();
    }
}

/// Ensures non-zero stream ID
fn validate_stream_id<E: Into<ConnectionError>>(id: StreamId, err: E)
    -> Result<(), ConnectionError>
{
    if id.is_zero() {
        Err(err.into())
    } else {
        Ok(())
    }
}
