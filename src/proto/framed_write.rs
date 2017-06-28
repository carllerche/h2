use {hpack, ConnectionError};
use frame::{self, Frame};
use proto::ReadySink;

use futures::*;
use tokio_io::{AsyncRead, AsyncWrite};
use bytes::{BytesMut, Buf, BufMut};

use std::cmp;
use std::io::{self, Cursor};

#[derive(Debug)]
pub struct FramedWrite<T> {
    /// Upstream `AsyncWrite`
    inner: T,

    /// HPACK encoder
    hpack: hpack::Encoder,

    /// Write buffer
    buf: Cursor<BytesMut>,

    /// Next frame to encode
    next: Option<Next>,

    /// Max frame size, this is specified by the peer
    max_frame_size: usize,
}

#[derive(Debug)]
enum Next {
    Data {
        /// Length of the current frame being written
        frame_len: usize,

        /// Data frame to encode
        data: frame::Data
    },
    Continuation(frame::Continuation),
}

/// Initialze the connection with this amount of write buffer.
const DEFAULT_BUFFER_CAPACITY: usize = 4 * 1_024;

/// Min buffer required to attempt to write a frame
const MIN_BUFFER_CAPACITY: usize = frame::HEADER_LEN + CHAIN_THRESHOLD;

/// Chain payloads bigger than this. The remote will never advertise a max frame
/// size less than this (well, the spec says the max frame size can't be less
/// than 16kb, so not even close).
const CHAIN_THRESHOLD: usize = 256;

impl<T: AsyncWrite> FramedWrite<T> {
    pub fn new(inner: T) -> FramedWrite<T> {
        FramedWrite {
            inner: inner,
            hpack: hpack::Encoder::default(),
            buf: Cursor::new(BytesMut::with_capacity(DEFAULT_BUFFER_CAPACITY)),
            next: None,
            max_frame_size: frame::DEFAULT_MAX_FRAME_SIZE,
        }
    }

    fn has_capacity(&self) -> bool {
        self.next.is_none() && self.buf.get_ref().remaining_mut() >= MIN_BUFFER_CAPACITY
    }

    fn is_empty(&self) -> bool {
        self.next.is_none() && !self.buf.has_remaining()
    }

    fn frame_len(&self, data: &frame::Data) -> usize {
        cmp::min(self.max_frame_size, data.len())
    }
}

impl<T: AsyncWrite> Sink for FramedWrite<T> {
    type SinkItem = Frame;
    type SinkError = ConnectionError;

    fn start_send(&mut self, item: Frame) -> StartSend<Frame, ConnectionError> {
        debug!("start_send; frame={:?}", item);

        if !try!(self.poll_ready()).is_ready() {
            return Ok(AsyncSink::NotReady(item));
        }

        match item {
            Frame::Data(v) => {
                if v.len() >= CHAIN_THRESHOLD {
                    let head = v.head();
                    let len = self.frame_len(&v);

                    // Encode the frame head to the buffer
                    head.encode(len, self.buf.get_mut());

                    // Save the data frame
                    self.next = Some(Next::Data {
                        frame_len: len,
                        data: v,
                    });
                } else {
                    v.encode(self.buf.get_mut());
                }
            }
            Frame::Headers(v) => {
                if let Some(continuation) = v.encode(&mut self.hpack, self.buf.get_mut()) {
                    self.next = Some(Next::Continuation(continuation));
                }
            }
            Frame::PushPromise(v) => {
                debug!("unimplemented PUSH_PROMISE write; frame={:?}", v);
                unimplemented!();
            }
            Frame::Settings(v) => {
                v.encode(self.buf.get_mut());
                trace!("encoded settings; rem={:?}", self.buf.remaining());
            }
            Frame::Ping(v) => {
                v.encode(self.buf.get_mut());
                trace!("encoded ping; rem={:?}", self.buf.remaining());
            }
        }

        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), ConnectionError> {
        trace!("FramedWrite::poll_complete");

        // TODO: implement
        match self.next {
            Some(Next::Data { .. }) => unimplemented!(),
            _ => {}
        }

        // As long as there is data to write, try to write it!
        while !self.is_empty() {
            trace!("writing buffer; next={:?}; rem={:?}", self.next, self.buf.remaining());
            try_ready!(self.inner.write_buf(&mut self.buf));
        }

        trace!("flushing buffer");
        // Flush the upstream
        try_nb!(self.inner.flush());

        // Clear internal buffer
        self.buf.set_position(0);
        self.buf.get_mut().clear();

        Ok(Async::Ready(()))
    }

    fn close(&mut self) -> Poll<(), ConnectionError> {
        try_ready!(self.poll_complete());
        self.inner.shutdown().map_err(Into::into)
    }
}

impl<T: AsyncWrite> ReadySink for FramedWrite<T> {
    fn poll_ready(&mut self) -> Poll<(), Self::SinkError> {
        if !self.has_capacity() {
            // Try flushing
            try!(self.poll_complete());

            if !self.has_capacity() {
                return Ok(Async::NotReady);
            }
        }

        Ok(Async::Ready(()))
    }
}

impl<T: Stream> Stream for FramedWrite<T> {
    type Item = T::Item;
    type Error = T::Error;

    fn poll(&mut self) -> Poll<Option<T::Item>, T::Error> {
        self.inner.poll()
    }
}

impl<T: io::Read> io::Read for FramedWrite<T> {
    fn read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
        self.inner.read(dst)
    }
}

impl<T: AsyncRead> AsyncRead for FramedWrite<T> {
    fn read_buf<B: BufMut>(&mut self, buf: &mut B) -> Poll<usize, io::Error>
        where Self: Sized,
    {
        self.inner.read_buf(buf)
    }

    unsafe fn prepare_uninitialized_buffer(&self, buf: &mut [u8]) -> bool {
        self.inner.prepare_uninitialized_buffer(buf)
    }
}
