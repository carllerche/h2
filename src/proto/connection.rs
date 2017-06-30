use {frame, Frame, ConnectionError, Peer, StreamId};
use client::Client;
use proto::{self, ReadySink, State};

use tokio_io::{AsyncRead, AsyncWrite};

use http::{request};

use futures::*;

use ordermap::OrderMap;
use fnv::FnvHasher;

use std::marker::PhantomData;
use std::hash::BuildHasherDefault;

/// An H2 connection
#[derive(Debug)]
pub struct Connection<T, P> {
    inner: proto::Inner<T>,
    streams: StreamMap<State>,
    peer: PhantomData<P>,
}

type StreamMap<T> = OrderMap<StreamId, T, BuildHasherDefault<FnvHasher>>;

pub fn new<T, P>(transport: proto::Inner<T>) -> Connection<T, P>
    where T: AsyncRead + AsyncWrite,
          P: Peer,
{
    Connection {
        inner: transport,
        streams: StreamMap::default(),
        peer: PhantomData,
    }
}

impl<T> Connection<T, Client>
    where T: AsyncRead + AsyncWrite,
{
    pub fn send_request(self,
                        id: StreamId, // TODO: Generate one internally?
                        request: request::Head,
                        end_of_stream: bool)
        -> sink::Send<Self>
    {
        self.send(Frame::Headers {
            id: id,
            headers: request,
            end_of_stream: end_of_stream,
        })
    }
}

impl<T, P> Stream for Connection<T, P>
    where T: AsyncRead + AsyncWrite,
          P: Peer,
{
    type Item = Frame<P::Poll>;
    type Error = ConnectionError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, ConnectionError> {
        use frame::Frame::*;

        let frame = match try!(self.inner.poll()) {
            Async::Ready(f) => f,
            Async::NotReady => {
                // Because receiving new frames may depend on ensuring that the
                // write buffer is clear, `poll_complete` is called here.
                let _ = try!(self.poll_complete());
                return Ok(Async::NotReady);
            }
        };

        let frame = match frame {
            Some(Headers(v)) => {
                // TODO: Update stream state
                let stream_id = v.stream_id();
                let end_of_stream = v.is_end_stream();

                Frame::Headers {
                    id: stream_id,
                    headers: P::convert_poll_message(v),
                    end_of_stream: end_of_stream,
                }
            }
            Some(Data(v)) => {
                // TODO: Validate frame

                let stream_id = v.stream_id();
                let end_of_stream = v.is_end_stream();

                Frame::Body {
                    id: stream_id,
                    chunk: v.into_payload(),
                    end_of_stream: end_of_stream,
                }
            }
            Some(frame) => panic!("unexpected frame; frame={:?}", frame),
            None => return Ok(Async::Ready(None)),
        };

        Ok(Async::Ready(Some(frame)))
    }
}

impl<T, P> Sink for Connection<T, P>
    where T: AsyncRead + AsyncWrite,
          P: Peer,
{
    type SinkItem = Frame<P::Send>;
    type SinkError = ConnectionError;

    fn start_send(&mut self, item: Self::SinkItem)
        -> StartSend<Self::SinkItem, Self::SinkError>
    {
        // First ensure that the upstream can process a new item
        if !try!(self.poll_ready()).is_ready() {
            return Ok(AsyncSink::NotReady(item));
        }

        match item {
            Frame::Headers { id, headers, end_of_stream } => {
                // Ensure ID is valid
                try!(P::check_initiating_id(id));

                // TODO: Ensure available capacity for a new stream
                // This won't be as simple as self.streams.len() as closed
                // connections should not be factored.

                // Transition the stream state, creating a new entry if needed
                //
                // TODO: Response can send multiple headers frames before body
                // (1xx responses).
                try!(self.streams.entry(id)
                     .or_insert(State::default())
                     .send_headers());

                let frame = P::convert_send_message(id, headers, end_of_stream);

                // We already ensured that the upstream can handle the frame, so
                // panic if it gets rejected.
                let res = try!(self.inner.start_send(frame::Frame::Headers(frame)));

                // This is a one-way conversion. By checking `poll_ready` first,
                // it's already been determined that the inner `Sink` can accept
                // the item. If the item is rejected, then there is a bug.
                assert!(res.is_ready());

                Ok(AsyncSink::Ready)
            }
            /*
            Frame::Trailers { id, headers } => {
                unimplemented!();
            }
            Frame::Body { id, chunk, end_of_stream } => {
                unimplemented!();
            }
            Frame::PushPromise { id, promise } => {
                unimplemented!();
            }
            Frame::Error { id, error } => {
                unimplemented!();
            }
            */
            _ => unimplemented!(),
        }
    }

    fn poll_complete(&mut self) -> Poll<(), ConnectionError> {
        self.inner.poll_complete()
    }
}

impl<T, P> ReadySink for Connection<T, P>
    where T: AsyncRead + AsyncWrite,
          P: Peer,
{
    fn poll_ready(&mut self) -> Poll<(), Self::SinkError> {
        self.inner.poll_ready()
    }
}
