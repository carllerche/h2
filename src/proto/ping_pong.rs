use ConnectionError;
use frame::{Frame, Ping};
use futures::*;
use proto::ReadySink;
use std::collections::VecDeque;

/// Acknowledges ping requests from the remote.
#[derive(Debug)]
pub struct PingPong<T> {
    inner: T,
    is_closed: bool,
    sending_pongs: VecDeque<Frame>,
}

impl<T> PingPong<T>
    where T: Stream<Item = Frame, Error = ConnectionError>,
          T: Sink<SinkItem = Frame, SinkError = ConnectionError>,
{
    pub fn new(inner: T) -> PingPong<T> {
        PingPong {
            inner,
            is_closed: false,
            sending_pongs: VecDeque::new(),
        }
    }

    fn send_pongs(&mut self) -> Poll<(), ConnectionError> {
        if self.sending_pongs.is_empty() {
            return Ok(Async::Ready(()));
        }

        while let Some(pong) = self.sending_pongs.pop_front() {
            if let AsyncSink::NotReady(pong) = self.inner.start_send(pong)? {
                // If the pong can't be sent, save it..
                self.sending_pongs.push_front(pong);
                return; // Ok(Async::NotReady);
            }
        }

        //self.inner.poll_complete()
    }
}

/// > Receivers of a PING frame that does not include an ACK flag MUST send
/// > a PING frame with the ACK flag set in response, with an identical
/// > payload. PING responses SHOULD be given higher priority than any
/// > other frame.
impl<T> Stream for PingPong<T>
    where T: Stream<Item = Frame, Error = ConnectionError>,
          T: Sink<SinkItem = Frame, SinkError = ConnectionError>,
{
    type Item = Frame;
    type Error = ConnectionError;

    /// Reads the next frame from the underlying socket, eliding ping requests.
    ///
    /// If a PING is received without the ACK flag, the frame is sent to the remote with
    /// its ACK flag set.
    fn poll(&mut self) -> Poll<Option<Frame>, ConnectionError> {
        if self.is_closed {
            return Ok(Async::Ready(None));
        }

        loop {
            match self.inner.poll()? {
                Async::Ready(Some(Frame::Ping(ping))) => {
                    if ping.is_ack() {
                        // If we received an ACK, pass it on (nothing to be done here).
                        return Ok(Async::Ready(Some(ping.into())));
                    }

                    // Save a pong to be sent when there is nothing more to be returned
                    // from the stream or when frames are sent to the sink..
                    let pong = Ping::pong(ping.into_payload());
                    self.sending_pongs.push_back(pong.into());

                    // There's nothing to return yet. Poll the underlying stream again to
                    // determine how to proceed.
                    continue;
                }

                // Everything other than ping gets passed through.
                f @ Async::Ready(Some(_)) => {
                    return Ok(f);
                }

                // If poll won't necessarily be called again, try to send pending pong
                // frames.
                f @ Async::Ready(None) => {
                    self.is_closed = true;
                    self.send_pongs()?;
                    return Ok(f);
                }
                f @ Async::NotReady => {
                    self.send_pongs()?;
                    return Ok(f);
                }
            }
        }
    }
}

impl<T> Sink for PingPong<T>
    where T: Stream<Item = Frame, Error = ConnectionError>,
          T: Sink<SinkItem = Frame, SinkError = ConnectionError>,
{
    type SinkItem = Frame;
    type SinkError = ConnectionError;

    fn start_send(&mut self, item: Frame) -> StartSend<Frame, ConnectionError> {
        // Pings _SHOULD_ have priority over other messages, so attempt to send pending
        // ping frames before attempting to send the 
        if self.send_pongs()?.is_not_ready() {
            return Ok(AsyncSink::NotReady(item));
        }

        self.inner.start_send(item)
    }

    /// Polls the underlying sink and tries to flush pending pong frames.
    fn poll_complete(&mut self) -> Poll<(), ConnectionError> {
        // Try to flush the underlying sink.
        let poll = self.inner.poll_complete()?;
        if self.sending_pongs.is_empty() {
            return Ok(poll);
        }

        // Then, try to flush pending pongs. Even if poll is not ready, we may be able to
        // start sending pongs.
        self.send_pongs()
    }
}

impl<T> ReadySink for PingPong<T>
    where T: Stream<Item = Frame, Error = ConnectionError>,
          T: Sink<SinkItem = Frame, SinkError = ConnectionError>,
          T: ReadySink,
{
    fn poll_ready(&mut self) -> Poll<(), ConnectionError> {
        if !self.sending_pongs.is_empty() {
            return Ok(Async::NotReady);
        }
        self.inner.poll_ready()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use bytes::Bytes;
    use std::cell::RefCell;
    use std::rc::Rc;

    #[test]
    fn responds_to_ping_with_pong() {
        let trans = Transport::default();
        let mut ping_pong = PingPong::new(trans.clone());

        {
            let mut trans = trans.0.borrow_mut();
            let ping = Ping::ping(*b"buoyant_");
            trans.from_socket.push_back(ping.into());
        }

        match ping_pong.poll() {
            Ok(Async::NotReady) => {} // cool
            rsp => panic!("unexpected poll result: {:?}", rsp),
        }

        {
            let mut trans = trans.0.borrow_mut();
            assert_eq!(trans.to_socket.len(), 1);
            match trans.to_socket.pop_front().unwrap() {
                Frame::Ping(pong) => {
                    assert!(pong.is_ack());
                    assert_eq!(&pong.into_payload(), b"buoyant_");
                }
                f => panic!("unexpected frame: {:?}", f),
            }
        }
    }

    #[test]
    fn responds_to_ping_even_when_blocked() {
        let trans = Transport::default();
        let mut ping_pong = PingPong::new(trans.clone());

        // Configure the transport so that writes can't proceed.
        {
            let mut trans = trans.0.borrow_mut();
            trans.start_send_blocked = true;
        }

        // The transport receives a ping but can't send it immediately.
        {
            let mut trans = trans.0.borrow_mut();
            let ping = Ping::ping(*b"buoyant?");
            trans.from_socket.push_back(ping.into());
        }
        assert!(ping_pong.poll().unwrap().is_not_ready());

        // The transport receives another ping but can't send it immediately.
        {
            let mut trans = trans.0.borrow_mut();
            let ping = Ping::ping(*b"buoyant!");
            trans.from_socket.push_back(ping.into());
        }
        assert!(ping_pong.poll().unwrap().is_not_ready());

        // At this point, ping_pong is holding two pongs that it cannot send.
        {
            let mut trans = trans.0.borrow_mut();
            assert!(trans.to_socket.is_empty());

            trans.start_send_blocked = false;
        }

        // Now that start_send_blocked is disabled, the next poll will successfully send
        // the pongs on the transport.
        assert!(ping_pong.poll().unwrap().is_not_ready());
        {
            let mut trans = trans.0.borrow_mut();
            assert_eq!(trans.to_socket.len(), 2);
            match trans.to_socket.pop_front().unwrap() {
                Frame::Ping(pong) => {
                    assert!(pong.is_ack());
                    assert_eq!(&pong.into_payload(), b"buoyant?");
                }
                f => panic!("unexpected frame: {:?}", f),
            }
            match trans.to_socket.pop_front().unwrap() {
                Frame::Ping(pong) => {
                    assert!(pong.is_ack());
                    assert_eq!(&pong.into_payload(), b"buoyant!");
                }
                f => panic!("unexpected frame: {:?}", f),
            }
        }
    }

    #[test]
    fn pong_passes_through() {
        let trans = Transport::default();
        let mut ping_pong = PingPong::new(trans.clone());

        {
            let mut trans = trans.0.borrow_mut();
            let pong = Ping::pong(*b"buoyant!");
            trans.from_socket.push_back(pong.into());
        }

        match ping_pong.poll().unwrap() {
            Async::Ready(Some(Frame::Ping(pong))) => {
                assert!(pong.is_ack());
                assert_eq!(&pong.into_payload(), b"buoyant!");
            }
            f => panic!("unexpected frame: {:?}", f),
        }

        {
            let trans = trans.0.borrow();
            assert_eq!(trans.to_socket.len(), 0);
        }
    }

    /// A stubbed transport for tests.a
    ///
    /// We probably want/have something generic for this?
    #[derive(Clone, Default)]
    struct Transport(Rc<RefCell<Inner>>);

    #[derive(Default)]
    struct Inner {
        from_socket: VecDeque<Frame>,
        to_socket: VecDeque<Frame>,
        read_blocked: bool,
        start_send_blocked: bool,
        closing: bool,
    }

    impl Stream for Transport {
        type Item = Frame;
        type Error = ConnectionError;

        fn poll(&mut self) -> Poll<Option<Frame>, ConnectionError> {
            let mut trans = self.0.borrow_mut();
            if trans.read_blocked || (!trans.closing && trans.from_socket.is_empty()) {
                Ok(Async::NotReady)
            } else {
                Ok(trans.from_socket.pop_front().into())
            }
        }
    }

    impl Sink for Transport {
        type SinkItem = Frame;
        type SinkError = ConnectionError;

        fn start_send(&mut self, item: Frame) -> StartSend<Frame, ConnectionError> {
            let mut trans = self.0.borrow_mut();
            if trans.closing || trans.start_send_blocked {
                Ok(AsyncSink::NotReady(item))
            } else {
                trans.to_socket.push_back(item);
                Ok(AsyncSink::Ready)
            }
        }

        fn poll_complete(&mut self) -> Poll<(), ConnectionError> {
            let trans = self.0.borrow();
            if !trans.to_socket.is_empty() {
                Ok(Async::NotReady)
            } else {
                Ok(Async::Ready(()))
            }
        }

        fn close(&mut self) -> Poll<(), ConnectionError> {
            {
                let mut trans = self.0.borrow_mut();
                trans.closing = true;
            }
            self.poll_complete()
        }
    }
}
