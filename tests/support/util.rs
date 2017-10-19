use h2;

use super::string::{String, TryFrom};
use bytes::Bytes;
use futures::{Async, Future, Poll};

pub fn byte_str(s: &str) -> String<Bytes> {
    String::try_from(Bytes::from(s)).unwrap()
}

pub fn wait_for_capacity(stream: h2::Stream<Bytes>, target: usize) -> WaitForCapacity {
    WaitForCapacity {
        stream: Some(stream),
        target: target,
    }
}

pub struct WaitForCapacity {
    stream: Option<h2::Stream<Bytes>>,
    target: usize,
}

impl WaitForCapacity {
    fn stream(&mut self) -> &mut h2::Stream<Bytes> {
        self.stream.as_mut().unwrap()
    }
}

impl Future for WaitForCapacity {
    type Item = h2::Stream<Bytes>;
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, ()> {
        let _ = try_ready!(self.stream().poll_capacity().map_err(|_| panic!()));

        let act = self.stream().capacity();

        if act >= self.target {
            return Ok(self.stream.take().unwrap().into());
        }

        Ok(Async::NotReady)
    }
}
