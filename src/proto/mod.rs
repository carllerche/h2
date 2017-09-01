mod connection;
mod error;
mod peer;
mod ping_pong;
mod settings;
mod streams;

pub(crate) use self::connection::Connection;
pub(crate) use self::error::Error;
pub(crate) use self::peer::Peer;
pub(crate) use self::streams::{Streams, StreamRef};

use codec::{Codec, FramedRead, FramedWrite};

use self::ping_pong::PingPong;
use self::settings::Settings;
use self::streams::Prioritized;

use frame::{self, Frame};

use futures::{task, Poll, Async, AsyncSink};
use futures::task::Task;

use bytes::{Buf, IntoBuf};

use tokio_io::{AsyncRead, AsyncWrite};
use tokio_io::codec::length_delimited;

pub type PingPayload = [u8; 8];

pub type WindowSize = u32;

// Constants
pub const DEFAULT_INITIAL_WINDOW_SIZE: WindowSize = 65_535;
pub const MAX_WINDOW_SIZE: WindowSize = (1 << 31) - 1;

/// Create a transport prepared to handle the server handshake.
///
/// When the server is performing the handshake, it is able to only send
/// `Settings` frames and is expected to receive the client preface as a byte
/// stream. To represent this, `Settings<FramedWrite<T>>` is returned.
pub(crate) fn framed_write<T, B>(io: T) -> FramedWrite<T, B>
    where T: AsyncRead + AsyncWrite,
          B: Buf,
{
    FramedWrite::new(io)
}

/// Create a full H2 transport from the server handshaker
pub(crate) fn from_framed_write<T, P, B>(framed_write: FramedWrite<T, Prioritized<B::Buf>>)
    -> Connection<T, P, B>
    where T: AsyncRead + AsyncWrite,
          P: Peer,
          B: IntoBuf,
{
    // Delimit the frames.
    let framed = length_delimited::Builder::new()
        .big_endian()
        .length_field_length(3)
        .length_adjustment(9)
        .num_skip(0) // Don't skip the header
        // TODO: make this configurable and allow it to be changed during
        // runtime.
        .max_frame_length(frame::DEFAULT_MAX_FRAME_SIZE as usize)
        .new_read(framed_write);

    let codec = Codec::from_framed(FramedRead::new(framed));

    Connection::new(codec)
}
