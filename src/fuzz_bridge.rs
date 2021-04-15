use crate::hpack;
use std::io::Cursor;
//use bytes::{Buf, Bytes, BytesMut};
use bytes::{Buf, BufMut, BytesMut};
use http::header::{HeaderName, HeaderValue};

pub mod fuzz_logic {
    use super::*;

    pub fn fuzz_addr_1(data_: &[u8]) {
		let mut decoder_ = hpack::Decoder::new(0);
        let mut buf = BytesMut::new();
        buf.extend(data_);
        decoder_.decode(&mut Cursor::new(&mut buf), |h| {});


        if let Ok(s) = std::str::from_utf8(data_) {
            if let Ok(h) = http::Method::from_bytes(s.as_bytes()) {
                let m_ = hpack::Header::Method(h);
                let mut encoder = hpack::Encoder::new(0, 0);
                let res_ = encode(&mut encoder, vec![m_]);
            }
            /*
            if let Ok(h) = hpack::Header::Method(http::Method::from_bytes(s.as_bytes())) {
                let mut encoder = hpack::Encoder::new(0, 0);
                let res_ = encode(&mut encoder, vec![h]);
            }*/
        }

        //let mut enc_buf = BytesMut::new();
        //hpack::huffman::encode(data_, &mut enc_buf);
	}

    fn encode(e: &mut hpack::Encoder, hdrs: Vec<hpack::Header<Option<HeaderName>>>) -> BytesMut {
        let mut dst = BytesMut::with_capacity(1024);
        e.encode(None, &mut hdrs.into_iter(), &mut (&mut dst).limit(1024));
        dst
    }

    fn method(s: &str) -> hpack::Header<Option<HeaderName>> {
        hpack::Header::Method(http::Method::from_bytes(s.as_bytes()).unwrap())
    }

    fn header(name: &str, val: &str) -> hpack::Header<Option<HeaderName>> {
        let name = HeaderName::from_bytes(name.as_bytes()).unwrap();
        let value = HeaderValue::from_bytes(val.as_bytes()).unwrap();

        hpack::Header::Field {
            name: Some(name),
            value,
        }
    }
}
