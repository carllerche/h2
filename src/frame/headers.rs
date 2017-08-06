use {hpack, BodyType, HeaderMap};
use super::StreamId;
use frame::{self, Frame, Head, Kind, Error};

use http::{self, request, response, version, uri, Method, StatusCode, Uri};
use http::{Request, Response};
use http::header::{self, HeaderName, HeaderValue};

use bytes::{BytesMut, Bytes};
use byteorder::{BigEndian, ByteOrder};
use string::String;

use std::io::Cursor;

/// Header frame
///
/// This could be either a request or a response.
#[derive(Debug)]
pub struct Headers {
    /// The ID of the stream with which this frame is associated.
    stream_id: StreamId,

    /// The stream dependency information, if any.
    stream_dep: Option<StreamDependency>,

    /// The decoded header fields
    fields: HeaderMap,

    /// Pseudo headers, these are broken out as they must be sent as part of the
    /// headers frame.
    pseudo: Pseudo,

    /// The associated flags
    flags: HeadersFlag,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct HeadersFlag(u8);

#[derive(Debug)]
pub struct PushPromise {
    /// The ID of the stream with which this frame is associated.
    stream_id: StreamId,

    /// The ID of the stream being reserved by this PushPromise.
    promised_id: StreamId,

    /// The associated flags
    flags: HeadersFlag,
}

impl PushPromise {
}

#[derive(Debug)]
pub struct Continuation {
    /// Stream ID of continuation frame
    stream_id: StreamId,

    /// Argument to pass to the HPACK encoder to resume encoding
    hpack: hpack::EncodeState,

    /// remaining headers to encode
    headers: Iter,
}

#[derive(Debug)]
pub struct StreamDependency {
    /// The ID of the stream dependency target
    stream_id: StreamId,

    /// The weight for the stream. The value exposed (and set) here is always in
    /// the range [0, 255], instead of [1, 256] (as defined in section 5.3.2.)
    /// so that the value fits into a `u8`.
    weight: u8,

    /// True if the stream dependency is exclusive.
    is_exclusive: bool,
}

#[derive(Debug, Default)]
pub struct Pseudo {
    // Request
    method: Option<Method>,
    scheme: Option<String<Bytes>>,
    authority: Option<String<Bytes>>,
    path: Option<String<Bytes>>,

    // Response
    status: Option<StatusCode>,
}

#[derive(Debug)]
pub struct Iter {
    /// Pseudo headers
    pseudo: Option<Pseudo>,

    /// Header fields
    fields: header::IntoIter<HeaderValue>,
}

const END_STREAM: u8 = 0x1;
const END_HEADERS: u8 = 0x4;
const PADDED: u8 = 0x8;
const PRIORITY: u8 = 0x20;
const ALL: u8 = END_STREAM
              | END_HEADERS
              | PADDED
              | PRIORITY;

// ===== impl Headers =====

impl Headers {
    pub fn new(stream_id: StreamId, pseudo: Pseudo, fields: HeaderMap) -> Self {
        Headers {
            stream_id: stream_id,
            stream_dep: None,
            fields: fields,
            pseudo: pseudo,
            flags: HeadersFlag::default(),
        }
    }

    pub fn load(head: Head, src: &mut Cursor<Bytes>, decoder: &mut hpack::Decoder)
        -> Result<Self, Error>
    {
        let flags = HeadersFlag(head.flag());

        assert!(!flags.is_priority(), "unimplemented stream priority");

        let mut pseudo = Pseudo::default();
        let mut fields = HeaderMap::new();
        let mut err = false;

        macro_rules! set_pseudo {
            ($field:ident, $val:expr) => {{
                if pseudo.$field.is_some() {
                    err = true;
                } else {
                    pseudo.$field = Some($val);
                }
            }}
        }

        // At this point, we're going to assume that the hpack encoded headers
        // contain the entire payload. Later, we need to check for stream
        // priority.
        //
        // TODO: Provide a way to abort decoding if an error is hit.
        try!(decoder.decode(src, |header| {
            use hpack::Header::*;

            match header {
                Field { name, value } => {
                    fields.append(name, value);
                }
                Authority(v) => set_pseudo!(authority, v),
                Method(v) => set_pseudo!(method, v),
                Scheme(v) => set_pseudo!(scheme, v),
                Path(v) => set_pseudo!(path, v),
                Status(v) => set_pseudo!(status, v),
            }
        }));

        if err {
            return Err(hpack::DecoderError::RepeatedPseudo.into());
        }

        Ok(Headers {
            stream_id: head.stream_id(),
            stream_dep: None,
            fields: fields,
            pseudo: pseudo,
            flags: flags,
        })
    }

    /// Returns `true` if the frame represents trailers
    ///
    /// Trailers are header frames that contain no pseudo headers.
    pub fn is_trailers(&self) -> bool {
        self.pseudo.method.is_none() &&
            self.pseudo.status.is_none()
    }

    pub fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    pub fn is_end_headers(&self) -> bool {
        self.flags.is_end_headers()
    }

    pub fn is_end_stream(&self) -> bool {
        self.flags.is_end_stream()
    }

    pub fn set_end_stream(&mut self) {
        self.flags.set_end_stream()
    }

    pub fn into_response(self) -> http::Result<Response<BodyType>> {
        let mut b = Response::builder();

        if let Some(status) = self.pseudo.status {
            b.status(status);
        }

        let body = if self.is_end_stream() {
            BodyType::Empty
        } else {
            BodyType::Stream
        };

        let mut response = try!(b.body(body));
        *response.headers_mut() = self.fields;

        Ok(response)
    }

    pub fn into_request(self) -> http::Result<Request<()>> {
        let mut b = Request::builder();

        // TODO: should we distinguish between HTTP_2 and HTTP_2C?
        // carllerche/http#42
        b.version(version::HTTP_2);

        if let Some(method) = self.pseudo.method {
            b.method(method);
        }

        // Convert the URI
        let mut parts = uri::Parts::default();

        if let Some(scheme) = self.pseudo.scheme {
            // TODO: Don't unwrap
            parts.scheme = Some(uri::Scheme::try_from_shared(scheme.into_inner()).unwrap());
        }

        if let Some(authority) = self.pseudo.authority {
            // TODO: Don't unwrap
            parts.authority = Some(uri::Authority::try_from_shared(authority.into_inner()).unwrap());
        }

        if let Some(path) = self.pseudo.path {
            // TODO: Don't unwrap
            parts.origin_form = Some(uri::OriginForm::try_from_shared(path.into_inner()).unwrap());
        }

        b.uri(parts);

        let mut request = try!(b.body(()));
        *request.headers_mut() = self.fields;

        Ok(request)
    }

    pub fn into_fields(self) -> HeaderMap {
        self.fields
    }

    pub fn encode(self, encoder: &mut hpack::Encoder, dst: &mut BytesMut)
        -> Option<Continuation>
    {
        let head = self.head();
        let pos = dst.len();

        // At this point, we don't know how big the h2 frame will be.
        // So, we write the head with length 0, then write the body, and
        // finally write the length once we know the size.
        head.encode(0, dst);

        // Encode the frame
        let mut headers = Iter {
            pseudo: Some(self.pseudo),
            fields: self.fields.into_iter(),
        };

        let ret = match encoder.encode(None, &mut headers, dst) {
            hpack::Encode::Full => None,
            hpack::Encode::Partial(state) => {
                Some(Continuation {
                    stream_id: self.stream_id,
                    hpack: state,
                    headers: headers,
                })
            }
        };

        // Compute the frame length
        let len = (dst.len() - pos) - frame::HEADER_LEN;

        // Write the frame length
        BigEndian::write_uint(&mut dst[pos..pos+3], len as u64, 3);

        ret
    }

    fn head(&self) -> Head {
        Head::new(Kind::Headers, self.flags.into(), self.stream_id)
    }
}

impl<T> From<Headers> for Frame<T> {
    fn from(src: Headers) -> Self {
        Frame::Headers(src)
    }
}

// ===== impl Pseudo =====

impl Pseudo {
    pub fn request(method: Method, uri: Uri) -> Self {
        let parts = uri::Parts::from(uri);

        fn to_string(src: Bytes) -> String<Bytes> {
            unsafe { String::from_utf8_unchecked(src) }
        }

        let path = parts.origin_form
            .map(|v| v.into())
            .unwrap_or_else(|| Bytes::from_static(b"/"));

        let mut pseudo = Pseudo {
            method: Some(method),
            scheme: None,
            authority: None,
            path: Some(to_string(path)),
            status: None,
        };

        // If the URI includes a scheme component, add it to the pseudo headers
        //
        // TODO: Scheme must be set...
        if let Some(scheme) = parts.scheme {
            pseudo.set_scheme(to_string(scheme.into()));
        }

        // If the URI includes an authority component, add it to the pseudo
        // headers
        if let Some(authority) = parts.authority {
            pseudo.set_authority(to_string(authority.into()));
        }

        pseudo
    }

    pub fn response(status: StatusCode) -> Self {
        Pseudo {
            method: None,
            scheme: None,
            authority: None,
            path: None,
            status: Some(status),
        }
    }

    pub fn set_scheme(&mut self, scheme: String<Bytes>) {
        self.scheme = Some(scheme);
    }

    pub fn set_authority(&mut self, authority: String<Bytes>) {
        self.authority = Some(authority);
    }
}

// ===== impl Iter =====

impl Iterator for Iter {
    type Item = hpack::Header<Option<HeaderName>>;

    fn next(&mut self) -> Option<Self::Item> {
        use hpack::Header::*;

        if let Some(ref mut pseudo) = self.pseudo {
            if let Some(method) = pseudo.method.take() {
                return Some(Method(method));
            }

            if let Some(scheme) = pseudo.scheme.take() {
                return Some(Scheme(scheme));
            }

            if let Some(authority) = pseudo.authority.take() {
                return Some(Authority(authority));
            }

            if let Some(path) = pseudo.path.take() {
                return Some(Path(path));
            }

            if let Some(status) = pseudo.status.take() {
                return Some(Status(status));
            }
        }

        self.pseudo = None;

        self.fields.next()
            .map(|(name, value)| {
                Field { name: name, value: value}
            })
    }
}

// ===== impl HeadersFlag =====

impl HeadersFlag {
    pub fn empty() -> HeadersFlag {
        HeadersFlag(0)
    }

    pub fn load(bits: u8) -> HeadersFlag {
        HeadersFlag(bits & ALL)
    }

    pub fn is_end_stream(&self) -> bool {
        self.0 & END_STREAM == END_STREAM
    }

    pub fn set_end_stream(&mut self) {
        self.0 |= END_STREAM
    }

    pub fn is_end_headers(&self) -> bool {
        self.0 & END_HEADERS == END_HEADERS
    }

    pub fn is_padded(&self) -> bool {
        self.0 & PADDED == PADDED
    }

    pub fn is_priority(&self) -> bool {
        self.0 & PRIORITY == PRIORITY
    }
}

impl Default for HeadersFlag {
    /// Returns a `HeadersFlag` value with `END_HEADERS` set.
    fn default() -> Self {
        HeadersFlag(END_HEADERS)
    }
}

impl From<HeadersFlag> for u8 {
    fn from(src: HeadersFlag) -> u8 {
        src.0
    }
}
