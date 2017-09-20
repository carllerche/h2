#[macro_use]
extern crate h2_test_support;
use h2_test_support::prelude::*;

// In this case, the stream & connection both have capacity, but capacity is not
// explicitly requested.
#[test]
fn send_data_without_requesting_capacity() {
    let _ = ::env_logger::init();

    let payload = [0; 1024];

    let mock = mock_io::Builder::new()
        .handshake()
        .write(&[
            // POST /
            0, 0, 16, 1, 4, 0, 0, 0, 1, 131, 135, 65, 139, 157, 41,
            172, 75, 143, 168, 233, 25, 151, 33, 233, 132,
        ])
        .write(&[
            // DATA
            0, 4, 0, 0, 1, 0, 0, 0, 1,
        ])
        .write(&payload[..])
        .write(frames::SETTINGS_ACK)
        // Read response
        .read(&[0, 0, 1, 1, 5, 0, 0, 0, 1, 0x89])
        .build();

    let mut h2 = Client::handshake(mock).wait().unwrap();

    let request = Request::builder()
        .method(Method::POST)
        .uri("https://http2.akamai.com/")
        .body(())
        .unwrap();

    let mut stream = h2.send_request(request, false).unwrap();

    // The capacity should be immediately allocated
    assert_eq!(stream.capacity(), 0);

    // Send the data
    stream.send_data(payload[..].into(), true).unwrap();

    // Get the response
    let resp = h2.run(poll_fn(|| stream.poll_response())).unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    h2.wait().unwrap();
}

#[test]
fn release_capacity_sends_window_update() {
    let _ = ::env_logger::init();

    let payload = vec![0u8; 16_384];

    let (io, srv) = mock::new();

    let mock = srv.assert_client_handshake().unwrap()
        .recv_settings()
        .recv_frame(
            frames::headers(1)
                .request("GET", "https://http2.akamai.com/")
                .eos()
        )
        .send_frame(
            frames::headers(1)
                .response(200)
        )
        .send_frame(frames::data(1, &payload[..]))
        .send_frame(frames::data(1, &payload[..]))
        .send_frame(frames::data(1, &payload[..]))
        .recv_frame(
            frames::window_update(0, 32_768)
        )
        .recv_frame(
            frames::window_update(1, 32_768)
        )
        .send_frame(frames::data(1, &payload[..]).eos())
        // gotta end the connection
        .map(drop);

    let h2 = Client::handshake(io).unwrap().and_then(|mut h2| {
        let request = Request::builder()
            .method(Method::GET)
            .uri("https://http2.akamai.com/")
            .body(())
            .unwrap();

        let req = h2.send_request(request, true).unwrap()
                .unwrap()
                // Get the response
                .and_then(|resp| {
                    assert_eq!(resp.status(), StatusCode::OK);
                    let body = resp.into_parts().1;
                    body.into_future().unwrap()
                })

                // read some body to use up window size to below half
                .and_then(|(buf, body)| {
                    assert_eq!(buf.unwrap().len(), payload.len());
                    body.into_future().unwrap()
                })
                .and_then(|(buf, body)| {
                    assert_eq!(buf.unwrap().len(), payload.len());
                    body.into_future().unwrap()
                })
                .and_then(|(buf, mut body)| {
                    let buf = buf.unwrap();
                    assert_eq!(buf.len(), payload.len());
                    body.release_capacity(buf.len() * 2).unwrap();
                    body.into_future().unwrap()
                })
                .and_then(|(buf, _)| {
                    assert_eq!(buf.unwrap().len(), payload.len());
                    Ok(())
                });
        h2.unwrap().join(req)
    });
    h2.join(mock).wait().unwrap();
}

#[test]
fn release_capacity_of_small_amount_does_not_send_window_update() {
    let _ = ::env_logger::init();

    let payload = [0; 16];

    let (io, srv) = mock::new();

    let mock = srv.assert_client_handshake().unwrap()
        .recv_settings()
        .recv_frame(
            frames::headers(1)
                .request("GET", "https://http2.akamai.com/")
                .eos()
        )
        .send_frame(
            frames::headers(1)
                .response(200)
        )
        .send_frame(frames::data(1, &payload[..]).eos())
        // gotta end the connection
        .map(drop);

    let h2 = Client::handshake(io).unwrap().and_then(|mut h2| {
        let request = Request::builder()
            .method(Method::GET)
            .uri("https://http2.akamai.com/")
            .body(())
            .unwrap();

        let req = h2.send_request(request, true).unwrap()
                .unwrap()
                // Get the response
                .and_then(|resp| {
                    assert_eq!(resp.status(), StatusCode::OK);
                    let body = resp.into_parts().1;
                    body.into_future().unwrap()
                })
                // read the small body and then release it
                .and_then(|(buf, mut body)| {
                    let buf = buf.unwrap();
                    assert_eq!(buf.len(), 16);
                    body.release_capacity(buf.len()).unwrap();
                    body.into_future().unwrap()
                })
                .and_then(|(buf, _)| {
                    assert!(buf.is_none());
                    Ok(())
                });
        h2.unwrap().join(req)
    });
    h2.join(mock).wait().unwrap();
}

#[test]
#[ignore]
fn expand_window_sends_window_update() {}

#[test]
#[ignore]
fn expand_window_calls_are_coalesced() {}

#[test]
fn recv_data_overflows_connection_window() {
    let _ = ::env_logger::init();

    let (io, srv) = mock::new();

    let mock = srv.assert_client_handshake().unwrap()
        .recv_settings()
        .recv_frame(
            frames::headers(1)
                .request("GET", "https://http2.akamai.com/")
                .eos()
        )
        .send_frame(
            frames::headers(1)
                .response(200)
        )
        // fill the whole window
        .send_frame(frames::data(1, vec![0u8; 16_384]))
        .send_frame(frames::data(1, vec![0u8; 16_384]))
        .send_frame(frames::data(1, vec![0u8; 16_384]))
        .send_frame(frames::data(1, vec![0u8; 16_383]))
        // this frame overflows the window!
        .send_frame(frames::data(1, vec![0u8; 128]).eos())
        // expecting goaway for the conn, not stream
        .recv_frame(frames::go_away(0).flow_control());
    // connection is ended by client

    let h2 = Client::handshake(io).unwrap().and_then(|mut h2| {
        let request = Request::builder()
            .method(Method::GET)
            .uri("https://http2.akamai.com/")
            .body(())
            .unwrap();

        let req = h2.send_request(request, true)
            .unwrap()
            .unwrap()
            .and_then(|resp| {
                assert_eq!(resp.status(), StatusCode::OK);
                let body = resp.into_parts().1;
                body.concat2().then(|res| {
                    let err = res.unwrap_err();
                    assert_eq!(
                        err.to_string(),
                        "protocol error: flow-control protocol violated"
                    );
                    Ok::<(), ()>(())
                })
            });

        // client should see a flow control error
        let conn = h2.then(|res| {
            let err = res.unwrap_err();
            assert_eq!(
                err.to_string(),
                "protocol error: flow-control protocol violated"
            );
            Ok::<(), ()>(())
        });
        conn.unwrap().join(req)
    });
    h2.join(mock).wait().unwrap();
}

#[test]
fn recv_data_overflows_stream_window() {
    // this tests for when streams have smaller windows than their connection
    let _ = ::env_logger::init();

    let (io, srv) = mock::new();

    let mock = srv.assert_client_handshake().unwrap()
        .ignore_settings()
        .recv_frame(
            frames::headers(1)
                .request("GET", "https://http2.akamai.com/")
                .eos()
        )
        .send_frame(
            frames::headers(1)
                .response(200)
        )
        // fill the whole window
        .send_frame(frames::data(1, vec![0u8; 16_384]))
        // this frame overflows the window!
        .send_frame(frames::data(1, &[0; 16][..]).eos())
        // expecting goaway for the conn
        // TODO: change to a RST_STREAM eventually
        .recv_frame(frames::go_away(0).flow_control())
        // close the connection
        .map(drop);

    let h2 = Client::builder()
        .initial_window_size(16_384)
        .handshake::<_, Bytes>(io)
        .unwrap()
        .and_then(|mut h2| {
            let request = Request::builder()
                .method(Method::GET)
                .uri("https://http2.akamai.com/")
                .body(())
                .unwrap();

            let req = h2.send_request(request, true)
                .unwrap()
                .unwrap()
                .and_then(|resp| {
                    assert_eq!(resp.status(), StatusCode::OK);
                    let body = resp.into_parts().1;
                    body.concat2().then(|res| {
                        let err = res.unwrap_err();
                        assert_eq!(
                            err.to_string(),
                            "protocol error: flow-control protocol violated"
                        );
                        Ok::<(), ()>(())
                    })
                });

            // client should see a flow control error
            let conn = h2.then(|res| {
                let err = res.unwrap_err();
                assert_eq!(
                    err.to_string(),
                    "protocol error: flow-control protocol violated"
                );
                Ok::<(), ()>(())
            });
            conn.unwrap().join(req)
        });
    h2.join(mock).wait().unwrap();
}



#[test]
#[ignore]
fn recv_window_update_causes_overflow() {
    // A received window update causes the window to overflow.
}

#[test]
fn stream_close_by_data_frame_releases_capacity() {
    let _ = ::env_logger::init();
    let (io, srv) = mock::new();

    let window_size = frame::DEFAULT_INITIAL_WINDOW_SIZE as usize;

    let h2 = Client::handshake(io).unwrap().and_then(|mut h2| {
        let request = Request::builder()
            .method(Method::POST)
            .uri("https://http2.akamai.com/")
            .body(())
            .unwrap();

        // Send request
        let mut s1 = h2.send_request(request, false).unwrap();

        // This effectively reserves the entire connection window
        s1.reserve_capacity(window_size);

        // The capacity should be immediately available as nothing else is
        // happening on the stream.
        assert_eq!(s1.capacity(), window_size);

        let request = Request::builder()
            .method(Method::POST)
            .uri("https://http2.akamai.com/")
            .body(())
            .unwrap();

        // Create a second stream
        let mut s2 = h2.send_request(request, false).unwrap();

        // Request capacity
        s2.reserve_capacity(5);

        // There should be no available capacity (as it is being held up by
        // the previous stream
        assert_eq!(s2.capacity(), 0);

        // Closing the previous stream by sending an empty data frame will
        // release the capacity to s2
        s1.send_data("".into(), true).unwrap();

        // The capacity should be available
        assert_eq!(s2.capacity(), 5);

        // Send the frame
        s2.send_data("hello".into(), true).unwrap();

        // Wait for the connection to close
        h2.unwrap()
    });

    let srv = srv.assert_client_handshake().unwrap()
        .ignore_settings()
        .recv_frame(
            frames::headers(1).request("POST", "https://http2.akamai.com/")
        )
        .send_frame(frames::headers(1).response(200))
        .recv_frame(
            frames::headers(3).request("POST", "https://http2.akamai.com/")
        )
        .send_frame(frames::headers(3).response(200))
        .recv_frame(frames::data(1, &b""[..]).eos())
        .recv_frame(frames::data(3, &b"hello"[..]).eos())
        .close()
        ;
    let _ = h2.join(srv).wait().unwrap();
}

#[test]
fn stream_close_by_trailers_frame_releases_capacity() {
    let _ = ::env_logger::init();
    let (io, srv) = mock::new();

    let window_size = frame::DEFAULT_INITIAL_WINDOW_SIZE as usize;

    let h2 = Client::handshake(io).unwrap().and_then(|mut h2| {
        let request = Request::builder()
            .method(Method::POST)
            .uri("https://http2.akamai.com/")
            .body(())
            .unwrap();

        // Send request
        let mut s1 = h2.send_request(request, false).unwrap();

        // This effectively reserves the entire connection window
        s1.reserve_capacity(window_size);

        // The capacity should be immediately available as nothing else is
        // happening on the stream.
        assert_eq!(s1.capacity(), window_size);

        let request = Request::builder()
            .method(Method::POST)
            .uri("https://http2.akamai.com/")
            .body(())
            .unwrap();

        // Create a second stream
        let mut s2 = h2.send_request(request, false).unwrap();

        // Request capacity
        s2.reserve_capacity(5);

        // There should be no available capacity (as it is being held up by
        // the previous stream
        assert_eq!(s2.capacity(), 0);

        // Closing the previous stream by sending a trailers frame will
        // release the capacity to s2
        s1.send_trailers(Default::default()).unwrap();

        // The capacity should be available
        assert_eq!(s2.capacity(), 5);

        // Send the frame
        s2.send_data("hello".into(), true).unwrap();

        // Wait for the connection to close
        h2.unwrap()
    });

    let srv = srv.assert_client_handshake().unwrap()
        // Get the first frame
        .ignore_settings()
        .recv_frame(
            frames::headers(1)
                .request("POST", "https://http2.akamai.com/")
        )
        .send_frame(frames::headers(1).response(200))
        .recv_frame(
            frames::headers(3)
                .request("POST", "https://http2.akamai.com/")
        )
        .send_frame(frames::headers(3).response(200))
        .recv_frame(frames::headers(1).eos())
        .recv(frames::data(3, b"hello"[..]).eos())
        .close()
        ;

    let _ = h2.join(srv).wait().unwrap();
}

#[test]
#[ignore]
fn stream_close_by_send_reset_frame_releases_capacity() {}

#[test]
#[ignore]
fn stream_close_by_recv_reset_frame_releases_capacity() {}

use futures::{Async, Poll};

struct GetResponse {
    stream: Option<client::Stream<Bytes>>,
}

impl Future for GetResponse {
    type Item = (Response<client::Body<Bytes>>, client::Stream<Bytes>);
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let response = match self.stream.as_mut().unwrap().poll_response() {
            Ok(Async::Ready(v)) => v,
            Ok(Async::NotReady) => return Ok(Async::NotReady),
            Err(e) => panic!("unexpected error; {:?}", e),
        };

        Ok(Async::Ready((response, self.stream.take().unwrap())))
    }
}

#[test]
fn recv_window_update_on_stream_closed_by_data_frame() {
    let _ = ::env_logger::init();
    let (io, srv) = mock::new();

    let h2 = Client::handshake(io)
        .unwrap()
        .and_then(|mut h2| {
            let request = Request::builder()
                .method(Method::POST)
                .uri("https://http2.akamai.com/")
                .body(())
                .unwrap();

            let stream = h2.send_request(request, false).unwrap();

            // Wait for the response
            h2.drive(GetResponse {
                stream: Some(stream),
            })
        })
        .and_then(|(h2, (response, mut stream))| {
            assert_eq!(response.status(), StatusCode::OK);

            // Send a data frame, this will also close the connection
            stream.send_data("hello".into(), true).unwrap();

            // Wait for the connection to close
            h2.unwrap()
        });

    let srv = srv.assert_client_handshake()
        .unwrap()
        .recv_settings()
        .recv_frame(
            frames::headers(1).request("POST", "https://http2.akamai.com/"),
        )
        .send_frame(frames::headers(1).response(200))
        .recv_frame(frames::data(1, "hello").eos())
        .send_frame(frames::window_update(1, 5))
        .map(drop);

    let _ = h2.join(srv).wait().unwrap();
}

#[test]
fn reserved_capacity_assigned_in_multi_window_updates() {
    let _ = ::env_logger::init();
    let (io, srv) = mock::new();

    let h2 = Client::handshake(io)
        .unwrap()
        .and_then(|mut h2| {
            let request = Request::builder()
                .method(Method::POST)
                .uri("https://http2.akamai.com/")
                .body(())
                .unwrap();

            let mut stream = h2.send_request(request, false).unwrap();

            // Consume the capacity
            let payload = vec![0; frame::DEFAULT_INITIAL_WINDOW_SIZE as usize];
            stream.send_data(payload.into(), false).unwrap();

            // Reserve more data than we want
            stream.reserve_capacity(10);

            h2.drive(util::wait_for_capacity(stream, 5))
        })
        .and_then(|(h2, mut stream)| {
            stream.send_data("hello".into(), false).unwrap();
            stream.send_data("world".into(), true).unwrap();

            h2.drive(GetResponse {
                stream: Some(stream),
            })
        })
        .and_then(|(h2, (response, _))| {
            assert_eq!(response.status(), StatusCode::NO_CONTENT);

            // Wait for the connection to close
            h2.unwrap()
        });

    let srv = srv.assert_client_handshake().unwrap()
        .recv_settings()
        .recv_frame(
            frames::headers(1)
                .request("POST", "https://http2.akamai.com/")
        )
        .recv_frame(frames::data(1, vec![0u8; 16_384]))
        .recv_frame(frames::data(1, vec![0u8; 16_384]))
        .recv_frame(frames::data(1, vec![0u8; 16_384]))
        .recv_frame(frames::data(1, vec![0u8; 16_383]))
        .idle_ms(100)
        // Increase the connection window
        .send_frame(
            frames::window_update(0, 10))
        // Incrementally increase the stream window
        .send_frame(
            frames::window_update(1, 4))
        .idle_ms(50)
        .send_frame(
            frames::window_update(1, 1))
        // Receive first chunk
        .recv_frame(frames::data(1, "hello"))
        .send_frame(
            frames::window_update(1, 5))
        // Receive second chunk
        .recv_frame(
            frames::data(1, "world").eos())
        .send_frame(
            frames::headers(1)
                .response(204)
                .eos()
        )
        /*
        .recv_frame(frames::data(1, "hello").eos())
        .send_frame(frames::window_update(1, 5))
        */
        .map(drop);

    let _ = h2.join(srv).wait().unwrap();
}

#[test]
fn connection_notified_on_released_capacity() {
    use futures::sync::oneshot;
    use std::thread;
    use std::sync::mpsc;

    let _ = ::env_logger::init();
    let (io, srv) = mock::new();

    // We're going to run the connection on a thread in order to isolate task
    // notifications. This test is here, in part, to ensure that the connection
    // receives the appropriate notifications to send out window updates.

    let (tx, rx) = mpsc::channel();

    // Because threading is fun
    let (settings_tx, settings_rx) = oneshot::channel();

    let th1 = thread::spawn(move || {
        srv.assert_client_handshake().unwrap()
            .recv_settings()
            .map(move |v| {
                settings_tx.send(()).unwrap();
                v
            })
            // Get the first request
            .recv_frame(
                frames::headers(1)
                    .request("GET", "https://example.com/a")
                    .eos())
            // Get the second request
            .recv_frame(
                frames::headers(3)
                    .request("GET", "https://example.com/b")
                    .eos())
            // Send the first response
            .send_frame(frames::headers(1).response(200))
            // Send the second response
            .send_frame(frames::headers(3).response(200))

            // Fill the connection window
            .send_frame(frames::data(1, vec![0u8; 16_384]).eos())
            .idle_ms(100)
            .send_frame(frames::data(3, vec![0u8; 16_384]).eos())

            // The window update is sent
            .recv_frame(frames::window_update(0, 16_384))
            .map(drop)
            .wait().unwrap();
    });


    let th2 = thread::spawn(move || {
        let h2 = Client::handshake(io).wait().unwrap();

        let (mut h2, _) = h2.drive(settings_rx).wait().unwrap();

        let request = Request::get("https://example.com/a")
            .body(())
            .unwrap();

        tx.send(h2.send_request(request, true).unwrap()).unwrap();

        let request = Request::get("https://example.com/b")
            .body(())
            .unwrap();

        tx.send(h2.send_request(request, true).unwrap()).unwrap();

        // Run the connection to completion
        h2.wait().unwrap();
    });

    // Get the two requests
    let a = rx.recv().unwrap();
    let b = rx.recv().unwrap();

    // Get the first response
    let response = a.wait().unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let (_, a) = response.into_parts();

    // Get the next chunk
    let (chunk, mut a) = a.into_future().wait().unwrap();
    assert_eq!(16_384, chunk.unwrap().len());

    // Get the second response
    let response = b.wait().unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let (_, b) = response.into_parts();

    // Get the next chunk
    let (chunk, b) = b.into_future().wait().unwrap();
    assert_eq!(16_384, chunk.unwrap().len());

    // Wait a bit
    thread::sleep(Duration::from_millis(100));

    // Release the capacity
    a.release_capacity(16_384).unwrap();

    th1.join().unwrap();
    th2.join().unwrap();

    // Explicitly drop this after the joins so that the capacity doesn't get
    // implicitly released before.
    drop(b);
}
