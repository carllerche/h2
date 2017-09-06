extern crate h2_test_support;
use h2_test_support::*;

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

    let mut h2 = Client::handshake(mock)
        .wait().unwrap();

    let request = Request::builder()
        .method(Method::POST)
        .uri("https://http2.akamai.com/")
        .body(()).unwrap();

    let mut stream = h2.request(request, false).unwrap();

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

    let payload = vec![0; 65_535];

    let mock = mock_io::Builder::new()
        .handshake()
        .write(&[
            // GET /
            0, 0, 16, 1, 5, 0, 0, 0, 1, 130, 135, 65, 139, 157, 41,
            172, 75, 143, 168, 233, 25, 151, 33, 233, 132,
        ])
        .write(frames::SETTINGS_ACK)
        // Read response
        .read(&[0, 0, 1, 1, 4, 0, 0, 0, 1, 0x88])
        .read(&[
            // DATA
            0, 64, 0, 0, 0, 0, 0, 0, 1,
        ])
        .read(&payload[0..16_384])
        .read(&[
            // DATA
            0, 64, 0, 0, 0, 0, 0, 0, 1,
        ])
        .read(&payload[16_384..16_384*2])
        .read(&[
            // DATA
            0, 64, 0, 0, 0, 0, 0, 0, 1,
        ])
        .read(&payload[16_384*2..16_384*3])
        .write(&[0, 0, 4, 8, 0, 0, 0, 0, 0, 0, 0, 128, 0])
        .write(&[0, 0, 4, 8, 0, 0, 0, 0, 1, 0, 0, 128, 0])
        .read(&[
            // DATA
            0, 63, 255, 0, 1, 0, 0, 0, 1,
        ])
        .read(&payload[16_384*3..16_384*4 - 1])

        // we send a 2nd stream in order to test the window update is
        // is actually written to the socket
        .write(&[
            // GET /
            0, 0, 4, 1, 5, 0, 0, 0, 3, 130, 135, 190, 132,
        ])
        .read(&[0, 0, 1, 1, 5, 0, 0, 0, 3, 0x88])
        .build();

    let mut h2 = Client::handshake(mock)
        .wait().unwrap();

    let request = Request::builder()
        .method(Method::GET)
        .uri("https://http2.akamai.com/")
        .body(()).unwrap();

    let mut stream = h2.request(request, true).unwrap();

    // Get the response
    let resp = h2.run(poll_fn(|| stream.poll_response())).unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // read some body to use up window size to below half
    let mut body = resp.into_parts().1;
    let buf = h2.run(poll_fn(|| body.poll())).unwrap().unwrap();
    assert_eq!(buf.len(), 16_384);
    let buf = h2.run(poll_fn(|| body.poll())).unwrap().unwrap();
    assert_eq!(buf.len(), 16_384);
    let buf = h2.run(poll_fn(|| body.poll())).unwrap().unwrap();
    assert_eq!(buf.len(), 16_384);

    // release some capacity to send a window_update
    body.release_capacity(buf.len() * 2).unwrap();

    let buf = h2.run(poll_fn(|| body.poll())).unwrap().unwrap();
    assert_eq!(buf.len(), 16_383);


    // send a 2nd stream to force flushing of window updates
    let request = Request::builder()
        .method(Method::GET)
        .uri("https://http2.akamai.com/")
        .body(()).unwrap();
    let mut stream = h2.request(request, true).unwrap();
    let resp = h2.run(poll_fn(|| stream.poll_response())).unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    h2.wait().unwrap();
}

#[test]
fn release_capacity_of_small_amount_does_not_send_window_update() {
    let _ = ::env_logger::init();

    let payload = [0; 16];

    let mock = mock_io::Builder::new()
        .handshake()
        .write(&[
            // GET /
            0, 0, 16, 1, 5, 0, 0, 0, 1, 130, 135, 65, 139, 157, 41,
            172, 75, 143, 168, 233, 25, 151, 33, 233, 132,
        ])
        .write(frames::SETTINGS_ACK)
        // Read response
        .read(&[0, 0, 1, 1, 4, 0, 0, 0, 1, 0x88])
        .read(&[
            // DATA
            0, 0, 16, 0, 1, 0, 0, 0, 1,
        ])
        .read(&payload[..])
        // write() or WINDOW_UPDATE purposefully excluded

        // we send a 2nd stream in order to test the window update is
        // is actually written to the socket
        .write(&[
            // GET /
            0, 0, 4, 1, 5, 0, 0, 0, 3, 130, 135, 190, 132,
        ])
        .read(&[0, 0, 1, 1, 5, 0, 0, 0, 3, 0x88])
        .build();

    let mut h2 = Client::handshake(mock)
        .wait().unwrap();

    let request = Request::builder()
        .method(Method::GET)
        .uri("https://http2.akamai.com/")
        .body(()).unwrap();

    let mut stream = h2.request(request, true).unwrap();

    // Get the response
    let resp = h2.run(poll_fn(|| stream.poll_response())).unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let mut body = resp.into_parts().1;
    let buf = h2.run(poll_fn(|| body.poll())).unwrap().unwrap();

    // release some capacity to send a window_update
    body.release_capacity(buf.len()).unwrap();

    // send a 2nd stream to force flushing of window updates
    let request = Request::builder()
        .method(Method::GET)
        .uri("https://http2.akamai.com/")
        .body(()).unwrap();
    let mut stream = h2.request(request, true).unwrap();
    let resp = h2.run(poll_fn(|| stream.poll_response())).unwrap();
    assert_eq!(resp.status(), StatusCode::OK);


    h2.wait().unwrap();
}

#[test]
#[ignore]
fn expand_window_sends_window_update() {
}

#[test]
#[ignore]
fn expand_window_calls_are_coalesced() {
}

#[test]
#[ignore]
fn recv_data_overflows_window() {
}

#[test]
#[ignore]
fn recv_window_update_causes_overflow() {
    // A received window update causes the window to overflow.
}
