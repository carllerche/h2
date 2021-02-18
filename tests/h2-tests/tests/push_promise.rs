use futures::future::join;
use futures::{StreamExt, TryStreamExt};
use h2_support::prelude::*;

#[tokio::test]
async fn recv_push_works() {
    h2_support::trace_init!();

    let (io, mut srv) = mock::new();
    let mock = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);
        srv.recv_frame(
            frames::headers(1)
                .request("GET", "https://http2.akamai.com/")
                .eos(),
        )
        .await;
        srv.send_frame(frames::headers(1).response(404)).await;
        srv.send_frame(
            frames::push_promise(1, 2).request("GET", "https://http2.akamai.com/style.css"),
        )
        .await;
        srv.send_frame(frames::data(1, "").eos()).await;
        srv.send_frame(frames::headers(2).response(200)).await;
        srv.send_frame(frames::data(2, "promised_data").eos()).await;
    };
    let h2 = async move {
        let (mut client, mut h2) = client::handshake(io).await.unwrap();
        let request = Request::builder()
            .method(Method::GET)
            .uri("https://http2.akamai.com/")
            .body(())
            .unwrap();
        let (mut resp, _) = client.send_request(request, true).unwrap();
        let pushed = resp.push_promises();
        let check_resp_status = async move {
            let resp = resp.await.unwrap();
            assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        };
        let check_pushed_response = async move {
            let p = pushed.and_then(|headers| async move {
                let (request, response) = headers.into_parts();
                assert_eq!(request.into_parts().0.method, Method::GET);
                let resp = response.await.unwrap();
                assert_eq!(resp.status(), StatusCode::OK);
                let b = util::concat(resp.into_body()).await.unwrap();
                assert_eq!(b, "promised_data");
                Ok(())
            });
            let ps: Vec<_> = p.collect().await;
            assert_eq!(1, ps.len())
        };

        h2.drive(join(check_resp_status, check_pushed_response))
            .await;
    };

    join(mock, h2).await;
}

#[tokio::test]
async fn pushed_streams_arent_dropped_too_early() {
    // tests that by default, received push promises work
    h2_support::trace_init!();

    let (io, mut srv) = mock::new();
    let mock = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);
        srv.recv_frame(
            frames::headers(1)
                .request("GET", "https://http2.akamai.com/")
                .eos(),
        )
        .await;
        srv.send_frame(frames::headers(1).response(404)).await;
        srv.send_frame(
            frames::push_promise(1, 2).request("GET", "https://http2.akamai.com/style.css"),
        )
        .await;
        srv.send_frame(
            frames::push_promise(1, 4).request("GET", "https://http2.akamai.com/style2.css"),
        )
        .await;
        srv.send_frame(frames::data(1, "").eos()).await;
        idle_ms(10).await;
        srv.send_frame(frames::headers(2).response(200)).await;
        srv.send_frame(frames::headers(4).response(200).eos()).await;
        srv.send_frame(frames::data(2, "").eos()).await;
        srv.recv_frame(frames::go_away(4)).await;
    };

    let h2 = async move {
        let (mut client, mut h2) = client::handshake(io).await.unwrap();
        let request = Request::builder()
            .method(Method::GET)
            .uri("https://http2.akamai.com/")
            .body(())
            .unwrap();
        let (mut resp, _) = client.send_request(request, true).unwrap();
        let mut pushed = resp.push_promises();
        let check_status = async move {
            let resp = resp.await.unwrap();
            assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        };

        let check_pushed = async move {
            let mut count = 0;
            while let Some(headers) = pushed.next().await {
                let (request, response) = headers.unwrap().into_parts();
                assert_eq!(request.into_parts().0.method, Method::GET);
                let resp = response.await.unwrap();
                assert_eq!(resp.status(), StatusCode::OK);
                count += 1;
            }
            assert_eq!(2, count);
        };

        drop(client);

        h2.drive(join(check_pushed, check_status)).await;
        h2.await.expect("client");
    };

    join(mock, h2).await;
}

#[tokio::test]
async fn recv_push_when_push_disabled_is_conn_error() {
    h2_support::trace_init!();

    let (io, mut srv) = mock::new();
    let mock = async move {
        let _ = srv.assert_client_handshake().await;
        srv.recv_frame(
            frames::headers(1)
                .request("GET", "https://http2.akamai.com/")
                .eos(),
        )
        .await;
        srv.send_frame(
            frames::push_promise(1, 3).request("GET", "https://http2.akamai.com/style.css"),
        )
        .await;
        srv.send_frame(frames::headers(1).response(200).eos()).await;
        srv.recv_frame(frames::go_away(0).protocol_error()).await;
    };

    let h2 = async move {
        let (mut client, h2) = client::Builder::new()
            .enable_push(false)
            .handshake::<_, Bytes>(io)
            .await
            .unwrap();
        let request = Request::builder()
            .method(Method::GET)
            .uri("https://http2.akamai.com/")
            .body(())
            .unwrap();

        let req = async move {
            let res = client.send_request(request, true).unwrap().0.await;
            let err = res.unwrap_err();
            assert_eq!(
                err.to_string(),
                "protocol error: unspecific protocol error detected"
            );
        };

        // client should see a protocol error
        let conn = async move {
            let res = h2.await;
            let err = res.unwrap_err();
            assert_eq!(
                err.to_string(),
                "protocol error: unspecific protocol error detected"
            );
        };

        join(conn, req).await;
    };

    join(mock, h2).await;
}

#[tokio::test]
async fn pending_push_promises_reset_when_dropped() {
    h2_support::trace_init!();

    let (io, mut srv) = mock::new();
    let srv = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);
        srv.recv_frame(
            frames::headers(1)
                .request("GET", "https://http2.akamai.com/")
                .eos(),
        )
        .await;
        srv.send_frame(
            frames::push_promise(1, 2).request("GET", "https://http2.akamai.com/style.css"),
        )
        .await;
        srv.send_frame(frames::headers(1).response(200).eos()).await;
        srv.recv_frame(frames::reset(2).cancel()).await;
    };

    let client = async move {
        let (mut client, mut conn) = client::handshake(io).await.unwrap();
        let request = Request::builder()
            .method(Method::GET)
            .uri("https://http2.akamai.com/")
            .body(())
            .unwrap();
        let req = async {
            let resp = client
                .send_request(request, true)
                .unwrap()
                .0
                .await
                .expect("response");
            assert_eq!(resp.status(), StatusCode::OK);
        };

        let _ = conn.drive(req).await;
        conn.await.expect("client");
        drop(client);
    };

    join(srv, client).await;
}

#[tokio::test]
async fn recv_push_promise_over_max_header_list_size() {
    h2_support::trace_init!();
    let (io, mut srv) = mock::new();

    let srv = async move {
        let settings = srv.assert_client_handshake().await;
        assert_frame_eq(settings, frames::settings().max_header_list_size(10));
        srv.recv_frame(
            frames::headers(1)
                .request("GET", "https://http2.akamai.com/")
                .eos(),
        )
        .await;
        srv.send_frame(
            frames::push_promise(1, 2).request("GET", "https://http2.akamai.com/style.css"),
        )
        .await;
        srv.recv_frame(frames::reset(2).refused()).await;
        srv.send_frame(frames::headers(1).response(200).eos()).await;
        idle_ms(10).await;
    };

    let client = async move {
        let (mut client, mut conn) = client::Builder::new()
            .max_header_list_size(10)
            .handshake::<_, Bytes>(io)
            .await
            .expect("handshake");
        let request = Request::builder()
            .uri("https://http2.akamai.com/")
            .body(())
            .unwrap();

        let req = async move {
            let err = client
                .send_request(request, true)
                .expect("send_request")
                .0
                .await
                .expect_err("response");
            assert_eq!(err.reason(), Some(Reason::REFUSED_STREAM));
        };

        conn.drive(req).await;
        conn.await.expect("client");
    };
    join(srv, client).await;
}

#[tokio::test]
async fn recv_invalid_push_promise_headers_is_stream_protocol_error() {
    // Unsafe method or content length is stream protocol error
    h2_support::trace_init!();

    let (io, mut srv) = mock::new();
    let mock = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);
        srv.recv_frame(
            frames::headers(1)
                .request("GET", "https://http2.akamai.com/")
                .eos(),
        )
        .await;
        srv.send_frame(frames::headers(1).response(404)).await;
        srv.send_frame(
            frames::push_promise(1, 2).request("POST", "https://http2.akamai.com/style.css"),
        )
        .await;
        srv.send_frame(
            frames::push_promise(1, 4)
                .request("GET", "https://http2.akamai.com/style.css")
                .field(http::header::CONTENT_LENGTH, 1),
        )
        .await;
        srv.send_frame(
            frames::push_promise(1, 6)
                .request("GET", "https://http2.akamai.com/style.css")
                .field(http::header::CONTENT_LENGTH, 0),
        )
        .await;
        srv.send_frame(frames::headers(1).response(404).eos()).await;
        srv.recv_frame(frames::reset(2).protocol_error()).await;
        srv.recv_frame(frames::reset(4).protocol_error()).await;
        srv.send_frame(frames::headers(6).response(200).eos()).await;
    };

    let h2 = async move {
        let (mut client, mut h2) = client::handshake(io).await.unwrap();
        let request = Request::builder()
            .method(Method::GET)
            .uri("https://http2.akamai.com/")
            .body(())
            .unwrap();
        let (mut resp, _) = client.send_request(request, true).unwrap();
        let check_pushed_response = async move {
            let pushed = resp.push_promises();
            let p = pushed.and_then(|headers| headers.into_parts().1);
            let ps: Vec<_> = p.collect().await;
            // CONTENT_LENGTH = 0 is ok
            assert_eq!(1, ps.len());
        };
        h2.drive(check_pushed_response).await;
    };

    join(mock, h2).await;
}

#[test]
#[ignore]
fn recv_push_promise_with_wrong_authority_is_stream_error() {
    // if server is foo.com, :authority = bar.com is stream error
}

#[tokio::test]
async fn recv_push_promise_skipped_stream_id() {
    h2_support::trace_init!();

    let (io, mut srv) = mock::new();
    let mock = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);
        srv.recv_frame(
            frames::headers(1)
                .request("GET", "https://http2.akamai.com/")
                .eos(),
        )
        .await;
        srv.send_frame(
            frames::push_promise(1, 4).request("GET", "https://http2.akamai.com/style.css"),
        )
        .await;
        srv.send_frame(
            frames::push_promise(1, 2).request("GET", "https://http2.akamai.com/style.css"),
        )
        .await;
        srv.recv_frame(frames::go_away(0).protocol_error()).await;
    };

    let h2 = async move {
        let (mut client, h2) = client::handshake(io).await.unwrap();
        let request = Request::builder()
            .method(Method::GET)
            .uri("https://http2.akamai.com/")
            .body(())
            .unwrap();

        let req = async move {
            let res = client.send_request(request, true).unwrap().0.await;
            assert!(res.is_err());
        };

        // client should see a protocol error
        let conn = async move {
            let res = h2.await;
            let err = res.unwrap_err();
            assert_eq!(
                err.to_string(),
                "protocol error: unspecific protocol error detected"
            );
        };

        join(conn, req).await;
    };

    join(mock, h2).await;
}

#[tokio::test]
async fn recv_push_promise_dup_stream_id() {
    h2_support::trace_init!();

    let (io, mut srv) = mock::new();
    let mock = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);
        srv.recv_frame(
            frames::headers(1)
                .request("GET", "https://http2.akamai.com/")
                .eos(),
        )
        .await;
        srv.send_frame(
            frames::push_promise(1, 2).request("GET", "https://http2.akamai.com/style.css"),
        )
        .await;
        srv.send_frame(
            frames::push_promise(1, 2).request("GET", "https://http2.akamai.com/style.css"),
        )
        .await;
        srv.recv_frame(frames::go_away(0).protocol_error()).await;
    };

    let h2 = async move {
        let (mut client, h2) = client::handshake(io).await.unwrap();
        let request = Request::builder()
            .method(Method::GET)
            .uri("https://http2.akamai.com/")
            .body(())
            .unwrap();

        let req = async move {
            let res = client.send_request(request, true).unwrap().0.await;
            assert!(res.is_err());
        };

        // client should see a protocol error
        let conn = async move {
            let res = h2.await;
            let err = res.unwrap_err();
            assert_eq!(
                err.to_string(),
                "protocol error: unspecific protocol error detected"
            );
        };

        join(conn, req).await;
    };

    join(mock, h2).await;
}

#[tokio::test]
async fn promised_stream_is_reset_if_max_streams_is_exceeded() {
    h2_support::trace_init!();

    let (io, mut srv) = mock::new();
    let mock = async move {
        srv.assert_client_handshake().await;
        srv.recv_frame(
            frames::headers(1)
                .request("GET", "https://http2.akamai.com/")
                .eos(),
        )
        .await;
        srv.send_frame(
            frames::push_promise(1, 2).request("GET", "https://http2.akamai.com/style.css"),
        )
        .await;
        srv.send_frame(frames::headers(2).response(404)).await;
        srv.send_frame(
            frames::push_promise(1, 4).request("GET", "https://http2.akamai.com/script.js"),
        )
        .await;
        srv.send_frame(
            frames::push_promise(1, 6).request("GET", "https://http2.akamai.com/image.png"),
        )
        .await;
        srv.ping_pong([1; 8]).await;
        srv.send_frame(frames::headers(4).response(404)).await;
        srv.recv_frame(frames::reset(4).refused()).await;
        srv.send_frame(frames::data(2, "").eos()).await;
        srv.send_frame(frames::headers(6).response(404).eos()).await;
        srv.send_frame(frames::headers(1).response(404).eos()).await;
    };

    let h2 = async move {
        let (mut client, mut h2) = client::Builder::new()
            .max_concurrent_streams(1)
            .handshake::<_, Bytes>(io)
            .await
            .unwrap();
        let request = Request::builder()
            .method(Method::GET)
            .uri("https://http2.akamai.com/")
            .body(())
            .unwrap();
        let (mut resp, _) = client.send_request(request, true).unwrap();
        let push_promises = resp.push_promises();
        let check_pushed_response = async move {
            let mut push_promises = push_promises
                .map(Result::unwrap)
                .map(h2::client::PushPromise::into_parts);

            while let Some((req, resp)) = push_promises.next().await {
                match req.uri().path() {
                    "/style.css" | "/image.png" => {
                        resp.await.unwrap().into_body().data().await;
                    }
                    "/script.js" => assert_eq!(
                        resp.await.unwrap_err().reason(),
                        Some(Reason::REFUSED_STREAM)
                    ),
                    _ => unreachable!(),
                }
            }
        };
        h2.drive(check_pushed_response).await;
    };

    join(mock, h2).await;
}

#[tokio::test]
async fn too_many_promises_is_enhance_your_calm_error() {
    h2_support::trace_init!();

    let (io, mut srv) = mock::new();
    let mock = async move {
        srv.assert_client_handshake().await;
        srv.recv_frame(
            frames::headers(1)
                .request("GET", "https://http2.akamai.com/")
                .eos(),
        )
        .await;
        srv.send_frame(
            frames::push_promise(1, 2).request("GET", "https://http2.akamai.com/style.css"),
        )
        .await;
        srv.send_frame(
            frames::push_promise(1, 4).request("GET", "https://http2.akamai.com/script.js"),
        )
        .await;
        srv.ping_pong([1; 8]).await;
        srv.send_frame(
            frames::push_promise(1, 6).request("GET", "https://http2.akamai.com/image.png"),
        )
        .await;
        srv.recv_frame(frames::reset(1).reason(Reason::ENHANCE_YOUR_CALM))
            .await;
        srv.send_frame(frames::headers(1).response(404).eos()).await;
    };

    let h2 = async move {
        let (mut client, mut h2) = client::Builder::new()
            .max_remote_reserved_streams(2)
            .handshake::<_, Bytes>(io)
            .await
            .unwrap();
        let request = Request::builder()
            .method(Method::GET)
            .uri("https://http2.akamai.com/")
            .body(())
            .unwrap();
        let (resp, _) = client.send_request(request, true).unwrap();
        let _ = h2.drive(resp).await;
    };

    join(mock, h2).await;
}
