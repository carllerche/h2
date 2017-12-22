extern crate env_logger;
extern crate futures;
extern crate h2;
extern crate http;
extern crate io_dump;
extern crate rustls;
extern crate tokio_core;
extern crate tokio_rustls;
extern crate webpki_roots;

use h2::client::Connection;

use futures::*;
use http::{Method, Request};

use tokio_core::net::TcpStream;
use tokio_core::reactor;

use rustls::Session;
use tokio_rustls::ClientConfigExt;

use std::net::ToSocketAddrs;

const ALPN_H2: &str = "h2";

pub fn main() {
    let _ = env_logger::init();

    let tls_client_config = std::sync::Arc::new({
        let mut c = rustls::ClientConfig::new();
        c.root_store
            .add_server_trust_anchors(&webpki_roots::TLS_SERVER_ROOTS);
        c.alpn_protocols.push(ALPN_H2.to_owned());
        c
    });

    // Sync DNS resolution.
    let addr = "http2.akamai.com:443"
        .to_socket_addrs()
        .unwrap()
        .next()
        .unwrap();

    println!("ADDR: {:?}", addr);

    let mut core = reactor::Core::new().unwrap();
    let handle = core.handle();

    let tcp = TcpStream::connect(&addr, &handle);

    let tcp = tcp.then(|res| {
        let tcp = res.unwrap();
        tls_client_config
            .connect_async("http2.akamai.com", tcp)
            .then(|res| {
                let tls = res.unwrap();
                let negotiated_protcol = {
                    let (_, session) = tls.get_ref();
                    session.get_alpn_protocol()
                };
                assert_eq!(Some(ALPN_H2), negotiated_protcol.as_ref().map(|x| &**x));

                // Dump output to stdout
                let tls = io_dump::Dump::to_stdout(tls);

                println!("Starting client handshake");
                Connection::handshake(tls)
            })
            .then(|res| {
                let (h2, mut client) = res.unwrap();

                let request = Request::builder()
                    .method(Method::GET)
                    .uri("https://http2.akamai.com/")
                    .body(())
                    .unwrap();

                let (response, _) = client.send_request(request, true).unwrap();

                let stream = response.and_then(|response| {
                    let (_, body) = response.into_parts();

                    body.for_each(|chunk| {
                        println!("RX: {:?}", chunk);
                        Ok(())
                    })
                });

                h2.join(stream)
            })
    });

    core.run(tcp).unwrap();
}
