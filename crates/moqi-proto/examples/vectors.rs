//! Prints canonical encodings as JSON so the browser codec can be checked
//! against this crate: `cargo run -p moqi-proto --example vectors | node web/test/interop.mjs`

use bytes::Bytes;
use moqi_proto::data::*;
use moqi_proto::message::*;
use moqi_proto::types::*;
use moqi_proto::Encode;

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn main() {
    let ns = TrackNamespace::new(["moqi", "demo", "a1b2c3d4"]);
    let mut out: Vec<(&str, Vec<u8>)> = Vec::new();

    let mut opts = Properties::default();
    opts.0.push((setup_option::MOQT_IMPLEMENTATION, KvValue::Bytes(Bytes::from_static(b"x/1"))));
    out.push(("setup", Message::Setup(Setup { options: opts }).encode_to_vec()));
    out.push((
        "subscribe",
        Message::Subscribe(Subscribe {
            request_id: 2,
            track: FullTrackName::new(ns.clone(), "video"),
            params: Parameters::new()
                .with(param::SUBSCRIBER_PRIORITY, ParamValue::U8(3))
                .with(param::LOCATION_FILTER, ParamValue::LocationFilter(LocationFilter::RelativeStart { start_group: 1 })),
        })
        .encode_to_vec(),
    ));
    out.push((
        "subscribe_ok",
        Message::SubscribeOk(SubscribeOk {
            track_alias: 7,
            params: Parameters::new().with(param::LARGEST_OBJECT, ParamValue::Location(Location::new(300, 20))),
            track_properties: Properties::default(),
        })
        .encode_to_vec(),
    ));
    out.push(("request_ok", Message::RequestOk(RequestOk::empty()).encode_to_vec()));
    out.push(("request_error", Message::RequestError(RequestError::new(request_error::EXCESSIVE_LOAD, "room is full")).encode_to_vec()));
    out.push((
        "publish_done",
        Message::PublishDone(PublishDone { status: publish_done::TRACK_ENDED, stream_count: 12, reason: "bye".into() }).encode_to_vec(),
    ));
    out.push((
        "publish_namespace",
        Message::PublishNamespace(PublishNamespace { request_id: 0, prefix: ns.clone(), params: Parameters::new() }).encode_to_vec(),
    ));
    out.push((
        "subscribe_namespace",
        Message::SubscribeNamespace(SubscribeNamespace { request_id: 4, prefix: ns.truncated(2), params: Parameters::new() })
            .encode_to_vec(),
    ));
    out.push(("namespace", Message::Namespace(Namespace { suffix: TrackNamespace::new(["a1b2c3d4"]) }).encode_to_vec()));

    let header = SubgroupHeader {
        track_alias: 7,
        group_id: 300,
        subgroup: SubgroupIdMode::Zero,
        publisher_priority: Some(3),
        has_properties: true,
        end_of_group: true,
        first_object: true,
    };
    let mut stream = header.encode_to_vec();
    let mut codec = SubgroupObjectCodec::new(&header);
    let mut ts = Properties::default();
    ts.set_varint(property::TIMESTAMP, 1_234_567_890);
    codec.encode(&Object::normal(0, ts.clone(), Bytes::from_static(b"key")), &mut stream);
    codec.encode(&Object::normal(1, ts.clone(), Bytes::from_static(b"delta")), &mut stream);
    out.push(("subgroup_stream", stream));

    let dg = ObjectDatagram {
        track_alias: 9,
        group_id: 70_000,
        publisher_priority: Some(1),
        end_of_group: true,
        object: Object::normal(0, ts, Bytes::from_static(b"opus")),
    };
    out.push(("datagram", dg.encode_to_vec()));

    let body: Vec<String> = out.iter().map(|(k, v)| format!("  \"{k}\": \"{}\"", hex(v))).collect();
    println!("{{\n{}\n}}", body.join(",\n"));
}
