//! Drive both clients against the same MMS peer, fragmenting the async path to seven bytes.
#![allow(clippy::panic)]

use super::{NODE, PROFILE};
use iec61850_kit::ber::Cursor;
use iec61850_kit::client::{Client, ClientConfig, TariffProfile, TariffRead, TariffSchedule};
use iec61850_kit::proto::data::{DataView, Value};
use iec61850_kit::proto::mms::association::{Association, AssociationConfig, AssociationEvent};
use iec61850_kit::proto::mms::{AccessResult, ConfirmedRequest, ConfirmedResponse, Mms, ObjectName, VariableAccess, VariableSpecification};
use iec61850_kit::{Error, Fc, Instant, Limits, ObjectReference, Result, TimeQuality, UtcTime};
use std::collections::VecDeque;
use std::io::{Read, Write};
use std::ops::ControlFlow;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::sync::oneshot;

const PROFILE_INT: TariffProfile<'static> = TariffProfile { first_price: "ValING1", ..PROFILE };

#[derive(Clone, Copy, PartialEq)]
enum Scenario {
    Stable,
    QualityChanged,
    MissingPrice,
    SurplusPrice,
    HoldFinalHeader,
}

struct Peer {
    assoc: Association,
    output: VecDeque<u8>,
    scenario: Scenario,
    saw_prices: bool,
    held: Option<oneshot::Sender<()>>,
}

impl Peer {
    fn new(scenario: Scenario) -> Self {
        Self { assoc: Association::server(AssociationConfig::default()), output: VecDeque::new(), scenario, saw_prices: false, held: None }
    }

    fn receive(&mut self, bytes: &[u8]) {
        self.assoc.on_bytes(Instant::ZERO, bytes);
        while let Some(event) = self.assoc.poll_event() {
            if let AssociationEvent::Request { invoke_id, pdu } = event {
                let Mms::ConfirmedRequest { service: ConfirmedRequest::Read { access: VariableAccess::ListOfVariable(vars), .. }, .. } =
                    Mms::parse(&pdu, &Limits::DEFAULT).unwrap()
                else {
                    panic!("expected a named-variable Read")
                };
                let mut values = Vec::new();
                for var in vars {
                    let Some(ObjectName::DomainSpecific { domain: "IED1LD0", item }) = var.name() else { panic!("unexpected domain") };
                    values.push(match *item {
                        "GridFSCH1$SP$NumEntr$setVal" => Value::Integer(3),
                        "GridFSCH1$SP$SchdIntv$setVal" => Value::Integer(8),
                        "GridFSCH1$CF$SchdIntv$units" => Value::Structure(vec![Value::Integer(84)]),
                        "GridFSCH1$SP$StrTm1$setTm" => {
                            if self.saw_prices && self.scenario == Scenario::HoldFinalHeader {
                                self.held.take().unwrap().send(()).unwrap();
                                return;
                            }
                            let quality = if self.saw_prices && self.scenario == Scenario::QualityChanged {
                                TimeQuality::UNSYNCHRONIZED
                            } else {
                                TimeQuality::SYNCHRONIZED
                            };
                            Value::UtcTime(UtcTime::from_unix(86_400, 0, quality))
                        }
                        "GridFSCH1$SP$ValING1$setVal" => {
                            self.saw_prices = true;
                            Value::Integer(-1)
                        }
                        "GridFSCH1$SP$ValING2$setVal" => Value::Integer(0),
                        "GridFSCH1$SP$ValING3$setVal" if self.scenario == Scenario::MissingPrice => continue,
                        "GridFSCH1$SP$ValING3$setVal" => Value::Integer(2),
                        other => panic!("unexpected field {other}"),
                    });
                }
                if self.scenario == Scenario::SurplusPrice && values.last() == Some(&Value::Integer(2)) {
                    values.push(Value::Integer(9));
                }
                let encoded = Value::encode_all(&values).unwrap();
                let mut cursor = Cursor::new(&encoded);
                let results = values.iter().map(|_| AccessResult::Success(cursor.next_required().unwrap())).collect();
                self.assoc.respond(invoke_id, &ConfirmedResponse::Read { access: None, results }).unwrap();
            }
        }
        while let Some(packet) = self.assoc.poll_transmit() {
            self.output.extend(packet);
        }
    }
}

impl Write for Peer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.receive(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl Read for Peer {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.output.read(buf)
    }
}

async fn serve(mut peer: Peer, mut stream: DuplexStream) {
    let mut buf = [0; 7];
    loop {
        let n = stream.read(&mut buf).await.unwrap();
        if n == 0 {
            return;
        }
        peer.receive(&buf[..n]);
        stream.write_all(peer.output.make_contiguous()).await.unwrap();
        peer.output.clear();
    }
}

async fn event(assoc: &mut Association, stream: &mut DuplexStream) -> AssociationEvent {
    loop {
        while let Some(packet) = assoc.poll_transmit() {
            stream.write_all(packet).await.unwrap();
        }
        if let Some(event) = assoc.poll_event() {
            return event;
        }
        let mut buf = [0; 7];
        let n = stream.read(&mut buf).await.unwrap();
        assert!(n > 0);
        assoc.on_bytes(Instant::ZERO, &buf[..n]);
    }
}

async fn read_async(mut stream: DuplexStream, max_pdu: u32) -> Result<TariffSchedule> {
    let mut assoc = Association::client(AssociationConfig { max_pdu, ..AssociationConfig::default() });
    assoc.start(Instant::ZERO)?;
    assert!(matches!(event(&mut assoc, &mut stream).await, AssociationEvent::Established(_)));
    let mut read = TariffRead::new(NODE, PROFILE_INT, 86_400..172_800)?;
    loop {
        let response = read_fields(&mut assoc, &mut stream, &read.fields()).await;
        match read.accept(response)? {
            ControlFlow::Continue(next) => read = next,
            ControlFlow::Break(day) => return Ok(day),
        }
    }
}

async fn read_fields(assoc: &mut Association, stream: &mut DuplexStream, fields: &[(String, Fc)]) -> Result<Vec<Value>> {
    let names: Vec<_> = fields
        .iter()
        .map(|(name, fc)| {
            let parsed = ObjectReference::parse(name).unwrap();
            let (domain, item) = parsed.to_mms(*fc);
            (domain.to_owned(), item)
        })
        .collect();
    let vars = names.iter().map(|(domain, item)| VariableSpecification::Name(ObjectName::DomainSpecific { domain, item })).collect();
    let id = assoc.call(Instant::ZERO, &ConfirmedRequest::Read { specification_with_result: false, access: VariableAccess::ListOfVariable(vars) })?;
    let AssociationEvent::Response { invoke_id, pdu } = event(assoc, stream).await else { panic!("expected response") };
    assert_eq!(invoke_id, id);
    let Mms::ConfirmedResponse { service: ConfirmedResponse::Read { results, .. }, .. } = Mms::parse(&pdu, &Limits::DEFAULT)? else { panic!("expected Read") };
    results
        .into_iter()
        .map(|r| match r {
            AccessResult::Success(tlv) => DataView::from_tlv(tlv)?.to_owned(&Limits::DEFAULT),
            AccessResult::Failure(code) => Err(Error::DataAccess(code)),
        })
        .collect()
}

#[tokio::test]
async fn fragmented_async_matches_blocking() {
    for (scenario, max_pdu) in
        [(Scenario::Stable, 65535), (Scenario::Stable, 128), (Scenario::QualityChanged, 128), (Scenario::MissingPrice, 128), (Scenario::SurplusPrice, 128)]
    {
        let mut cfg = ClientConfig::default();
        cfg.association.max_pdu = max_pdu;
        let mut client = Client::with_transport(Peer::new(scenario), &cfg, |_, _| Ok(())).unwrap();
        let expected = client.read_tariff_schedule(NODE, PROFILE_INT, 86_400..172_800);
        match scenario {
            Scenario::Stable => assert_eq!(expected.as_ref().unwrap().slots.len(), 3),
            Scenario::QualityChanged => assert_eq!(expected, Err(Error::InvalidValue("FSCH header changed while reading prices"))),
            Scenario::MissingPrice | Scenario::SurplusPrice => {
                assert_eq!(expected, Err(Error::InvalidValue("server returned the wrong number of Read results")));
            }
            Scenario::HoldFinalHeader => unreachable!(),
        }
        if scenario == Scenario::Stable && max_pdu == 128 {
            assert!(client.stats().requests_sent > 3, "the negotiated size must split requests");
        }
        let (stream, other) = tokio::io::duplex(7);
        let peer = tokio::spawn(serve(Peer::new(scenario), other));
        assert_eq!(read_async(stream, max_pdu).await, expected);
        peer.await.unwrap();
    }
}

#[tokio::test]
async fn cancel_after_prices_before_header_confirmation() {
    let (stream, other) = tokio::io::duplex(7);
    let (held, waiting) = oneshot::channel();
    let mut peer = Peer::new(Scenario::HoldFinalHeader);
    peer.held = Some(held);
    let server = tokio::spawn(serve(peer, other));
    let read = tokio::spawn(read_async(stream, 128));
    waiting.await.unwrap();
    read.abort();
    assert!(read.await.unwrap_err().is_cancelled());
    server.await.unwrap();
}
