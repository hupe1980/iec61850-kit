//! Read an engineered FSCH node through the public MMS client and CLI.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use iec61850_kit::client::{Client, TariffProfile};
use iec61850_kit::proto::data::Value;
use iec61850_kit::server::{Ied, Server, ServerHandle};
use iec61850_kit::{Error, Fc, TimeQuality, UtcTime};
use std::fmt::Write as _;

const NODE: &str = "IED1LD0/GridFSCH1";
const PROFILE: TariffProfile<'static> = TariffProfile::new("ct/kWh");

fn relay(start: u32, count: i32, minutes: i32, clients: usize) -> (String, ServerHandle) {
    relay_with_fc(start, count, minutes, clients, Fc::SP)
}

fn relay_with_fc(start: u32, count: i32, minutes: i32, clients: usize, fc: Fc) -> (String, ServerHandle) {
    let mut objects = String::new();
    for n in 1..=100 {
        write!(objects, r#"<DO name="ValASG{n:03}" type="Price"/><DO name="ValING{n}" type="Int"/>"#).unwrap();
    }
    let types = format!(
        r#"<LNodeType id="Schedule" lnClass="FSCH"><DO name="NumEntr" type="Int"/><DO name="SchdIntv" type="Interval"/>
        <DO name="StrTm1" type="Start"/>{objects}</LNodeType>
        <DOType id="Int" cdc="ING"><DA name="setVal" bType="INT32" fc="{fc}"/></DOType>
        <DOType id="Interval" cdc="ING"><DA name="setVal" bType="INT32" fc="{fc}"/><DA name="units" bType="Struct" type="Units" fc="CF"/></DOType>
        <DAType id="Units"><BDA name="SIUnit" bType="Enum" type="Unit"/></DAType>
        <EnumType id="Unit"><EnumVal ord="85">min</EnumVal></EnumType>
        <DOType id="Start" cdc="TSG"><DA name="setTm" bType="Timestamp" fc="{fc}"/></DOType>
        <DOType id="Price" cdc="ASG"><DA name="setMag" fc="{fc}" bType="Struct" type="AnalogueValue_T"/></DOType>"#
    );
    let xml = include_str!("fixtures/relay.icd")
        .replace("</LDevice>", r#"<LN lnClass="FSCH" prefix="Grid" inst="1" lnType="Schedule"/></LDevice>"#)
        .replace("</DataTypeTemplates>", &format!("{types}</DataTypeTemplates>"));
    let mut ied = Ied::from_scl(&xml, None).unwrap();
    let mut set = |item: &str, v| ied.set_internal(&format!("{NODE}${item}"), v).unwrap();
    set(&format!("{fc}$NumEntr$setVal"), Value::Integer(count.into()));
    set(&format!("{fc}$SchdIntv$setVal"), Value::Integer(minutes.into()));
    set("CF$SchdIntv$units$SIUnit", Value::Integer(85));
    set(&format!("{fc}$StrTm1$setTm"), Value::UtcTime(UtcTime::from_unix(start, 0, TimeQuality::SYNCHRONIZED)));
    for n in 1_i16..=100 {
        set(&format!("{fc}$ValASG{n:03}$setMag$f"), Value::Float32(f32::from(n) - 2.0));
        set(&format!("{fc}$ValING{n}$setVal"), Value::Integer(i64::from(n) - 2));
    }
    let server = Server::bind("127.0.0.1:0", ied).unwrap();
    let addr = server.local_addr().unwrap().to_string();
    let handle = server.handle();
    std::thread::spawn(move || {
        for _ in 0..clients {
            server.accept_one().unwrap();
        }
    });
    (addr, handle)
}

#[test]
fn tariff_covers_the_requested_day_in_order() {
    // Midnight-to-midnight UTC boundaries of Berlin's spring and autumn transitions in 2026.
    for (start, count, minutes) in [(1_774_738_800, 92, 15), (1_792_879_200, 100, 15), (86_400, 96, 15), (86_400, 24, 60)] {
        let window = start..start + u32::try_from(count * minutes * 60).unwrap();
        let (addr, _) = relay(start, count, minutes, 1);
        let mut client = Client::connect(&addr).unwrap();
        for first in ["ValASG001", "ValING1"] {
            let schedule = client.read_tariff_schedule(NODE, TariffProfile { first_price: first, ..PROFILE }, window.clone()).unwrap();
            assert_eq!(schedule.price_unit, "ct/kWh");
            assert_eq!(schedule.slots.len(), usize::try_from(count).unwrap());
            for (n, slot) in schedule.slots.iter().enumerate() {
                let step = u32::try_from(minutes * 60).unwrap();
                let begin = start + u32::try_from(n).unwrap() * step;
                assert_eq!(slot.utc, begin..begin + step);
                assert_eq!(slot.price.to_bits(), (f64::from(u32::try_from(n).unwrap()) - 1.0).to_bits());
            }
        }
    }
}

#[test]
fn tariff_rejects_bad_metadata_and_prices() {
    let (addr, handle) = relay(0, 96, 15, 1);
    let mut client = Client::connect(&addr).unwrap();
    let mut read = || client.read_tariff_schedule(NODE, PROFILE, 0..86_400);
    let set = |item: &str, value| assert_eq!(handle.txn().set(&format!("{NODE}${item}"), value).commit(), [Ok(())]);
    set("SP$NumEntr$setVal", Value::Integer(i32::MAX.into()));
    assert!(matches!(read(), Err(Error::LimitExceeded { .. })));
    set("SP$NumEntr$setVal", Value::Integer(95));
    assert!(matches!(read(), Err(Error::InvalidValue(_))));
    set("SP$NumEntr$setVal", Value::Integer(96));
    set("SP$ValASG096$setMag$f", Value::Float32(f32::NAN));
    assert!(matches!(read(), Err(Error::InvalidValue(_))));
}

#[cfg(feature = "cli")]
#[test]
fn tariff_cli_emits_only_complete_tsv() {
    let (addr, _) = relay(0, 24, 60, 2);
    let run = |end| {
        std::process::Command::new(env!("CARGO_BIN_EXE_ied"))
            .args(["mms", "tariff", &addr, NODE, "0", end, "ct/kWh", "--first-price", "ValING1"])
            .output()
            .unwrap()
    };
    let ok = run("86400");
    assert!(ok.status.success(), "{}", String::from_utf8_lossy(&ok.stderr));
    let out = String::from_utf8(ok.stdout).unwrap();
    assert_eq!(out.lines().count(), 25);
    assert_eq!(out.lines().nth(1), Some("0\t3600\t-1\tct/kWh"));
    assert_eq!(out.lines().last(), Some("82800\t86400\t22\tct/kWh"));
    let wrong = run("90000");
    assert!(!wrong.status.success());
    assert_eq!(wrong.stdout, b"");
}

#[test]
fn tariff_rechecks_metadata_after_price_reads() {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    struct UpdatingPeer {
        tcp: TcpStream,
        handle: ServerHandle,
    }
    impl Read for UpdatingPeer {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.tcp.read(buf)
        }
    }
    impl Write for UpdatingPeer {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if buf.windows(6).any(|name| name == b"ValASG") {
                assert_eq!(self.handle.txn().set(&format!("{NODE}$SP$NumEntr$setVal"), Value::Integer(23)).commit(), [Ok(())]);
            }
            self.tcp.write(buf)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.tcp.flush()
        }
    }
    let (addr, handle) = relay(0, 24, 60, 1);
    let socket = UpdatingPeer { tcp: TcpStream::connect(addr).unwrap(), handle };
    let mut client = Client::with_transport(socket, &iec61850_kit::client::ClientConfig::default(), |s, t| s.tcp.set_read_timeout(Some(t))).unwrap();
    assert_eq!(client.read_tariff_schedule(NODE, PROFILE, 0..86_400), Err(Error::InvalidValue("FSCH header changed while reading prices")));
}

#[test]
fn tariff_reads_split_to_fit_the_negotiated_pdu() {
    let (addr, _) = relay(0, 96, 15, 1);
    let mut config = iec61850_kit::client::ClientConfig::default();
    config.association.max_pdu = 224;
    let mut client = Client::connect_with(&addr, &config).unwrap();
    let schedule = client.read_tariff_schedule(NODE, PROFILE, 0..86_400).unwrap();
    assert_eq!(schedule.slots.len(), 96);
    assert_eq!(schedule.slots[95].utc, 85_500..86_400);
}

#[test]
fn tariff_reads_active_group_settings() {
    let (addr, _) = relay_with_fc(0, 24, 60, 1, Fc::SG);
    let mut client = Client::connect(&addr).unwrap();
    let day = client.read_tariff_schedule(NODE, TariffProfile { settings_fc: Fc::SG, ..PROFILE }, 0..86_400).unwrap();
    assert_eq!(day.slots.len(), 24);
    assert_eq!(day.slots[0].price.to_bits(), (-1.0_f64).to_bits());
}
