//! Read a fixed-start FSCH tariff for one settlement day.

use std::io::{Read, Write};
use std::ops::{ControlFlow, Range};

use super::Client;
use crate::common::{Error, Fc, ObjectReference, Result, UtcTime};
use crate::proto::data::{Typed, Value};

/// Object names, settings constraint and price unit for one engineered FSCH instance.
#[derive(Clone, Copy, Debug)]
pub struct TariffProfile<'a> {
    /// Functional constraint for setting values, either SP or active SG.
    pub settings_fc: Fc,
    /// Absolute UTC start object, e.g. `StrTm` or `StrTm1`. Reads its `setTm` attribute.
    pub start_object: &'a str,
    /// First numbered price object, e.g. `ValASG001` or `ValING1`. Padding is preserved.
    pub first_price: &'a str,
    /// Unit of each numeric price as agreed with the provider, e.g. `ct/kWh`.
    pub price_unit: &'a str,
}

impl<'a> TariffProfile<'a> {
    /// Use SP settings, `StrTm1.setTm` and `ValASG001.setMag.f` for the supplied price unit.
    pub const fn new(price_unit: &'a str) -> Self {
        Self { settings_fc: Fc::SP, start_object: "StrTm1", first_price: "ValASG001", price_unit }
    }
}

/// A complete schedule in the unit supplied by the tariff contract.
#[derive(Clone, Debug, PartialEq)]
pub struct TariffSchedule {
    /// Provider's absolute start timestamp, including its original clock quality.
    pub start: UtcTime,
    /// Contracted unit, without conversion.
    pub price_unit: String,
    /// Consecutive delivery intervals in chronological order.
    pub slots: Vec<TariffSlot>,
}

/// A tariff valid from the inclusive start to the exclusive end, in Unix seconds.
#[derive(Clone, Debug, PartialEq)]
pub struct TariffSlot {
    /// UTC delivery interval.
    pub utc: Range<u32>,
    /// Price in the schedule's unit.
    pub price: f64,
}

const MAX_ENTRIES: u32 = 25 * 60;

struct PriceSeries<'a> {
    family: &'a str,
    width: usize,
    attribute: &'static str,
}

impl<'a> PriceSeries<'a> {
    fn parse(first: &'a str) -> Result<Self> {
        let (family, suffix) = first.split_at_checked(6).ok_or(Error::InvalidReference("expected ValASG1 or ValING1 with optional zero padding"))?;
        let attribute = match family {
            "ValASG" => "setMag.f",
            "ValING" => "setVal",
            _ => return Err(Error::InvalidReference("tariff values must use ValASG or ValING")),
        };
        if suffix.len() > 10 || suffix.trim_start_matches('0') != "1" {
            return Err(Error::InvalidReference("expected first price instance 1 with optional zero padding"));
        }
        Ok(Self { family, width: suffix.len(), attribute })
    }

    fn price(&self, value: &Value) -> Result<f64> {
        match self.family {
            "ValASG" => value.as_f64().filter(|p| p.is_finite()).ok_or(Error::InvalidValue("FSCH setMag.f needs a finite floating-point price")),
            _ => value.as_i64().and_then(|i| i32::try_from(i).ok()).map(f64::from).ok_or(Error::InvalidValue("FSCH setVal needs an INT32 price")),
        }
    }
}

/// Convert an IEC 61850 Unit structure and positive ING setting to whole seconds.
fn interval_seconds(value: &Value, units: &Value) -> Result<u32> {
    const INVALID: Error = Error::InvalidValue("SchdIntv must specify a positive whole number of seconds");
    let amount = value.as_i64().and_then(|i| i32::try_from(i).ok()).and_then(|i| u64::try_from(i).ok()).ok_or(INVALID)?;
    let Value::Structure(fields) = units else { return Err(Error::InvalidValue("SchdIntv.units must be a Unit structure")) };
    let (unit, power) = match fields.as_slice() {
        [unit] => (unit, 0),
        [unit, multiplier] => (unit, multiplier.as_i64().ok_or(Error::InvalidValue("invalid SchdIntv.units.multiplier"))?),
        _ => return Err(Error::InvalidValue("expected SIUnit and optional multiplier")),
    };
    let seconds = match unit.as_i64() {
        Some(4) => 1,
        Some(84) => 3600,
        Some(85) => 60,
        _ => return Err(Error::InvalidValue("SchdIntv.units.SIUnit must be seconds, minutes or hours")),
    };
    if !(-24..=24).contains(&power) || (power.abs() > 2 && power % 3 != 0) {
        return Err(Error::InvalidValue("unknown SchdIntv.units.multiplier"));
    }
    let scale = u32::try_from(power.unsigned_abs()).ok().and_then(|p| 10_u64.checked_pow(p)).ok_or(INVALID)?;
    let base = amount.checked_mul(seconds).ok_or(INVALID)?;
    let scaled = if power < 0 {
        if !base.is_multiple_of(scale) {
            return Err(INVALID);
        }
        base / scale
    } else {
        base.checked_mul(scale).ok_or(INVALID)?
    };
    u32::try_from(scaled).ok().filter(|&s| s > 0).ok_or(INVALID)
}

fn dimensions(header: &[Value], window: &Range<u32>) -> Result<(u32, u32, UtcTime)> {
    let [count, interval, units, start] = header else { return Err(Error::InvalidValue("incomplete FSCH header")) };
    let count = count
        .as_i64()
        .and_then(|n| i32::try_from(n).ok())
        .and_then(|n| u32::try_from(n).ok())
        .filter(|&n| n > 0)
        .ok_or(Error::InvalidValue("NumEntr must be a positive INT32"))?;
    if count > MAX_ENTRIES {
        return Err(Error::LimitExceeded { limit: "daily FSCH tariff entries (1500)", value: count as usize });
    }
    let seconds = interval_seconds(interval, units)?;
    let start = start.as_utc_time().ok_or(Error::InvalidValue("FSCH start must be an absolute UTC timestamp"))?;
    if start.seconds != window.start || start.fraction != 0 || count.checked_mul(seconds).and_then(|span| start.seconds.checked_add(span)) != Some(window.end) {
        return Err(Error::InvalidValue("FSCH schedule does not cover the requested UTC window"));
    }
    Ok((count, seconds, start))
}

/// A transport-independent FSCH read. Dropping it discards the unfinished schedule.
pub struct TariffRead<'a> {
    node: &'a str,
    profile: TariffProfile<'a>,
    window: Range<u32>,
    series: PriceSeries<'a>,
    phase: Phase,
    batch_limit: usize,
    header_part: Vec<Value>,
    slots: Vec<TariffSlot>,
}

enum Phase {
    Header,
    Prices { header: Vec<Value>, count: usize, seconds: u32, start: UtcTime },
    Verify { header: Vec<Value>, start: UtcTime },
}

impl<'a> TariffRead<'a> {
    /// Begin a read for the provider's 23–25 hour delivery day.
    pub fn new(node: &'a str, profile: TariffProfile<'a>, window: Range<u32>) -> Result<Self> {
        let fc = profile.settings_fc;
        if !matches!(fc, Fc::SP | Fc::SG) {
            return Err(Error::InvalidValue("FSCH settings require SP or active SG"));
        }
        let reference = ObjectReference::parse(node)?;
        if node.contains(['.', '$']) || !reference.ln.trim_end_matches(|c: char| c.is_ascii_digit()).ends_with("FSCH") {
            return Err(Error::InvalidReference("expected an LD/FSCH logical node"));
        }
        if !profile.start_object.strip_prefix("StrTm").is_some_and(|s| s.bytes().all(|b| b.is_ascii_digit())) {
            return Err(Error::InvalidReference("expected StrTm with optional instance number"));
        }
        if profile.price_unit.trim().is_empty() || profile.price_unit.chars().any(char::is_control) {
            return Err(Error::InvalidValue("supply a single-line tariff price unit"));
        }
        if !(23 * 3600..=25 * 3600).contains(&window.end.saturating_sub(window.start)) {
            return Err(Error::InvalidValue("expected a delivery window of 23 to 25 hours in UTC seconds"));
        }
        let series = PriceSeries::parse(profile.first_price)?;
        Ok(Self { node, profile, window, series, phase: Phase::Header, batch_limit: 128, header_part: Vec::new(), slots: Vec::new() })
    }

    /// Cap the number of fields per MMS Read. A zero limit permits a single field.
    #[must_use]
    pub fn with_batch_limit(mut self, limit: usize) -> Self {
        self.batch_limit = limit.clamp(1, 128);
        self
    }

    fn field_count(&self) -> usize {
        let remaining = match self.phase {
            Phase::Prices { count, .. } => count - self.slots.len(),
            _ => 4 - self.header_part.len(),
        };
        remaining.min(self.batch_limit)
    }

    /// Ordered fields for the next MMS Read. Supply its outcome to [`Self::accept`].
    pub fn fields(&self) -> Vec<(String, Fc)> {
        let node = self.node;
        let fc = self.profile.settings_fc;
        if matches!(self.phase, Phase::Prices { .. }) {
            let first = self.slots.len() + 1;
            let series = &self.series;
            return (first..first + self.field_count())
                .map(|i| (format!("{node}.{}{i:0width$}.{}", series.family, series.attribute, width = series.width), fc))
                .collect();
        }
        [
            (format!("{node}.NumEntr.setVal"), fc),
            (format!("{node}.SchdIntv.setVal"), fc),
            (format!("{node}.SchdIntv.units"), Fc::CF),
            (format!("{node}.{}.setTm", self.profile.start_object), fc),
        ]
        .into_iter()
        .skip(self.header_part.len())
        .take(self.field_count())
        .collect()
    }

    /// Supply the outcome of one request in field order. A local `peer max PDU` error
    /// before transmission retries a smaller batch. Only the closing header completes the read.
    pub fn accept(mut self, response: Result<Vec<Value>>) -> Result<ControlFlow<TariffSchedule, Self>> {
        let count = self.field_count();
        let values = match response {
            Err(Error::LimitExceeded { limit: "peer max PDU", .. }) if count > 1 => {
                self.batch_limit = count / 2;
                return Ok(ControlFlow::Continue(self));
            }
            result => result?,
        };
        if values.len() != count {
            return Err(Error::InvalidValue("server returned the wrong number of Read results"));
        }
        let values = if matches!(self.phase, Phase::Header | Phase::Verify { .. }) && count < 4 {
            self.header_part.extend(values);
            if self.header_part.len() < 4 {
                return Ok(ControlFlow::Continue(self));
            }
            std::mem::take(&mut self.header_part)
        } else {
            values
        };
        self.phase = match self.phase {
            Phase::Header => {
                let (count, seconds, start) = dimensions(&values, &self.window)?;
                self.slots.reserve(count as usize);
                Phase::Prices { header: values, count: count as usize, seconds, start }
            }
            Phase::Prices { header, count, seconds, start } => {
                let mut begin = self.slots.last().map_or(start.seconds, |slot| slot.utc.end);
                for value in values {
                    let end = begin + seconds;
                    self.slots.push(TariffSlot { utc: begin..end, price: self.series.price(&value)? });
                    begin = end;
                }
                if self.slots.len() == count { Phase::Verify { header, start } } else { Phase::Prices { header, count, seconds, start } }
            }
            Phase::Verify { header, start } => {
                if values != header {
                    return Err(Error::InvalidValue("FSCH header changed while reading prices"));
                }
                return Ok(ControlFlow::Break(TariffSchedule { start, price_unit: self.profile.price_unit.into(), slots: self.slots }));
            }
        };
        Ok(ControlFlow::Continue(self))
    }
}

impl<S: Read + Write> Client<S> {
    /// Read a fixed-start tariff for the provider's local day. Prices must remain stable during the read.
    pub fn read_tariff_schedule(&mut self, node: &str, profile: TariffProfile<'_>, window: Range<u32>) -> Result<TariffSchedule> {
        let mut read = TariffRead::new(node, profile, window)?.with_batch_limit(self.limits.max_dataset_members);
        loop {
            let fields = read.fields();
            let refs: Vec<_> = fields.iter().map(|(name, fc)| (name.as_str(), *fc)).collect();
            match read.accept(self.read_many(&refs))? {
                ControlFlow::Continue(next) => read = next,
                ControlFlow::Break(day) => return Ok(day),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tariff_intervals_use_the_wire_unit_and_multiplier() {
        let convert = |n, fields: &[i64]| interval_seconds(&Value::Integer(n), &Value::Structure(fields.iter().copied().map(Value::Integer).collect()));
        assert_eq!(convert(1, &[84]), Ok(3600));
        assert_eq!(convert(15, &[85]), Ok(900));
        assert_eq!(convert(900_000, &[4, -3]), Ok(900));
        for (n, fields) in [
            (1, vec![4, -3]),
            (0, vec![4]),
            (-1, vec![85]),
            (1, vec![2]),
            (1, vec![4, 4]),
            (i64::MAX, vec![84]),
            (1, vec![4, i64::MIN]),
            (1, vec![]),
            (1, vec![4, 0, 0]),
        ] {
            assert!(convert(n, &fields).is_err());
        }
    }
}
