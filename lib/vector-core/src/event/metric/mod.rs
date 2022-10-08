#[cfg(feature = "vrl")]
use std::convert::TryFrom;
use std::{
    collections::{btree_map, BTreeMap},
    convert::AsRef,
    fmt::{self, Display, Formatter},
    sync::Arc,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use vector_common::EventDataEq;
#[cfg(feature = "vrl")]
use vrl_lib::prelude::VrlValueConvert;

use crate::{
    event::{BatchNotifier, EventFinalizer, EventFinalizers, EventMetadata, Finalizable},
    metrics::Handle,
    ByteSizeOf,
};

mod data;
pub use self::data::*;

mod series;
pub use self::series::*;

mod value;
pub use self::value::*;

pub type MetricTags = BTreeMap<String, String>;

#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
pub struct Metric {
    #[serde(flatten)]
    pub(super) series: MetricSeries,

    #[serde(flatten)]
    pub(super) data: MetricData,

    #[serde(skip_serializing, default = "EventMetadata::default")]
    metadata: EventMetadata,
}

impl Metric {
    /// Creates a new `Metric` with the given `name`, `kind`, and `value`.
    pub fn new<T: Into<String>>(name: T, kind: MetricKind, value: MetricValue) -> Self {
        Self::new_with_metadata(name, kind, value, EventMetadata::default())
    }

    /// Creates a new `Metric` with the given `name`, `kind`, `value`, and `metadata`.
    pub fn new_with_metadata<T: Into<String>>(
        name: T,
        kind: MetricKind,
        value: MetricValue,
        metadata: EventMetadata,
    ) -> Self {
        Self {
            series: MetricSeries {
                name: MetricName {
                    name: name.into(),
                    namespace: None,
                },
                tags: None,
            },
            data: MetricData {
                timestamp: None,
                kind,
                value,
            },
            metadata,
        }
    }

    /// Consumes this metric, returning it with an updated series based on the given `name`.
    #[inline]
    #[must_use]
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.series.name.name = name.into();
        self
    }

    /// Consumes this metric, returning it with an updated series based on the given `namespace`.
    #[inline]
    #[must_use]
    pub fn with_namespace<T: Into<String>>(mut self, namespace: Option<T>) -> Self {
        self.series.name.namespace = namespace.map(Into::into);
        self
    }

    /// Consumes this metric, returning it with an updated timestamp.
    #[inline]
    #[must_use]
    pub fn with_timestamp(mut self, timestamp: Option<DateTime<Utc>>) -> Self {
        self.data.timestamp = timestamp;
        self
    }

    pub fn add_finalizer(&mut self, finalizer: EventFinalizer) {
        self.metadata.add_finalizer(finalizer);
    }

    /// Consumes this metric, returning it with an updated set of event finalizers attached to `batch`.
    #[must_use]
    pub fn with_batch_notifier(mut self, batch: &Arc<BatchNotifier>) -> Self {
        self.metadata = self.metadata.with_batch_notifier(batch);
        self
    }

    /// Consumes this metric, returning it with an optionally updated set of event finalizers attached to `batch`.
    #[must_use]
    pub fn with_batch_notifier_option(mut self, batch: &Option<Arc<BatchNotifier>>) -> Self {
        self.metadata = self.metadata.with_batch_notifier_option(batch);
        self
    }

    /// Consumes this metric, returning it with an updated series based on the given `tags`.
    #[inline]
    #[must_use]
    pub fn with_tags(mut self, tags: Option<MetricTags>) -> Self {
        self.series.tags = tags;
        self
    }

    /// Consumes this metric, returning it with an updated value.
    #[inline]
    #[must_use]
    pub fn with_value(mut self, value: MetricValue) -> Self {
        self.data.value = value;
        self
    }

    /// Gets a reference to the series of this metric.
    ///
    /// The "series" is the name of the metric itself, including any tags. In other words, it is the unique identifier
    /// for a metric, although metrics of different values (counter vs gauge) may be able to co-exist in outside metrics
    /// implementations with identical series.
    pub fn series(&self) -> &MetricSeries {
        &self.series
    }

    /// Gets a reference to the data of this metric.
    pub fn data(&self) -> &MetricData {
        &self.data
    }

    /// Gets a mutable reference to the data of this metric.
    pub fn data_mut(&mut self) -> &mut MetricData {
        &mut self.data
    }

    /// Gets a reference to the metadata of this metric.
    pub fn metadata(&self) -> &EventMetadata {
        &self.metadata
    }

    /// Gets a mutable reference to the metadata of this metric.
    pub fn metadata_mut(&mut self) -> &mut EventMetadata {
        &mut self.metadata
    }

    /// Gets a reference to the name of this metric.
    ///
    /// The name of the metric does not include the namespace or tags.
    #[inline]
    pub fn name(&self) -> &str {
        &self.series.name.name
    }

    /// Gets a reference to the namespace of this metric, if it exists.
    #[inline]
    pub fn namespace(&self) -> Option<&str> {
        self.series.name.namespace.as_deref()
    }

    /// Takes the namespace out of this metric, if it exists, leaving it empty.
    #[inline]
    pub fn take_namespace(&mut self) -> Option<String> {
        self.series.name.namespace.take()
    }

    /// Gets a reference to the tags of this metric, if they exist.
    #[inline]
    pub fn tags(&self) -> Option<&MetricTags> {
        self.series.tags.as_ref()
    }

    /// Gets a reference to the timestamp of this metric, if it exists.
    #[inline]
    pub fn timestamp(&self) -> Option<DateTime<Utc>> {
        self.data.timestamp
    }

    /// Gets a reference to the value of this metric.
    #[inline]
    pub fn value(&self) -> &MetricValue {
        &self.data.value
    }

    /// Gets the kind of this metric.
    #[inline]
    pub fn kind(&self) -> MetricKind {
        self.data.kind
    }

    /// Decomposes a `Metric` into its individual parts.
    #[inline]
    pub fn into_parts(self) -> (MetricSeries, MetricData, EventMetadata) {
        (self.series, self.data, self.metadata)
    }

    /// Creates a `Metric` directly from the raw components of another metric.
    #[inline]
    pub fn from_parts(series: MetricSeries, data: MetricData, metadata: EventMetadata) -> Self {
        Self {
            series,
            data,
            metadata,
        }
    }

    /// Consumes this metric, returning it as an absolute metric.
    ///
    /// If the metric was already absolute, nothing is changed.
    #[must_use]
    pub fn into_absolute(self) -> Self {
        Self {
            series: self.series,
            data: self.data.into_absolute(),
            metadata: self.metadata,
        }
    }

    /// Consumes this metric, returning it as an incremental metric.
    ///
    /// If the metric was already incremental, nothing is changed.
    #[must_use]
    pub fn into_incremental(self) -> Self {
        Self {
            series: self.series,
            data: self.data.into_incremental(),
            metadata: self.metadata,
        }
    }

    /// Creates a new metric from components specific to a metric emitted by `metrics`.
    #[allow(clippy::cast_precision_loss)]
    pub fn from_metric_kv(key: &metrics::Key, handle: &Handle) -> Self {
        let value = match handle {
            Handle::Counter(counter) => MetricValue::Counter {
                // NOTE this will truncate if `counter.count()` is a value
                // greater than 2**52.
                value: counter.count() as f64,
            },
            Handle::Gauge(gauge) => MetricValue::Gauge {
                value: gauge.gauge(),
            },
            Handle::Histogram(histogram) => {
                let buckets: Vec<Bucket> = histogram
                    .buckets()
                    .map(|(upper_limit, count)| Bucket { upper_limit, count })
                    .collect();

                MetricValue::AggregatedHistogram {
                    buckets,
                    sum: histogram.sum() as f64,
                    count: histogram.count(),
                }
            }
        };

        let labels = key
            .labels()
            .map(|label| (String::from(label.key()), String::from(label.value())))
            .collect::<MetricTags>();

        Self::new(key.name().to_string(), MetricKind::Absolute, value)
            .with_namespace(Some("vector"))
            .with_timestamp(Some(Utc::now()))
            .with_tags(if labels.is_empty() {
                None
            } else {
                Some(labels)
            })
    }

    /// Removes a tag from this metric, returning the value of the tag if the tag was previously in the metric.
    pub fn remove_tag(&mut self, key: &str) -> Option<String> {
        self.series.remove_tag(key)
    }

    /// Returns `true` if `name` tag is present, and matches the provided `value`
    pub fn tag_matches(&self, name: &str, value: &str) -> bool {
        self.tags()
            .filter(|t| t.get(name).filter(|v| *v == value).is_some())
            .is_some()
    }

    /// Returns the string value of a tag, if it exists
    pub fn tag_value(&self, name: &str) -> Option<String> {
        self.tags().and_then(|t| t.get(name).cloned())
    }

    /// Inserts a tag into this metric.
    ///
    /// If the metric did not have this tag, `None` will be returned. Otherwise, `Some(String)` will be returned,
    /// containing the previous value of the tag.
    ///
    /// *Note:* This will create the tags map if it is not present.
    pub fn insert_tag(&mut self, name: String, value: String) -> Option<String> {
        self.series.insert_tag(name, value)
    }

    /// Gets the given tag's corresponding entry in this metric.
    ///
    /// *Note:* This will create the tags map if it is not present, even if nothing is later inserted.
    pub fn tag_entry(&mut self, key: String) -> btree_map::Entry<String, String> {
        self.series.tag_entry(key)
    }

    /// Zeroes out the data in this metric.
    pub fn zero(&mut self) {
        self.data.zero();
    }

    /// Adds the data from the `other` metric to this one.
    ///
    /// The other metric must be incremental and contain the same value type as this one.
    #[must_use]
    pub fn add(&mut self, other: impl AsRef<MetricData>) -> bool {
        self.data.add(other.as_ref())
    }

    /// Updates this metric by adding the data from `other`.
    #[must_use]
    pub fn update(&mut self, other: impl AsRef<MetricData>) -> bool {
        self.data.update(other.as_ref())
    }

    /// Subtracts the data from the `other` metric from this one.
    ///
    /// The other metric must contain the same value type as this one.
    #[must_use]
    pub fn subtract(&mut self, other: impl AsRef<MetricData>) -> bool {
        self.data.subtract(other.as_ref())
    }
}

impl AsRef<MetricData> for Metric {
    fn as_ref(&self) -> &MetricData {
        &self.data
    }
}

impl AsRef<MetricValue> for Metric {
    fn as_ref(&self) -> &MetricValue {
        &self.data.value
    }
}

impl Display for Metric {
    /// Display a metric using something like Prometheus' text format:
    ///
    /// ```text
    /// TIMESTAMP NAMESPACE_NAME{TAGS} KIND DATA
    /// ```
    ///
    /// TIMESTAMP is in ISO 8601 format with UTC time zone.
    ///
    /// KIND is either `=` for absolute metrics, or `+` for incremental
    /// metrics.
    ///
    /// DATA is dependent on the type of metric, and is a simplified
    /// representation of the data contents. In particular,
    /// distributions, histograms, and summaries are represented as a
    /// list of `X@Y` words, where `X` is the rate, count, or quantile,
    /// and `Y` is the value or bucket.
    ///
    /// example:
    /// ```text
    /// 2020-08-12T20:23:37.248661343Z vector_processed_bytes_total{component_kind="sink",component_type="blackhole"} = 6391
    /// ```
    fn fmt(&self, fmt: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        if let Some(timestamp) = &self.data.timestamp {
            write!(fmt, "{:?} ", timestamp)?;
        }
        let kind = match self.data.kind {
            MetricKind::Absolute => '=',
            MetricKind::Incremental => '+',
        };
        self.series.fmt(fmt)?;
        write!(fmt, " {} ", kind)?;
        self.data.value.fmt(fmt)
    }
}

impl EventDataEq for Metric {
    fn event_data_eq(&self, other: &Self) -> bool {
        self.series == other.series
            && self.data == other.data
            && self.metadata.event_data_eq(&other.metadata)
    }
}

impl ByteSizeOf for Metric {
    fn allocated_bytes(&self) -> usize {
        self.series.allocated_bytes()
            + self.data.allocated_bytes()
            + self.metadata.allocated_bytes()
    }
}

impl Finalizable for Metric {
    fn take_finalizers(&mut self) -> EventFinalizers {
        self.metadata.take_finalizers()
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
/// A metric may be an incremental value, updating the previous value of
/// the metric, or absolute, which sets the reference for future
/// increments.
pub enum MetricKind {
    Incremental,
    Absolute,
}

#[cfg(feature = "vrl")]
impl TryFrom<::value::Value> for MetricKind {
    type Error = String;

    fn try_from(value: ::value::Value) -> Result<Self, Self::Error> {
        let value = value.try_bytes().map_err(|e| e.to_string())?;
        match std::str::from_utf8(&value).map_err(|e| e.to_string())? {
            "incremental" => Ok(Self::Incremental),
            "absolute" => Ok(Self::Absolute),
            value => Err(format!(
                "invalid metric kind {}, metric kind must be `absolute` or `incremental`",
                value
            )),
        }
    }
}

#[cfg(feature = "vrl")]
impl From<MetricKind> for ::value::Value {
    fn from(kind: MetricKind) -> Self {
        match kind {
            MetricKind::Incremental => "incremental".into(),
            MetricKind::Absolute => "absolute".into(),
        }
    }
}

#[macro_export]
macro_rules! samples {
    ( $( $value:expr => $rate:expr ),* ) => {
        vec![ $( crate::event::metric::Sample { value: $value, rate: $rate }, )* ]
    }
}

#[macro_export]
macro_rules! buckets {
    ( $( $limit:expr => $count:expr ),* ) => {
        vec![ $( crate::event::metric::Bucket { upper_limit: $limit, count: $count }, )* ]
    }
}

#[macro_export]
macro_rules! quantiles {
    ( $( $q:expr => $value:expr ),* ) => {
        vec![ $( crate::event::metric::Quantile { quantile: $q, value: $value }, )* ]
    }
}

#[inline]
pub(crate) fn zip_samples(
    values: impl IntoIterator<Item = f64>,
    rates: impl IntoIterator<Item = u32>,
) -> Vec<Sample> {
    values
        .into_iter()
        .zip(rates.into_iter())
        .map(|(value, rate)| Sample { value, rate })
        .collect()
}

#[inline]
pub(crate) fn zip_buckets(
    limits: impl IntoIterator<Item = f64>,
    counts: impl IntoIterator<Item = u32>,
) -> Vec<Bucket> {
    limits
        .into_iter()
        .zip(counts.into_iter())
        .map(|(upper_limit, count)| Bucket { upper_limit, count })
        .collect()
}

#[inline]
pub(crate) fn zip_quantiles(
    quantiles: impl IntoIterator<Item = f64>,
    values: impl IntoIterator<Item = f64>,
) -> Vec<Quantile> {
    quantiles
        .into_iter()
        .zip(values.into_iter())
        .map(|(quantile, value)| Quantile { quantile, value })
        .collect()
}

fn write_list<I, T, W>(
    fmt: &mut Formatter<'_>,
    sep: &str,
    items: I,
    writer: W,
) -> Result<(), fmt::Error>
where
    I: IntoIterator<Item = T>,
    W: Fn(&mut Formatter<'_>, T) -> Result<(), fmt::Error>,
{
    let mut this_sep = "";
    for item in items {
        write!(fmt, "{}", this_sep)?;
        writer(fmt, item)?;
        this_sep = sep;
    }
    Ok(())
}

fn write_word(fmt: &mut Formatter<'_>, word: &str) -> Result<(), fmt::Error> {
    if word.contains(|c: char| !c.is_ascii_alphanumeric() && c != '_') {
        write!(fmt, "{:?}", word)
    } else {
        write!(fmt, "{}", word)
    }
}

pub fn samples_to_buckets(samples: &[Sample], buckets: &[f64]) -> (Vec<Bucket>, u32, f64) {
    let mut counts = vec![0; buckets.len()];
    let mut sum = 0.0;
    let mut count = 0;
    for sample in samples {
        if let Some((i, _)) = buckets
            .iter()
            .enumerate()
            .find(|&(_, b)| *b >= sample.value)
        {
            counts[i] += sample.rate;
        }

        sum += sample.value * f64::from(sample.rate);
        count += sample.rate;
    }

    let buckets = buckets
        .iter()
        .zip(counts.iter())
        .map(|(b, c)| Bucket {
            upper_limit: *b,
            count: *c,
        })
        .collect();

    (buckets, count, sum)
}

#[cfg(test)]
mod test {
    use std::collections::BTreeSet;

    use chrono::{offset::TimeZone, DateTime, Utc};
    use pretty_assertions::assert_eq;

    use super::*;

    fn ts() -> DateTime<Utc> {
        Utc.ymd(2018, 11, 14).and_hms_nano(8, 9, 10, 11)
    }

    fn tags() -> MetricTags {
        vec![
            ("normal_tag".to_owned(), "value".to_owned()),
            ("true_tag".to_owned(), "true".to_owned()),
            ("empty_tag".to_owned(), "".to_owned()),
        ]
        .into_iter()
        .collect()
    }

    #[test]
    fn merge_counters() {
        let mut counter = Metric::new(
            "counter",
            MetricKind::Incremental,
            MetricValue::Counter { value: 1.0 },
        );

        let delta = Metric::new(
            "counter",
            MetricKind::Incremental,
            MetricValue::Counter { value: 2.0 },
        )
        .with_namespace(Some("vector"))
        .with_tags(Some(tags()))
        .with_timestamp(Some(ts()));

        let expected = counter
            .clone()
            .with_value(MetricValue::Counter { value: 3.0 })
            .with_timestamp(Some(ts()));

        assert!(counter.data.add(&delta.data));
        assert_eq!(counter, expected);
    }

    #[test]
    fn merge_gauges() {
        let mut gauge = Metric::new(
            "gauge",
            MetricKind::Incremental,
            MetricValue::Gauge { value: 1.0 },
        );

        let delta = Metric::new(
            "gauge",
            MetricKind::Incremental,
            MetricValue::Gauge { value: -2.0 },
        )
        .with_namespace(Some("vector"))
        .with_tags(Some(tags()))
        .with_timestamp(Some(ts()));

        let expected = gauge
            .clone()
            .with_value(MetricValue::Gauge { value: -1.0 })
            .with_timestamp(Some(ts()));

        assert!(gauge.data.add(&delta.data));
        assert_eq!(gauge, expected);
    }

    #[test]
    fn merge_sets() {
        let mut set = Metric::new(
            "set",
            MetricKind::Incremental,
            MetricValue::Set {
                values: vec!["old".into()].into_iter().collect(),
            },
        );

        let delta = Metric::new(
            "set",
            MetricKind::Incremental,
            MetricValue::Set {
                values: vec!["new".into()].into_iter().collect(),
            },
        )
        .with_namespace(Some("vector"))
        .with_tags(Some(tags()))
        .with_timestamp(Some(ts()));

        let expected = set
            .clone()
            .with_value(MetricValue::Set {
                values: vec!["old".into(), "new".into()].into_iter().collect(),
            })
            .with_timestamp(Some(ts()));

        assert!(set.data.add(&delta.data));
        assert_eq!(set, expected);
    }

    #[test]
    fn merge_histograms() {
        let mut dist = Metric::new(
            "hist",
            MetricKind::Incremental,
            MetricValue::Distribution {
                samples: samples![1.0 => 10],
                statistic: StatisticKind::Histogram,
            },
        );

        let delta = Metric::new(
            "hist",
            MetricKind::Incremental,
            MetricValue::Distribution {
                samples: samples![1.0 => 20],
                statistic: StatisticKind::Histogram,
            },
        )
        .with_namespace(Some("vector"))
        .with_tags(Some(tags()))
        .with_timestamp(Some(ts()));

        let expected = dist
            .clone()
            .with_value(MetricValue::Distribution {
                samples: samples![1.0 => 10, 1.0 => 20],
                statistic: StatisticKind::Histogram,
            })
            .with_timestamp(Some(ts()));

        assert!(dist.data.add(&delta.data));
        assert_eq!(dist, expected);
    }

    #[test]
    fn subtract_counters() {
        // Make sure a newer/higher value counter can subtract an older/lesser value counter:
        let old_counter = Metric::new(
            "counter",
            MetricKind::Absolute,
            MetricValue::Counter { value: 4.0 },
        );

        let mut new_counter = Metric::new(
            "counter",
            MetricKind::Absolute,
            MetricValue::Counter { value: 6.0 },
        );

        assert!(new_counter.subtract(&old_counter));
        assert_eq!(new_counter.value(), &MetricValue::Counter { value: 2.0 });

        // But not the other way around:
        let old_counter = Metric::new(
            "counter",
            MetricKind::Absolute,
            MetricValue::Counter { value: 6.0 },
        );

        let mut new_reset_counter = Metric::new(
            "counter",
            MetricKind::Absolute,
            MetricValue::Counter { value: 1.0 },
        );

        assert!(!new_reset_counter.subtract(&old_counter));
    }

    #[test]
    fn subtract_aggregated_histograms() {
        // Make sure a newer/higher count aggregated histogram can subtract an older/lower count
        // aggregated histogram:
        let old_histogram = Metric::new(
            "histogram",
            MetricKind::Absolute,
            MetricValue::AggregatedHistogram {
                count: 1,
                sum: 1.0,
                buckets: buckets!(2.0 => 1),
            },
        );

        let mut new_histogram = Metric::new(
            "histogram",
            MetricKind::Absolute,
            MetricValue::AggregatedHistogram {
                count: 3,
                sum: 3.0,
                buckets: buckets!(2.0 => 3),
            },
        );

        assert!(new_histogram.subtract(&old_histogram));
        assert_eq!(
            new_histogram.value(),
            &MetricValue::AggregatedHistogram {
                count: 2,
                sum: 2.0,
                buckets: buckets!(2.0 => 2),
            }
        );

        // But not the other way around:
        let old_histogram = Metric::new(
            "histogram",
            MetricKind::Absolute,
            MetricValue::AggregatedHistogram {
                count: 3,
                sum: 3.0,
                buckets: buckets!(2.0 => 3),
            },
        );

        let mut new_reset_histogram = Metric::new(
            "histogram",
            MetricKind::Absolute,
            MetricValue::AggregatedHistogram {
                count: 1,
                sum: 1.0,
                buckets: buckets!(2.0 => 1),
            },
        );

        assert!(!new_reset_histogram.subtract(&old_histogram));
    }

    #[test]
    // `too_many_lines` is mostly just useful for production code but we're not
    // able to flag the lint on only for non-test.
    #[allow(clippy::too_many_lines)]
    fn display() {
        assert_eq!(
            format!(
                "{}",
                Metric::new(
                    "one",
                    MetricKind::Absolute,
                    MetricValue::Counter { value: 1.23 },
                )
                .with_tags(Some(tags()))
            ),
            r#"one{empty_tag="",normal_tag="value",true_tag="true"} = 1.23"#
        );

        assert_eq!(
            format!(
                "{}",
                Metric::new(
                    "two word",
                    MetricKind::Incremental,
                    MetricValue::Gauge { value: 2.0 }
                )
                .with_timestamp(Some(ts()))
            ),
            r#"2018-11-14T08:09:10.000000011Z "two word"{} + 2"#
        );

        assert_eq!(
            format!(
                "{}",
                Metric::new(
                    "namespace",
                    MetricKind::Absolute,
                    MetricValue::Counter { value: 1.23 },
                )
                .with_namespace(Some("vector"))
            ),
            r#"vector_namespace{} = 1.23"#
        );

        assert_eq!(
            format!(
                "{}",
                Metric::new(
                    "namespace",
                    MetricKind::Absolute,
                    MetricValue::Counter { value: 1.23 },
                )
                .with_namespace(Some("vector host"))
            ),
            r#""vector host"_namespace{} = 1.23"#
        );

        let mut values = BTreeSet::<String>::new();
        values.insert("v1".into());
        values.insert("v2_two".into());
        values.insert("thrəë".into());
        values.insert("four=4".into());
        assert_eq!(
            format!(
                "{}",
                Metric::new("three", MetricKind::Absolute, MetricValue::Set { values })
            ),
            r#"three{} = "four=4" "thrəë" v1 v2_two"#
        );

        assert_eq!(
            format!(
                "{}",
                Metric::new(
                    "four",
                    MetricKind::Absolute,
                    MetricValue::Distribution {
                        samples: samples![1.0 => 3, 2.0 => 4],
                        statistic: StatisticKind::Histogram,
                    }
                )
            ),
            r#"four{} = histogram 3@1 4@2"#
        );

        assert_eq!(
            format!(
                "{}",
                Metric::new(
                    "five",
                    MetricKind::Absolute,
                    MetricValue::AggregatedHistogram {
                        buckets: buckets![51.0 => 53, 52.0 => 54],
                        count: 107,
                        sum: 103.0,
                    }
                )
            ),
            r#"five{} = count=107 sum=103 53@51 54@52"#
        );

        assert_eq!(
            format!(
                "{}",
                Metric::new(
                    "six",
                    MetricKind::Absolute,
                    MetricValue::AggregatedSummary {
                        quantiles: quantiles![1.0 => 63.0, 2.0 => 64.0],
                        count: 2,
                        sum: 127.0,
                    }
                )
            ),
            r#"six{} = count=2 sum=127 1@63 2@64"#
        );
    }

    #[test]
    fn quantile_to_percentile_string() {
        let quantiles = [
            (-1.0, "0"),
            (0.0, "0"),
            (0.25, "25"),
            (0.50, "50"),
            (0.999, "999"),
            (0.9999, "9999"),
            (0.99999, "9999"),
            (1.0, "100"),
            (3.0, "100"),
        ];

        for (quantile, expected) in quantiles {
            let quantile = Quantile {
                quantile,
                value: 1.0,
            };
            let result = quantile.to_percentile_string();
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn quantile_to_string() {
        let quantiles = [
            (-1.0, "0"),
            (0.0, "0"),
            (0.25, "0.25"),
            (0.50, "0.5"),
            (0.999, "0.999"),
            (0.9999, "0.9999"),
            (0.99999, "0.9999"),
            (1.0, "1"),
            (3.0, "1"),
        ];

        for (quantile, expected) in quantiles {
            let quantile = Quantile {
                quantile,
                value: 1.0,
            };
            let result = quantile.to_quantile_string();
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn value_conversions() {
        let counter_value = MetricValue::Counter { value: 3.13 };
        assert_eq!(counter_value.distribution_to_agg_histogram(&[1.0]), None);

        let counter_value = MetricValue::Counter { value: 3.13 };
        assert_eq!(counter_value.distribution_to_sketch(), None);

        let distrib_value = MetricValue::Distribution {
            samples: samples!(1.0 => 10, 2.0 => 5, 5.0 => 2),
            statistic: StatisticKind::Summary,
        };
        let converted = distrib_value.distribution_to_agg_histogram(&[1.0, 5.0, 10.0]);
        assert_eq!(
            converted,
            Some(MetricValue::AggregatedHistogram {
                buckets: vec![
                    Bucket {
                        upper_limit: 1.0,
                        count: 10,
                    },
                    Bucket {
                        upper_limit: 5.0,
                        count: 7,
                    },
                    Bucket {
                        upper_limit: 10.0,
                        count: 0,
                    },
                ],
                sum: 30.0,
                count: 17,
            })
        );

        let distrib_value = MetricValue::Distribution {
            samples: samples!(1.0 => 1),
            statistic: StatisticKind::Summary,
        };
        let converted = distrib_value.distribution_to_sketch();
        assert!(matches!(converted, Some(MetricValue::Sketch { .. })));
    }
}
