//! Per-track fan-out and cache.
//!
//! A [`Track`] is written by exactly one upstream subscription and read by any
//! number of downstream subscriptions. Every reader proceeds at its own pace:
//! a slow subscriber never blocks the publisher or other subscribers, it simply
//! skips to the newest group once it falls behind the cache window (live media
//! prefers latency over completeness).
//!
//! The cache keeps the subgroups of the most recent groups, so a late joiner
//! starts at the beginning of the current group (for video: the last keyframe)
//! instead of waiting for the next one.

use std::collections::VecDeque;
use std::sync::Arc;

use moqi_proto::data::{Object, ObjectDatagram, SubgroupHeader};
use moqi_proto::types::{FullTrackName, Location};
use tokio::sync::{broadcast, watch};

/// How many groups behind the largest one stay cached.
const CACHE_GROUPS: u64 = 1;
/// Hard cap on cached subgroups regardless of group span.
const CACHE_SUBGROUPS: usize = 32;
const DATAGRAM_BACKLOG: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubgroupEnd {
    /// Stream finished cleanly: all objects were delivered.
    Fin,
    /// Stream was reset upstream: objects may be missing.
    Reset,
}

#[derive(Default)]
struct SubgroupState {
    objects: Vec<Object>,
    end: Option<SubgroupEnd>,
}

/// One subgroup stream's worth of objects, appended as they arrive.
pub struct Subgroup {
    /// Header as received upstream; the track alias is rewritten per session.
    pub header: SubgroupHeader,
    state: watch::Sender<SubgroupState>,
}

impl Subgroup {
    pub fn push(&self, object: Object) {
        self.state.send_modify(|s| s.objects.push(object));
    }

    pub fn finish(&self, end: SubgroupEnd) {
        self.state.send_modify(|s| {
            s.end.get_or_insert(end);
        });
    }

    pub fn reader(self: &Arc<Self>) -> SubgroupReader {
        SubgroupReader { _subgroup: self.clone(), rx: self.state.subscribe(), index: 0 }
    }
}

impl Drop for Subgroup {
    fn drop(&mut self) {
        self.finish(SubgroupEnd::Reset);
    }
}

pub struct SubgroupReader {
    _subgroup: Arc<Subgroup>,
    rx: watch::Receiver<SubgroupState>,
    index: usize,
}

impl SubgroupReader {
    /// Next object in order, or the way the subgroup ended.
    pub async fn next(&mut self) -> Result<Object, SubgroupEnd> {
        loop {
            {
                let state = self.rx.borrow_and_update();
                if let Some(obj) = state.objects.get(self.index) {
                    self.index += 1;
                    return Ok(obj.clone());
                }
                if let Some(end) = state.end {
                    return Err(end);
                }
            }
            if self.rx.changed().await.is_err() {
                return Err(SubgroupEnd::Reset);
            }
        }
    }
}

struct TrackState {
    /// (sequence number, subgroup), oldest first.
    subgroups: VecDeque<(u64, Arc<Subgroup>)>,
    next_seq: u64,
    largest: Option<Location>,
    /// PUBLISH_DONE status once the upstream subscription has ended.
    ended: Option<u64>,
}

pub struct Track {
    pub name: FullTrackName,
    state: watch::Sender<TrackState>,
    datagrams: broadcast::Sender<Arc<ObjectDatagram>>,
}

impl Track {
    pub fn new(name: FullTrackName) -> Arc<Self> {
        let state = TrackState { subgroups: VecDeque::new(), next_seq: 0, largest: None, ended: None };
        Arc::new(Self {
            name,
            state: watch::Sender::new(state),
            datagrams: broadcast::Sender::new(DATAGRAM_BACKLOG),
        })
    }

    pub fn largest(&self) -> Option<Location> {
        self.state.borrow().largest
    }

    fn bump_largest(s: &mut TrackState, loc: Location) {
        if s.largest.is_none_or(|l| loc > l) {
            s.largest = Some(loc);
        }
    }

    // ---- writer side (upstream) ----

    pub fn create_subgroup(&self, header: SubgroupHeader) -> Arc<Subgroup> {
        let subgroup = Arc::new(Subgroup { header, state: watch::Sender::new(SubgroupState::default()) });
        self.state.send_modify(|s| {
            let seq = s.next_seq;
            s.next_seq += 1;
            s.subgroups.push_back((seq, subgroup.clone()));
            let newest = s.subgroups.iter().map(|(_, sg)| sg.header.group_id).max().unwrap_or(0);
            while let Some((_, front)) = s.subgroups.front() {
                let stale = front.header.group_id + CACHE_GROUPS < newest;
                if stale || s.subgroups.len() > CACHE_SUBGROUPS {
                    s.subgroups.pop_front();
                } else {
                    break;
                }
            }
        });
        subgroup
    }

    pub fn push_object(&self, subgroup: &Subgroup, object: Object) {
        let loc = Location::new(subgroup.header.group_id, object.id);
        self.state.send_if_modified(|s| {
            Self::bump_largest(s, loc);
            false // largest alone doesn't need to wake readers
        });
        subgroup.push(object);
    }

    pub fn push_datagram(&self, datagram: ObjectDatagram) {
        let loc = Location::new(datagram.group_id, datagram.object.id);
        self.state.send_if_modified(|s| {
            Self::bump_largest(s, loc);
            false
        });
        // No receivers is fine: nobody is listening right now.
        let _ = self.datagrams.send(Arc::new(datagram));
    }

    pub fn end(&self, status: u64) {
        self.state.send_modify(|s| {
            s.ended.get_or_insert(status);
        });
    }

    // ---- reader side (downstream) ----

    /// A reader positioned at the first cached subgroup of the latest group.
    pub fn reader(self: &Arc<Self>) -> TrackReader {
        let rx = self.state.subscribe();
        let next_seq = {
            let s = rx.borrow();
            let latest_group = s.subgroups.iter().map(|(_, sg)| sg.header.group_id).max();
            s.subgroups
                .iter()
                .find(|(_, sg)| Some(sg.header.group_id) == latest_group)
                .map(|(seq, _)| *seq)
                .unwrap_or(s.next_seq)
        };
        TrackReader { rx, next_seq, datagrams: self.datagrams.subscribe() }
    }
}

pub enum TrackEvent {
    Subgroup(Arc<Subgroup>),
    Datagram(Arc<ObjectDatagram>),
    /// Upstream finished with this PUBLISH_DONE status.
    Ended(u64),
}

pub struct TrackReader {
    rx: watch::Receiver<TrackState>,
    next_seq: u64,
    datagrams: broadcast::Receiver<Arc<ObjectDatagram>>,
}

impl TrackReader {
    pub async fn next(&mut self) -> TrackEvent {
        loop {
            {
                let s = self.rx.borrow_and_update();
                // Anything older than `next_seq` that was evicted is skipped.
                if let Some((seq, sg)) = s.subgroups.iter().find(|(seq, _)| *seq >= self.next_seq) {
                    self.next_seq = seq + 1;
                    return TrackEvent::Subgroup(sg.clone());
                }
                if let Some(status) = s.ended {
                    return TrackEvent::Ended(status);
                }
            }
            tokio::select! {
                changed = self.rx.changed() => {
                    if changed.is_err() {
                        return TrackEvent::Ended(moqi_proto::message::publish_done::INTERNAL_ERROR);
                    }
                }
                dg = self.datagrams.recv() => match dg {
                    Ok(dg) => return TrackEvent::Datagram(dg),
                    // Lagging on datagrams just means drops, same as the network would.
                    Err(broadcast::error::RecvError::Lagged(n)) => tracing::debug!(n, "datagram reader lagged"),
                    Err(broadcast::error::RecvError::Closed) => {}
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use moqi_proto::data::SubgroupIdMode;
    use moqi_proto::types::{Properties, TrackNamespace};

    fn header(group_id: u64) -> SubgroupHeader {
        SubgroupHeader {
            track_alias: 0,
            group_id,
            subgroup: SubgroupIdMode::Zero,
            publisher_priority: None,
            has_properties: false,
            end_of_group: true,
            first_object: true,
        }
    }

    fn obj(id: u64) -> Object {
        Object::normal(id, Properties::default(), Bytes::from(vec![id as u8]))
    }

    #[tokio::test]
    async fn late_joiner_starts_at_latest_group() {
        let track = Track::new(FullTrackName::new(TrackNamespace::new(["a"]), "v"));
        let g0 = track.create_subgroup(header(0));
        track.push_object(&g0, obj(0));
        let g1 = track.create_subgroup(header(1));
        track.push_object(&g1, obj(0));
        track.push_object(&g1, obj(1));

        let mut reader = track.reader();
        let TrackEvent::Subgroup(sg) = reader.next().await else { panic!("expected subgroup") };
        assert_eq!(sg.header.group_id, 1);
        let mut objs = sg.reader();
        assert_eq!(objs.next().await.unwrap().id, 0);
        assert_eq!(objs.next().await.unwrap().id, 1);

        g1.finish(SubgroupEnd::Fin);
        assert_eq!(objs.next().await.unwrap_err(), SubgroupEnd::Fin);
        assert_eq!(track.largest(), Some(Location::new(1, 1)));
    }

    #[tokio::test]
    async fn live_reader_sees_new_groups_then_end() {
        let track = Track::new(FullTrackName::new(TrackNamespace::new(["a"]), "v"));
        let mut reader = track.reader();
        let t = track.clone();
        tokio::spawn(async move {
            let g = t.create_subgroup(header(5));
            t.push_object(&g, obj(0));
            g.finish(SubgroupEnd::Fin);
            t.end(2);
        });
        let TrackEvent::Subgroup(sg) = reader.next().await else { panic!() };
        assert_eq!(sg.header.group_id, 5);
        assert!(matches!(reader.next().await, TrackEvent::Ended(2)));
    }

    #[tokio::test]
    async fn cache_evicts_old_groups() {
        let track = Track::new(FullTrackName::new(TrackNamespace::new(["a"]), "v"));
        for g in 0..10 {
            track.create_subgroup(header(g));
        }
        let s = track.state.borrow();
        assert!(s.subgroups.iter().all(|(_, sg)| sg.header.group_id + CACHE_GROUPS >= 9));
    }
}
