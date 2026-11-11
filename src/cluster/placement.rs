//! Which nodes of a YT Cluster hold a table, and why a table cannot be repaired from one primary - from
//! ytctl's data diff (its `tui/data_diff.rs`, 2026-11-11). The placement row is ytserv's `yt_info('P')`
//! (its `state/placement.rs` `row_json`): a copy of its format, so change both when the row changes.

/// Where a table's rows live by its config (YT Cluster's `shard`, `az` and `db_source`)
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Placing {
    pub shard: Option<String>,
    pub az: Option<String>,
    pub db_source: Option<String>,
}

fn is_set(v: &Option<String>) -> bool {
    v.as_deref().is_some_and(|s| !s.trim().is_empty())
}

/// Why a table cannot be diffed against one primary, if it cannot
pub fn refused(p: &Placing) -> Option<String> {
    if is_set(&p.shard) {
        return Some("sharded (shard): each node holds its own rows - a primary's would land where they do not belong".into());
    }
    if is_set(&p.az) {
        return Some("held by some zones only (az): a node outside them holds none of its rows".into());
    }
    if is_set(&p.db_source) {
        return Some("its data lives in another database (db_source)".into());
    }
    None
}

/// A table's placement as ytserv's `yt_info('P')` row gives it: the holders, each in ytserv's uuid8 spelling
/// (`aa..bbbbbb`), whether the answer is complete, and whether each row lives on its owner only
#[derive(Debug, Clone, PartialEq)]
pub struct Placement {
    pub holders: Vec<String>,
    pub complete: bool,
    pub disjoint: bool,
}

impl Placement {
    /// Is the node with this full uuid a holder? The holder spelling is the uuid's first two and last six
    /// hex digits, as ytserv's `as_uuid8` writes them
    pub fn holds(&self, uuid: &str) -> bool {
        let u = uuid.trim().to_ascii_lowercase();
        if u.len() != 36 || !u.is_ascii() {
            return false;
        }
        self.holders.iter().any(|h| h.split_once("..").is_some_and(|(a, b)| a == &u[..2] && b == &u[30..]))
    }
}

/// A placement row's value: `{"holds":[{"node":"aa..bbbbbb",..}],"disjoint":bool,"complete":bool,..}`. A row
/// without `disjoint` is an older ytserv's: read as disjoint, so the diff refuses it
pub fn parse_placement(value: &str) -> Option<Placement> {
    let v: serde_json::Value = serde_json::from_str(value).ok()?;
    let holders = v.get("holds")?.as_array()?.iter()
        .filter_map(|h| h.get("node").and_then(|n| n.as_str()))
        .map(|n| n.trim().to_ascii_lowercase())
        .filter(|n| !n.is_empty())
        .collect();
    Some(Placement {
        holders,
        complete: v.get("complete").and_then(|c| c.as_bool()).unwrap_or(false),
        disjoint: v.get("disjoint").and_then(|d| d.as_bool()).unwrap_or(true),
    })
}

/// Whether the table's rows are not on every node by its config: a zone list or a shard column names its
/// holders. Only such a table is placed; any other is held everywhere
pub fn is_placed(p: &Placing) -> bool {
    is_set(&p.az) || is_set(&p.shard)
}

/// Why a table cannot be diffed on its placement's nodes, if it cannot. With the placement row, its holders
/// decide: a disjoint table has no primary to copy from, and an incomplete answer may leave a holder out.
/// Without the row (an older ytserv), the static refusal ([`refused`]) stands
pub fn placement_refusal(placement: Option<&Placement>, p: &Placing) -> Option<String> {
    if is_set(&p.db_source) {
        return Some("its data lives in another database (db_source)".into());
    }
    let Some(pl) = placement else { return refused(p) };
    if pl.disjoint {
        return Some(if is_set(&p.az) && !is_set(&p.shard) {
            "held by zone (az): each zone holds its own rows - a primary's would land where they do not belong".into()
        } else {
            "sharded (shard): each node holds its own rows - a primary's would land where they do not belong".into()
        });
    }
    if !pl.complete {
        return Some("the placement is not complete (a peer is unreachable): a holder may be left out of the compare".into());
    }
    if pl.holders.is_empty() {
        return Some("no node holds it by the placement".into());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // ytctl's tests of these (tui/data_diff.rs)

    fn placing(shard: Option<&str>, az: Option<&str>, db_source: Option<&str>) -> Placing {
        Placing { shard: shard.map(str::to_string), az: az.map(str::to_string), db_source: db_source.map(str::to_string) }
    }

    #[test]
    fn a_sharded_table_is_not_diffed_against_one_primary() {
        assert!(refused(&Placing::default()).is_none());
        assert!(refused(&placing(Some("tenant"), None, None)).unwrap().contains("sharded"));
        assert!(refused(&placing(None, Some("east"), None)).unwrap().contains("zones"));
        assert!(refused(&placing(None, None, Some("other"))).is_some());
    }

    #[test]
    fn a_disjoint_placement_names_the_zone_or_the_shard_its_config_has() {
        let p = Placement { holders: vec![], complete: true, disjoint: true };
        let why = placement_refusal(Some(&p), &placing(None, Some("east"), None)).unwrap();
        assert!(why.contains("zone") && !why.contains("sharded"), "{}", why);
        assert!(placement_refusal(Some(&p), &placing(Some("owner"), None, None)).unwrap().starts_with("sharded"));
    }

    #[test]
    fn a_placement_row_reads_its_holders_completeness_and_disjointness() {
        const EXAMPLE: &str = r#"{"table":"mydb.public.orders","holds":[{"node":"9b..0914c0","az":"east","host":"n3:7421"}],"writable_az":[],"shard":{"column":"owner","strategy":"az_node_uuid","outer":"pin","redundancy":0},"disjoint":true,"filter":"","ignore":[],"db_source":"","stable":{"window_sec":300,"ts_col":null,"stable_before":null},"complete":true}"#;
        let p = parse_placement(EXAMPLE).unwrap();
        assert_eq!(p, Placement { holders: vec!["9b..0914c0".into()], complete: true, disjoint: true });
        assert!(p.holds("9b3a1c2e-0000-4000-8000-0000000914c0"), "the holder's full uuid");
        assert!(!p.holds("9b3a1c2e-0000-4000-8000-0000000914c1"), "another node's uuid");
        assert!(!p.holds("9b3a1c2e"), "a short uuid holds nothing");
        assert!(parse_placement("not json").is_none());
    }

    #[test]
    fn a_row_without_disjoint_is_refused_as_sharded() {
        let p = parse_placement(r#"{"holds":[{"node":"aa..bbbbbb"}],"complete":true}"#).unwrap();
        assert!(p.disjoint);
        assert!(placement_refusal(Some(&p), &Placing::default()).unwrap().contains("sharded"));
    }

    #[test]
    fn a_table_on_every_node_is_not_placed() {
        assert!(!is_placed(&Placing::default()));
        assert!(!is_placed(&placing(None, None, Some("other"))));
        assert!(is_placed(&placing(None, Some("east"), None)));
        assert!(is_placed(&placing(Some("tenant"), None, None)));
    }

    #[test]
    fn a_mirror_shard_is_diffed_on_its_holders_and_the_rest_are_refused() {
        let holders = Placement { holders: vec!["aa..bbbbbb".into()], complete: true, disjoint: false };
        let mirror = placing(Some("tenant"), None, None);
        assert_eq!(placement_refusal(Some(&holders), &mirror), None);
        let disjoint = Placement { disjoint: true, ..holders.clone() };
        assert!(placement_refusal(Some(&disjoint), &mirror).unwrap().contains("sharded"));
        let partial = Placement { complete: false, ..holders.clone() };
        assert!(placement_refusal(Some(&partial), &mirror).unwrap().contains("not complete"));
        let nobody = Placement { holders: vec![], ..holders.clone() };
        assert!(placement_refusal(Some(&nobody), &mirror).unwrap().contains("no node"));
        let other_db = placing(None, None, Some("other"));
        assert!(placement_refusal(Some(&holders), &other_db).unwrap().contains("another database"));
        assert!(placement_refusal(None, &mirror).unwrap().contains("sharded"));
        assert_eq!(placement_refusal(None, &Placing::default()), None);
    }
}
