//! A row's copies across a cluster, as YT Cluster's ytserv compares them (its `state/recon.rs`, the
//! reference: the first-class side of this module, 2026-11-11). Copied as ytserv has them, so that ytserv
//! can take them from here without a change in what it computes:
//!
//! - a row's id: its primary-key columns in key order, each as `<col>::text`, joined by `'|'`
//!   ([`pk_expr`]). Integer, bigint, uuid, text and boolean keys round-trip exactly; a float key may
//!   not, and a text key holding `'|'` can render the id of another key - documented, not handled;
//! - the value a row is compared by: `to_jsonb(t.*)` less the ignored columns ([`row_basis`]) - jsonb
//!   normalises the key order, so two nodes whose table grew its columns in another order agree;
//! - a row's checksum: the first 32 bits of the md5 of that value; a missing row is its own copy;
//! - a frame of rows: `(count, xor32, sum64)` over an integer key range ([`Frame`], [`frames_sql`],
//!   [`folds_sql`]) - all commutative, as two nodes never scan in the same order. FROZEN: a node of
//!   another fold disagrees with every frame;
//! - the vote ([`decide`]): every copy the same, done; else a strict majority of the voters wins; on a
//!   table a node commits alone (nodes=N) the copy of the node that wrote the row last decides,
//!   majority or not ([`writer_of`]); no winner, the row is blocked for an operator.

/// A column name fit to paste into SQL: everything else is bound as a parameter, a column cannot be
pub fn safe_ident(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 63
        && s.chars().next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// The row filter as a SQL fragment, or empty. Pasted rather than bound - a predicate cannot be a
/// parameter - and parenthesised, so it cannot escape its own AND
pub fn filter_and(filter: &str) -> String {
    let f = filter.trim();
    if f.is_empty() { String::new() } else { format!(" and ({})", f) }
}

/// The per-row value every checksum is taken over: `to_jsonb(t.*)`, not `t.*::text`, which is
/// column-order dependent; the ignored columns subtracted. A name that is not a plain identifier is
/// dropped rather than pasted
pub fn row_basis(ignore: &[String]) -> String {
    let cols: Vec<&String> = ignore.iter().filter(|c| safe_ident(c)).collect();
    if cols.is_empty() {
        "to_jsonb(t.*)::text".to_string()
    } else {
        format!("(to_jsonb(t.*) - ARRAY[{}]::text[])::text",
            cols.iter().map(|c| format!("'{}'", c)).collect::<Vec<_>>().join(","))
    }
}

/// What a row checksum is taken over: `t.*::text` (ytserv's unary rounds, which every version computes
/// alike), or the frames' `to_jsonb(t.*) - ignore`
pub fn crc_basis(basis: Option<&[String]>) -> String {
    match basis {
        None => "t.*::text".to_string(),
        Some(ignore) => row_basis(ignore),
    }
}

/// The SQL that renders a row's id: `(t.<col>::text)`, or the columns joined by `'|'`
pub fn pk_expr(cols: &[String]) -> String {
    if cols.len() == 1 {
        format!("(t.{}::text)", cols[0])
    } else {
        format!("concat_ws('|', {})", cols.iter().map(|c| format!("t.{}::text", c)).collect::<Vec<_>>().join(", "))
    }
}

/// The query for one row's own values of the `keep` columns, as a jsonb object (`$1` the row id), or
/// None when there is nothing to keep
pub fn kept_values_sql(rel: &str, pk_expr: &str, keep: &[String]) -> Option<String> {
    let cols: Vec<&String> = keep.iter().filter(|c| safe_ident(c)).collect();
    if cols.is_empty() {
        return None;
    }
    Some(format!("select jsonb_build_object({})::text from {} t where {} = $1",
        cols.iter().map(|c| format!("'{}', t.{}", c, c)).collect::<Vec<_>>().join(", "), rel, pk_expr))
}

/// The 64-bit sum of the rows' 60-bit hashes, wrapped into a bigint: below 2^63 the plain sum, so the
/// frames of nodes on either side of the wrap agree where both can compute them
pub const SUM64_WRAPPED: &str = "(case when coalesce(sum(c60), 0) % 18446744073709551616 >= 9223372036854775808 \
    then coalesce(sum(c60), 0) % 18446744073709551616 - 18446744073709551616 \
    else coalesce(sum(c60), 0) % 18446744073709551616 end)::bigint";

/// The type oids a frame key may have: int2, int8, int4 - the key `ntile` and the ranges cut on
pub const FRAME_KEY_TYPES: [u32; 3] = [21, 20, 23];

/// Whether a key column of this type oid can frame a table
pub fn is_frame_key_type(oid: u32) -> bool {
    FRAME_KEY_TYPES.contains(&oid)
}

/// The rows of `[$1, $2)` of the frame key `key`, cut in `n` frames of about equal row count (equal
/// counts, not equal key widths: keys are gappy), each folded to `(min key, max key, count, xor32,
/// sum64, max ts)`; `ts_col` names a last-update column, a hint to skip a frame that did not change,
/// never proof. A key or a `ts_col` that is not a plain identifier is refused, as ytserv refuses the
/// `ts_col` (review 2026-11-11); `rel` is the caller's, as in ytserv
pub fn frames_sql(rel: &str, key: &str, n: u32, ignore: &[String], ts_col: &str, filter: &str) -> Result<String, String> {
    if !safe_ident(key) {
        return Err(tr!("error.recon_frame_key", key = key));
    }
    if !ts_col.is_empty() && !safe_ident(ts_col) {
        return Err(tr!("error.recon_ts_col", column = ts_col));
    }
    let n = n.clamp(1, 4096);
    let basis = row_basis(ignore);
    let ts_sel = if ts_col.is_empty() { "null::text".to_string() } else { format!("t.{}::text", ts_col) };
    Ok(format!(
        "with b as ( \
           select t.{k}::bigint as k, \
                  ('x' || md5({basis}))::bit(32)::int as c32, \
                  ('x' || substr(md5({basis}), 1, 15))::bit(60)::bigint as c60, \
                  {ts_sel} as ts, \
                  ntile({n}) over (order by t.{k}) as nt \
           from {rel} t where t.{k} >= $1::bigint and t.{k} < $2::bigint{flt} ) \
         select min(k)::bigint, max(k)::bigint, count(*)::bigint, \
                coalesce(bit_xor(c32),0)::bigint, {wrap}, \
                max(ts)::text \
         from b group by nt order by 1",
        k = key, rel = rel, basis = basis, n = n, flt = filter_and(filter), wrap = SUM64_WRAPPED, ts_sel = ts_sel))
}

/// The folds of every `[$1[i], $1[i+1])`, the last up to `$3`: `width_bucket` numbers a row by the
/// lower bound it is at or past (1 for `$1[0]`)
pub fn folds_sql(rel: &str, key: &str, basis: &str, filter: &str) -> String {
    format!("with b as ( \
           select width_bucket(t.{k}::bigint, $1::bigint[]) as nb, \
                  ('x' || md5({basis}))::bit(32)::int as c32, \
                  ('x' || substr(md5({basis}), 1, 15))::bit(60)::bigint as c60 \
           from {rel} t where t.{k} >= $2::bigint and t.{k} < $3::bigint{flt} ) \
         select nb, count(*)::bigint, coalesce(bit_xor(c32),0)::bigint, {wrap} from b group by nb",
        k = key, rel = rel, basis = basis, flt = filter_and(filter), wrap = SUM64_WRAPPED)
}

/// The rows of `[$1, $2)` of the frame key: id and checksum
pub fn rows_in_sql(rel: &str, pkx: &str, key: &str, ignore: &[String], filter: &str, lim: usize) -> String {
    format!("select {pkx} as yt_pk, ('x' || md5({b}))::bit(32)::int as yt_crc from {rel} t \
        where t.{k} >= $1::bigint and t.{k} < $2::bigint{flt} order by t.{k} limit {lim}",
        pkx = pkx, b = row_basis(ignore), rel = rel, k = key, flt = filter_and(filter), lim = lim)
}

/// The next `lim` rows by id after `$1`, in byte order: id and checksum
pub fn rows_after_sql(rel: &str, pkx: &str, ignore: &[String], filter: &str, lim: usize) -> String {
    format!("select {pkx} as yt_pk, ('x' || md5({b}))::bit(32)::int as yt_crc from {rel} t \
        where {pkx} collate \"C\" > $1{flt} order by {pkx} collate \"C\" limit {lim}",
        pkx = pkx, b = row_basis(ignore), rel = rel, flt = filter_and(filter), lim = lim)
}

/// One frame of a table's rows: half-open `[lo, hi)` on the frame key, its row count, the xor of the
/// rows' crc32 and the sum of their 64-bit hashes (ytserv's `ReconFrame`), and the frame's latest
/// last-update value when the table names a column for it (empty when not)
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Frame {
    pub lo: i64,
    pub hi: i64,
    pub count: i64,
    pub xor32: u32,
    pub sum64: i64,
    pub max_ts: String,
}

impl Frame {
    /// The frame from one row of [`frames_sql`]: `hi` one past its largest key, so the next frame
    /// starts there and the last one holds its own max
    pub fn from_row(min: i64, max: i64, count: i64, xor: i64, sum64: i64, max_ts: Option<String>) -> Frame {
        Frame { lo: min, hi: max.saturating_add(1), count, xor32: xor as u32, sum64, max_ts: max_ts.unwrap_or_default() }
    }

    /// The same rows on both sides: the count and both folds agree (`max_ts` is a hint, not compared)
    pub fn same_rows(&self, other: &Frame) -> bool {
        (self.count, self.xor32, self.sum64) == (other.count, other.xor32, other.sum64)
    }
}

/// A row's copy on one node: its checksum, or missing there
pub type Copy = (u32, bool);

/// A copy as a report shows it: the checksum in hex, or `missing`
pub fn crc_text(v: Copy) -> String {
    if v.1 { "missing".to_string() } else { format!("{:08x}", v.0) }
}

/// A row's vote from one node: the node, the newest writer it knows of the row (`(term, subterm)`,
/// `(0, 0)` for none), its copy, and whether it is that writer's origin
pub type WriterVote = (u128, (u64, u32), Copy, bool);

/// The newest writer any voter knows of the row
pub fn newest_writer(votes: &[WriterVote]) -> Option<(u64, u32)> {
    votes.iter().map(|v| v.1).filter(|k| *k != (0, 0)).max()
}

/// The source whose copy decides: the origin of the newest writer `newest` - as the node's own WAL
/// names it (`wal`), else the voter that says it wrote the row with that key. Two nodes that say they
/// wrote it with the very same key are a tie no order breaks: none decides, for an operator
pub fn writer_of(votes: &[WriterVote], newest: (u64, u32), wal: Option<u128>) -> Option<u128> {
    let origin = wal.or_else(|| votes.iter().find(|v| v.3 && v.1 == newest).map(|v| v.0))?;
    if votes.iter().any(|v| v.3 && v.1 == newest && v.0 != origin) {
        return None;
    }
    Some(origin)
}

/// The copy `origin` holds, when it voted
pub fn origin_copy(votes: &[WriterVote], origin: u128) -> Option<Copy> {
    votes.iter().find(|v| v.0 == origin).map(|v| v.2)
}

/// Whether a blocked row may be released on these answers: only a majority of every node (the
/// voters among `peers` and this one) releases it
pub fn released_by(voters: usize, peers: usize) -> bool {
    2 * voters > peers + 1
}

/// What the vote on one row's copies decides
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// every copy is the same
    Unanimous(Copy),
    /// the copies differ, and this one wins: a strict majority's, or the writer's
    Winner(Copy),
    /// the copies differ and none wins: the row is blocked for an operator
    Blocked,
}

/// The vote on a row: `votes` every answering node's, this one among them; `writer_decides` whether
/// the table commits on its writer alone (nodes=N), and `wal` the writer the node's own WAL names
/// for the newest key, when it holds that record. The writer's copy wins over any majority on such a
/// table; else a strict majority of the voters; else none.
///
/// Two checks stay the caller's, as in ytserv's round (review 2026-11-11): only the nodes that answered
/// for this row vote - a peer that did not answer is no vote, not a missing copy; and before a unanimous
/// verdict is taken, the WAL's record of the row's last writer outranks it: copies that all agree but
/// disagree with that record were all missed, and the write is run again rather than the row converged
pub fn decide(votes: &[WriterVote], writer_decides: bool, wal: impl Fn((u64, u32)) -> Option<u128>) -> Verdict {
    let mut tally: Vec<(Copy, usize)> = Vec::new();
    for v in votes {
        match tally.iter_mut().find(|(c, _)| *c == v.2) {
            Some((_, n)) => *n += 1,
            None => tally.push((v.2, 1)),
        }
    }
    match tally.as_slice() {
        [] => return Verdict::Blocked,
        [(c, _)] => return Verdict::Unanimous(*c),
        _ => {}
    }
    if writer_decides
        && let Some(newest) = newest_writer(votes)
            && let Some(copy) = writer_of(votes, newest, wal(newest)).and_then(|o| origin_copy(votes, o)) {
                return Verdict::Winner(copy);
            }
    let voters = votes.len();
    tally.iter().find(|(_, n)| *n * 2 > voters).map_or(Verdict::Blocked, |(c, _)| Verdict::Winner(*c))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ytserv's own tests of these, as ytserv has them (state/recon_test.rs)

    #[test]
    fn filter_is_parenthesised_or_absent() {
        assert_eq!(filter_and(""), "");
        assert_eq!(filter_and("   "), "");
        assert_eq!(filter_and("a = 1 or b = 2"), " and (a = 1 or b = 2)");
    }

    #[test]
    fn row_basis_normalises_and_subtracts() {
        assert_eq!(row_basis(&[]), "to_jsonb(t.*)::text");
        let b = row_basis(&["updated_by".into(), "local_seq".into()]);
        assert!(b.contains("to_jsonb(t.*) - ARRAY['updated_by','local_seq']"), "{}", b);
        let b = row_basis(&["ok_col".into(), "bad; drop table x".into()]);
        assert!(b.contains("'ok_col'"));
        assert!(!b.contains("drop table"), "{}", b);
    }

    #[test]
    fn the_compare_queries_bind_their_bounds() {
        let f = folds_sql("public.t", "id", &row_basis(&["note".to_string()]), "v > 0");
        assert!(f.contains("width_bucket(t.id::bigint, $1::bigint[]) as nb"), "{}", f);
        assert!(f.contains("where t.id >= $2::bigint and t.id < $3::bigint and (v > 0) )"), "{}", f);
        assert!(f.contains("md5((to_jsonb(t.*) - ARRAY['note']::text[])::text)"), "{}", f);
        let r = rows_in_sql("public.t", "(t.id::text)", "id", &[], "", 7);
        assert!(r.contains("where t.id >= $1::bigint and t.id < $2::bigint order by t.id limit 7"), "{}", r);
        let a = rows_after_sql("public.t", "concat_ws('|', t.a::text, t.b::text)", &[], "", 9);
        assert!(a.contains("where concat_ws('|', t.a::text, t.b::text) collate \"C\" > $1 order by \
            concat_ws('|', t.a::text, t.b::text) collate \"C\" limit 9"), "{}", a);
        assert_eq!(crc_basis(None), "t.*::text", "recon's unary rounds keep theirs");
        assert_eq!(kept_values_sql("public.t", "(t.id::text)", &["note".to_string(), "bad;col".to_string()]).as_deref(),
            Some("select jsonb_build_object('note', t.note)::text from public.t t where (t.id::text) = $1"));
        assert_eq!(kept_values_sql("public.t", "(t.id::text)", &[]), None);
    }

    #[test]
    fn pk_expr_matches_pk_text_shape() {
        assert_eq!(pk_expr(&["id".to_string()]), "(t.id::text)");
        assert_eq!(pk_expr(&["a".to_string(), "b".to_string()]), "concat_ws('|', t.a::text, t.b::text)");
    }

    #[test]
    fn a_checksum_renders_as_hex_or_missing() {
        assert_eq!(crc_text((255, false)), "000000ff");
        assert_eq!(crc_text((255, true)), "missing");
    }

    #[test]
    fn with_no_majority_the_writers_copy_decides() {
        let (a, b, c, d) = (1u128, 2u128, 3u128, 4u128);
        let votes: Vec<WriterVote> = vec![
            (a, (1, 40), (111, false), false),
            (b, (1, 31), (222, false), false),
            (c, (1, 38), (333, false), false),
            (d, (0, 0), (0, true), false),
        ];
        assert_eq!(newest_writer(&votes), Some((1, 40)));
        assert_eq!(origin_copy(&votes, a), Some((111, false)), "the origin's copy decides");
        let mut later = votes.clone();
        later.push((c, (2, 1), (333, false), false));
        assert_eq!(newest_writer(&later), Some((2, 1)));
        assert_eq!(origin_copy(&votes[1..], a), None);
        let blind: Vec<WriterVote> = vec![(a, (0, 0), (1, false), false), (b, (0, 0), (2, false), false)];
        assert_eq!(newest_writer(&blind), None);
        let deleted: Vec<WriterVote> = vec![(a, (1, 50), (0, true), false), (b, (1, 49), (7, false), false), (c, (1, 49), (8, false), false)];
        assert_eq!(origin_copy(&deleted, a), Some((0, true)));
    }

    #[test]
    fn the_writer_names_itself_where_the_wal_lacks_its_record() {
        let (a, b, c) = (1u128, 2u128, 3u128);
        let votes: Vec<WriterVote> = vec![
            (a, (1, 40), (111, false), true),
            (b, (1, 40), (222, false), false),
            (c, (1, 12), (333, false), true),
        ];
        let newest = newest_writer(&votes).unwrap();
        assert_eq!(writer_of(&votes, newest, None), Some(a));
        let one: Vec<WriterVote> = votes[..2].to_vec();
        assert_eq!(writer_of(&one, newest, None), Some(a), "the one claimant with the newest key");
        assert_eq!(writer_of(&one, newest, Some(a)), Some(a), "the WAL names the same writer");
        assert_eq!(origin_copy(&one, a), Some((111, false)), "the claimant's copy, not b's with the same key");
        assert_eq!(writer_of(&votes[1..], newest, None), None);
    }

    #[test]
    fn of_two_writers_the_higher_subterm_wins_a_tie_is_for_an_operator() {
        let (a, b) = (1u128, 2u128);
        let split: Vec<WriterVote> = vec![(a, (1, 40), (111, false), true), (b, (1, 41), (222, false), true)];
        assert_eq!(writer_of(&split, (1, 41), None), Some(b));
        assert_eq!(writer_of(&split, (1, 41), Some(b)), Some(b), "the WAL names b too");
        assert_eq!(origin_copy(&split, b), Some((222, false)));
        let same: Vec<WriterVote> = vec![(a, (3, 7), (111, false), true), (b, (3, 7), (222, false), true)];
        assert_eq!(writer_of(&same, (3, 7), None), None);
        let quiet: Vec<WriterVote> = vec![(a, (1, 40), (111, false), false), (b, (1, 39), (222, false), false)];
        assert_eq!(writer_of(&quiet, (1, 40), Some(a)), Some(a));
    }

    // the vote as one function, as ytserv's round takes it (state/recon.rs, the round's tally)

    /// every copy the same: done; a strict majority of the voters wins; two against two is blocked
    #[test]
    fn the_vote_is_unanimous_a_strict_majority_or_blocked() {
        let v = |n: u128, crc: u32| (n, (0u64, 0u32), (crc, false), false);
        assert_eq!(decide(&[v(1, 7), v(2, 7), v(3, 7)], false, |_| None), Verdict::Unanimous((7, false)));
        assert_eq!(decide(&[v(1, 7), v(2, 7), v(3, 9)], false, |_| None), Verdict::Winner((7, false)));
        assert_eq!(decide(&[v(1, 7), v(2, 7), v(3, 9), v(4, 9)], false, |_| None), Verdict::Blocked, "a tie is no majority");
        assert_eq!(decide(&[], false, |_| None), Verdict::Blocked);
        // a missing row is a copy of its own
        let gone = (3u128, (0u64, 0u32), (0u32, true), false);
        assert_eq!(decide(&[v(1, 7), v(2, 7), gone], false, |_| None), Verdict::Winner((7, false)));
    }

    /// on a table its writer commits alone, the writer's copy wins over a majority; with no writer known
    /// the majority stands, and a tie of two writers falls back to it too
    #[test]
    fn the_writers_copy_wins_over_a_majority_where_the_writer_decides() {
        let votes: Vec<WriterVote> = vec![(1, (1, 40), (111, false), true), (2, (1, 31), (222, false), false), (3, (1, 31), (222, false), false)];
        assert_eq!(decide(&votes, true, |_| None), Verdict::Winner((111, false)), "the writer's copy, against two");
        assert_eq!(decide(&votes, false, |_| None), Verdict::Winner((222, false)), "elsewhere the majority");
        assert_eq!(decide(&votes, true, |_| Some(1)), Verdict::Winner((111, false)), "the WAL names the same writer");
        let blind: Vec<WriterVote> = vec![(1, (0, 0), (1, false), false), (2, (0, 0), (2, false), false), (3, (0, 0), (2, false), false)];
        assert_eq!(decide(&blind, true, |_| None), Verdict::Winner((2, false)), "no writer known: the majority");
        let tie: Vec<WriterVote> = vec![(1, (3, 7), (1, false), true), (2, (3, 7), (2, false), true)];
        assert_eq!(decide(&tie, true, |_| None), Verdict::Blocked, "two writers of one key and no majority");
    }

    #[test]
    fn a_blocked_row_is_released_by_a_majority_of_every_node() {
        assert!(!released_by(1, 2), "one of three");
        assert!(released_by(2, 2), "two of three");
        assert!(!released_by(2, 3), "two of four is no majority");
    }

    /// the frames' query cuts by row count and folds as the frozen fold; a frame's `hi` is one past
    /// its largest key; `max_ts` does not count
    #[test]
    fn a_frame_is_folded_as_the_frozen_fold() {
        let q = frames_sql("public.t", "id", 8, &["note".to_string()], "updated_at", "v > 0").unwrap();
        assert!(q.contains("ntile(8) over (order by t.id) as nt"), "{}", q);
        assert!(q.contains("('x' || md5((to_jsonb(t.*) - ARRAY['note']::text[])::text))::bit(32)::int as c32"), "{}", q);
        assert!(q.contains("('x' || substr(md5((to_jsonb(t.*) - ARRAY['note']::text[])::text), 1, 15))::bit(60)::bigint as c60"), "{}", q);
        assert!(q.contains("t.updated_at::text as ts"), "{}", q);
        assert!(q.contains("where t.id >= $1::bigint and t.id < $2::bigint and (v > 0) )"), "{}", q);
        assert!(q.contains(SUM64_WRAPPED), "{}", q);
        assert!(frames_sql("t", "id", 0, &[], "", "").unwrap().contains("ntile(1)"), "at least one frame");
        assert!(frames_sql("t", "id", 9999, &[], "", "").unwrap().contains("ntile(4096)"), "at most 4096");
        // a name that is not a plain identifier is refused, not pasted
        assert!(frames_sql("t", "id; drop table x", 4, &[], "", "").unwrap_err().contains("frame key"));
        assert!(frames_sql("t", "id", 4, &[], "ts) or (1=1", "").unwrap_err().contains("last-update column"));
        let f = Frame::from_row(1, 10, 10, -1, 5, None);
        assert_eq!((f.lo, f.hi, f.xor32), (1, 11, u32::MAX));
        assert!(f.same_rows(&Frame { max_ts: "2026-11-11".into(), ..f.clone() }));
        assert!(!f.same_rows(&Frame { sum64: 6, ..f.clone() }));
        assert!(is_frame_key_type(20) && is_frame_key_type(21) && is_frame_key_type(23) && !is_frame_key_type(25));
    }
}
