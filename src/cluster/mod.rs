//! One table compared across the nodes of a cluster - the compares of YT Cluster combined here for reuse
//! (2026-11-11): ytserv's background reconciliation first ([`recon`]: the row's id and value, the frames'
//! fold, the vote), ytctl's data diff second ([`diff`]: a node repaired from a primary, on recon's id, value
//! and fold), and the placement of a table on a YT Cluster ([`placement`]). The rest of this crate compares
//! two databases, a reference and a target; this module compares N copies.
//!
//! The SQL builders and the vote are pure; the reads take a `tokio_postgres::Client` - any pool's connection
//! derefs to one.

pub mod diff;
pub mod placement;
pub mod recon;
