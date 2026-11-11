//! The cluster compare against a live PostgreSQL: a primary and a node, two databases on one server, made the
//! same by the repair the module plans - whole, and range by range with ranges small enough to be halved.
//! Run with `DIFF_DATA_PG="host=127.0.0.1 port=5432 user=postgres" cargo test --test cluster_live`; without
//! it the test does nothing. It creates and drops the databases `dd_cluster_primary` and `dd_cluster_node`.

use diff_data::cluster::diff::{self, Plan, TableRules};
use tokio_postgres::{Client, NoTls};

async fn connect(base: &str, db: &str) -> Client {
    let (c, conn) = tokio_postgres::connect(&format!("{} dbname={}", base, db), NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    c
}

const TABLE: &str = "CREATE TABLE public.orders (tenant int, id bigint, item text, note text, \
    total int GENERATED ALWAYS AS (length(item)) STORED, PRIMARY KEY (tenant, id))";

async fn repaired(p: &Client, n: &Client, rules: &TableRules, ranged: bool) -> usize {
    let shape = diff::shape_of(p, "public", "orders", rules).await.expect("the shape");
    let plan = if ranged {
        diff::ranged_plan(p, n, "public", "orders", &shape, 3).await.expect("the ranged plan")
    } else {
        let pm = diff::key_md5s(p, "public", "orders", &shape).await.expect("the primary's ids");
        let nm = diff::key_md5s(n, "public", "orders", &shape).await.expect("the node's ids");
        Plan::of(&pm, &nm)
    };
    let up = diff::rows_of(p, "public", "orders", &shape, &plan.upsert).await.expect("the primary's rows");
    let gone = diff::rows_of(n, "public", "orders", &shape, &plan.delete).await.expect("the node's rows");
    let mut stmts: Vec<String> = up.iter().map(|(_, j)| shape.upsert_sql("public", "orders", j)).collect();
    stmts.extend(gone.iter().map(|(_, j)| shape.delete_sql("public", "orders", j)));
    diff::apply(n, &diff::repair_sql("test", None, &stmts)).await.expect("the repair applies");
    stmts.len()
}

#[tokio::test]
async fn a_node_is_made_the_same_as_its_primary() {
    let Ok(base) = std::env::var("DIFF_DATA_PG") else { return };
    let admin = connect(&base, "postgres").await;
    for db in ["dd_cluster_primary", "dd_cluster_node"] {
        admin.batch_execute(&format!("DROP DATABASE IF EXISTS {db}")).await.unwrap();
        admin.batch_execute(&format!("CREATE DATABASE {db}")).await.unwrap();
    }
    let p = connect(&base, "dd_cluster_primary").await;
    let n = connect(&base, "dd_cluster_node").await;
    for c in [&p, &n] {
        c.batch_execute(TABLE).await.unwrap();
        c.batch_execute("INSERT INTO public.orders (tenant, id, item, note) SELECT g % 3, g, 'i' || g, 'n' || g FROM generate_series(1, 40) g")
            .await.unwrap();
    }
    // the node differs: a changed row, a missing one, an extra one, a changed ignored column (no difference),
    // and a row outside the filter (no difference); a key value holding '|'
    n.batch_execute("UPDATE public.orders SET item = 'changed' WHERE id = 5; DELETE FROM public.orders WHERE id = 7; \
        INSERT INTO public.orders (tenant, id, item, note) VALUES (1, 99, 'extra', 'x'); \
        UPDATE public.orders SET note = 'local' WHERE id = 9; UPDATE public.orders SET item = 'outside' WHERE id = 12")
        .await.unwrap();
    let rules = TableRules::new(Some("note"), Some("id <> 12"));
    let (pf, nf) = (diff::table_fold(&p, "public", "orders", &rules).await.unwrap(),
        diff::table_fold(&n, "public", "orders", &rules).await.unwrap());
    assert_ne!(pf, nf, "phase 1 sees the node differ");
    assert_eq!(repaired(&p, &n, &rules, false).await, 3, "an upsert of 5 and 7, a delete of 99");
    assert_eq!(diff::table_fold(&p, "public", "orders", &rules).await.unwrap(),
        diff::table_fold(&n, "public", "orders", &rules).await.unwrap(), "the same after the repair");
    let note: String = n.query_one("SELECT note FROM public.orders WHERE id = 9", &[]).await.unwrap().get(0);
    assert_eq!(note, "local", "an ignored column stays the node's");

    // range by range, the ranges halved down to 3 rows: the same repair
    n.batch_execute("UPDATE public.orders SET item = 'again' WHERE id IN (2, 33); DELETE FROM public.orders WHERE id = 20; \
        INSERT INTO public.orders (tenant, id, item) VALUES (0, 77, 'extra')").await.unwrap();
    assert_eq!(repaired(&p, &n, &rules, true).await, 4, "two upserts, the missing row, a delete");
    assert_eq!(diff::table_fold(&p, "public", "orders", &rules).await.unwrap(),
        diff::table_fold(&n, "public", "orders", &rules).await.unwrap(), "the same after the ranged repair");

    // an ambiguous id - two keys rendering one id - refuses the table
    for c in [&p, &n] {
        c.batch_execute("CREATE TABLE public.t2 (a text, b text, PRIMARY KEY (a, b)); \
            INSERT INTO public.t2 VALUES ('x|y', 'z'), ('x', 'y|z')").await.unwrap();
    }
    let shape = diff::shape_of(&p, "public", "t2", &TableRules::default()).await.unwrap();
    let err = diff::key_md5s(&p, "public", "t2", &shape).await.unwrap_err();
    assert!(err.contains("[x|y|z]"), "{}", err);

    drop((p, n));
    for db in ["dd_cluster_primary", "dd_cluster_node"] {
        admin.batch_execute(&format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)")).await.unwrap();
    }
}

/// What the reads report and decide at their edges: the server's error with its detail and hint (and a
/// client-side error as itself), the first key's type, no ranges, empty tables, a single-column key's leaf
#[tokio::test]
async fn the_reads_report_the_servers_error_and_decide_at_their_edges() {
    let Ok(base) = std::env::var("DIFF_DATA_PG") else { return };
    let admin = connect(&base, "postgres").await;
    for db in ["dd_edges_primary", "dd_edges_node"] {
        admin.batch_execute(&format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)")).await.unwrap();
        admin.batch_execute(&format!("CREATE DATABASE {db}")).await.unwrap();
    }
    let p = connect(&base, "dd_edges_primary").await;
    let n = connect(&base, "dd_edges_node").await;

    let err = diff::apply(&n, "DO $$ BEGIN RAISE EXCEPTION 'boom' USING DETAIL = 'the detail', HINT = 'the hint'; END $$")
        .await.unwrap_err();
    assert_eq!(err, "[P0001] boom | detail: the detail | hint: the hint");
    let client_side = n.query("SELECT $1::int", &[]).await.unwrap_err();
    assert_eq!(diff::pg_text(&client_side), client_side.to_string());
    // as a diff_data error: its code, then what the server said
    diff_data::i18n::set_lang("en");
    let server_side = n.query("SELECT 1/0", &[]).await.unwrap_err();
    let e = diff_data::DataDiffError::from(server_side);
    assert_eq!(e.to_string(), "DA-E001 PostgreSQL error: [22012] division by zero");
    assert!(std::error::Error::source(&e).is_some());

    for c in [&p, &n] {
        c.batch_execute("CREATE TABLE public.one (id int PRIMARY KEY, v text); CREATE TABLE public.named (k text PRIMARY KEY)")
            .await.unwrap();
    }
    let rules = TableRules::default();
    let one = diff::shape_of(&p, "public", "one", &rules).await.unwrap();
    let named = diff::shape_of(&p, "public", "named", &rules).await.unwrap();
    assert!(diff::first_key_is_integer(&p, "public", "one", &one).await.unwrap());
    assert!(!diff::first_key_is_integer(&p, "public", "named", &named).await.unwrap());
    assert!(diff::range_folds(&p, "public", "one", &one, &[]).await.unwrap().is_empty());
    assert_eq!(diff::key_bounds(&p, "public", "one", &one).await.unwrap(), None);
    assert!(diff::ranged_plan(&p, &n, "public", "one", &one, 2).await.unwrap().is_empty(), "no rows on either side");

    // a single-column key: its leaves are read whole, never paged
    p.batch_execute("INSERT INTO public.one SELECT g, 'v' || g FROM generate_series(1, 9) g").await.unwrap();
    n.batch_execute("INSERT INTO public.one SELECT g, 'v' || g FROM generate_series(2, 9) g; \
        UPDATE public.one SET v = 'changed' WHERE id = 8; INSERT INTO public.one VALUES (20, 'extra')").await.unwrap();
    let plan = diff::ranged_plan(&p, &n, "public", "one", &one, 2).await.unwrap();
    assert_eq!((plan.upsert.clone(), plan.delete.clone()), (vec!["1".to_string(), "8".to_string()], vec!["20".to_string()]));

    drop((p, n));
    for db in ["dd_edges_primary", "dd_edges_node"] {
        admin.batch_execute(&format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)")).await.unwrap();
    }
}
