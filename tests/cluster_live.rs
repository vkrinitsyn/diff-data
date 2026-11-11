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
