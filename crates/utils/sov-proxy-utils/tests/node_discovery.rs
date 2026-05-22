//! Integration tests for [`NodeDiscovery`] against a real PostgreSQL instance.
//!
//! These tests exercise the discovery task's resilience in isolation: that it
//! re-polls cluster info even when no notification fires, and that it survives
//! losing its database connection instead of exiting.

use std::time::Duration;

use sov_proxy_utils::{NodeDiscovery, NodeDiscoveryTask};
use sov_test_utils::postgres::{
    connection_string_from_postgres_container, create_postgres_container, ContainerAsync,
    CreatePostgresError, Postgres,
};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

/// Minimal schema `NodeDiscovery` depends on: the `nodes` table it queries, the
/// `sequencer_leader` table it `LEFT JOIN`s, and the `nodes_changes` NOTIFY
/// trigger. This is a trimmed copy of the sequencer's production migrations.
const SCHEMA: &str = r#"
CREATE TABLE sequencer_leader (
    singleton INTEGER GENERATED ALWAYS AS (1) STORED UNIQUE,
    node_id TEXT NOT NULL,
    last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (singleton)
);

CREATE TABLE nodes (
    node_id TEXT PRIMARY KEY,
    address TEXT NOT NULL,
    last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE FUNCTION notify_nodes_changes() RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('nodes_changes', NEW.node_id || ',' || NEW.address || ',' || TG_OP);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER nodes_changes_trigger
    AFTER INSERT OR UPDATE ON nodes
    FOR EACH ROW EXECUTE FUNCTION notify_nodes_changes();
"#;

/// Spins up a Postgres container with [`SCHEMA`] applied.
///
/// Returns `None` when Docker is unavailable so the test can skip cleanly. The
/// returned [`ContainerAsync`] must be kept alive for the whole test: dropping
/// it stops the database.
async fn setup() -> Option<(ContainerAsync<Postgres>, String, PgPool)> {
    let container = match create_postgres_container().await {
        Ok(container) => container,
        Err(CreatePostgresError::DockerNotSupported) => return None,
        Err(CreatePostgresError::DockerError(e)) => {
            panic!("Failed to create Postgres container: {e}");
        }
    };

    let connection_string = connection_string_from_postgres_container(&container)
        .await
        .expect("Failed to build connection string");

    let writer = PgPoolOptions::new()
        .connect(&connection_string)
        .await
        .expect("Failed to connect writer pool");

    sqlx::raw_sql(SCHEMA)
        .execute(&writer)
        .await
        .expect("Failed to apply schema");

    Some((container, connection_string, writer))
}

/// Inserts a node into the `nodes` table with a fresh heartbeat.
async fn insert_node(pool: &PgPool, node_id: &str, address: &str) {
    sqlx::query("INSERT INTO nodes (node_id, address, last_updated) VALUES ($1, $2, NOW())")
        .bind(node_id)
        .bind(address)
        .execute(pool)
        .await
        .expect("Failed to insert node");
}

/// Waits for the discovery task to publish a cluster info update.
async fn wait_for_change(task: &mut NodeDiscoveryTask) {
    tokio::time::timeout(Duration::from_secs(10), task.receiver.changed())
        .await
        .expect("Timed out waiting for a cluster info update")
        .expect("Cluster info watch channel closed");
}

/// A membership change made while no notification fires is still discovered,
/// because `NodeDiscovery` re-polls cluster info every `poll_interval`.
#[tokio::test(flavor = "multi_thread")]
async fn discovers_membership_change_without_notification() {
    let Some((_container, connection_string, writer)) = setup().await else {
        return; // Docker unavailable — skip.
    };

    let mut task = NodeDiscovery::connect(
        &connection_string,
        Duration::from_secs(300),   // max_age: keep nodes well within the window.
        Duration::from_millis(500), // poll_interval: short, to keep the test fast.
        None,
    )
    .await
    .expect("Failed to connect NodeDiscovery")
    .spawn();

    // Baseline: a node inserted while the NOTIFY trigger is active is discovered.
    insert_node(&writer, "node_a", "127.0.0.1:9001").await;
    wait_for_change(&mut task).await;
    assert!(
        task.receiver.borrow_and_update().has_follower("node_a"),
        "node inserted with the trigger active should be discovered"
    );

    // Drop the trigger so the next insert fires no notification, then change
    // membership. Only the periodic poll can surface this.
    sqlx::query("DROP TRIGGER nodes_changes_trigger ON nodes")
        .execute(&writer)
        .await
        .expect("Failed to drop trigger");
    insert_node(&writer, "node_b", "127.0.0.1:9002").await;

    wait_for_change(&mut task).await;
    assert!(
        task.receiver.borrow_and_update().has_follower("node_b"),
        "node added without a notification should be discovered via the periodic poll"
    );

    task.abort();
}

/// The discovery task survives its database connection being dropped: it
/// reconnects and keeps reporting cluster changes instead of exiting.
#[tokio::test(flavor = "multi_thread")]
async fn survives_database_connection_loss() {
    let Some((_container, connection_string, writer)) = setup().await else {
        return; // Docker unavailable — skip.
    };

    let mut task = NodeDiscovery::connect(
        &connection_string,
        Duration::from_secs(300),
        Duration::from_millis(500),
        None,
    )
    .await
    .expect("Failed to connect NodeDiscovery")
    .spawn();

    insert_node(&writer, "node_a", "127.0.0.1:9001").await;
    wait_for_change(&mut task).await;

    // Terminate every backend except this one — drops NodeDiscovery's listener
    // and pool connections, the same as an abrupt network failure.
    sqlx::query(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
         WHERE datname = current_database() AND pid <> pg_backend_pid()",
    )
    .execute(&writer)
    .await
    .expect("Failed to terminate backends");

    // The task must reconnect rather than exit, and keep discovering nodes.
    insert_node(&writer, "node_b", "127.0.0.1:9002").await;
    wait_for_change(&mut task).await;
    assert!(
        task.receiver.borrow_and_update().has_follower("node_b"),
        "discovery task should recover from connection loss and keep discovering nodes"
    );

    task.abort();
}
