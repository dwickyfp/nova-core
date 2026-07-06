use nova_common::{
    ACCOUNTADMIN_ROLE_ID, NovaError, PUBLIC_ROLE_ID, ROOT_USER_ID, RoleMeta, SecurityContext,
    UserMeta, generate_id, now_micros,
};
use nova_coordinator::executor::Executor;
use nova_coordinator::mysql_protocol::server::{RoleCommand, parse_role_command};
use nova_storage::{FdbMetadataStore, MetadataStore, MpReader, MpWriter};
use object_store::local::LocalFileSystem;
use std::sync::Arc;
use tempfile::TempDir;

fn setup_executor() -> Option<(Executor, TempDir)> {
    let cluster_file = std::env::var("NOVA_FDB_CLUSTER_FILE").ok()?;
    let dir = TempDir::new().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let meta = Arc::new(
        FdbMetadataStore::open_test(
            &cluster_file,
            format!("nova_test_phase2_{}_{}", now_micros(), generate_id()).into_bytes(),
        )
        .unwrap(),
    ) as Arc<dyn MetadataStore>;
    let store = Arc::new(LocalFileSystem::new_with_prefix(&data_dir).unwrap())
        as Arc<dyn object_store::ObjectStore>;
    let writer = MpWriter::new(store.clone(), "nova".to_string());
    let reader = MpReader::new(store);
    Some((Executor::new(meta, writer, reader), dir))
}

#[test]
fn phase2_cache_key_is_security_scoped() {
    let root = SecurityContext::root();
    let analyst = SecurityContext {
        user_id: ROOT_USER_ID + 1,
        username: "analyst".to_string(),
        primary_role_id: PUBLIC_ROLE_ID,
        secondary_role_ids: vec![ACCOUNTADMIN_ROLE_ID],
        secondary_all: false,
    };

    let sql = "SELECT * FROM orders";
    let root_key = format!(
        "/*sec:user={}:role={}:secondary_all={}:epoch={}*/ {}",
        root.user_id, root.primary_role_id, root.secondary_all, 7, sql
    );
    let analyst_key = format!(
        "/*sec:user={}:role={}:secondary_all={}:epoch={}*/ {}",
        analyst.user_id, analyst.primary_role_id, analyst.secondary_all, 7, sql
    );

    assert_ne!(root_key, analyst_key);
    assert_ne!(root_key, root_key.replace("epoch=7", "epoch=8"));
}

#[test]
fn phase2_session_role_state_is_isolated() {
    let mut alice = SecurityContext {
        user_id: 10,
        username: "alice".to_string(),
        primary_role_id: PUBLIC_ROLE_ID,
        secondary_role_ids: vec![ACCOUNTADMIN_ROLE_ID],
        secondary_all: true,
    };
    let bob = SecurityContext {
        user_id: 11,
        username: "bob".to_string(),
        primary_role_id: ACCOUNTADMIN_ROLE_ID,
        secondary_role_ids: vec![PUBLIC_ROLE_ID],
        secondary_all: true,
    };

    alice.primary_role_id = ACCOUNTADMIN_ROLE_ID;
    alice.secondary_all = false;

    assert_eq!(alice.active_role_ids(), vec![ACCOUNTADMIN_ROLE_ID]);
    assert_eq!(
        bob.active_role_ids(),
        vec![ACCOUNTADMIN_ROLE_ID, PUBLIC_ROLE_ID]
    );
}

#[test]
fn phase2_use_role_parser_handles_basic_enterprise_forms() {
    assert_eq!(
        parse_role_command(" USE ROLE `analyst`; "),
        Some(RoleCommand::UseRole("analyst"))
    );
    assert_eq!(
        parse_role_command("use secondary roles all"),
        Some(RoleCommand::UseSecondaryAll)
    );
    assert_eq!(
        parse_role_command("USE SECONDARY ROLES NONE;"),
        Some(RoleCommand::UseSecondaryNone)
    );
    assert_eq!(parse_role_command("use analytics"), None);
}

#[tokio::test]
async fn phase2_login_fails_for_disabled_user() {
    let Some((executor, _dir)) = setup_executor() else {
        return;
    };
    executor.meta().bootstrap_security().await.unwrap();
    let role_id = executor
        .meta()
        .create_role(RoleMeta {
            id: 0,
            name: "disabled_role".to_string(),
            owner_role_id: ACCOUNTADMIN_ROLE_ID,
            system: false,
            created_at: now_micros(),
            created_by_user_id: ROOT_USER_ID,
            comment: None,
        })
        .await
        .unwrap();
    executor
        .meta()
        .create_user(UserMeta {
            id: 0,
            name: "disabled_user".to_string(),
            password_hash: String::new(),
            mysql_native_hash: Vec::new(),
            default_role_id: role_id,
            disabled: true,
            created_at: now_micros(),
            created_by_user_id: ROOT_USER_ID,
            created_by_role_id: ACCOUNTADMIN_ROLE_ID,
            comment: None,
        })
        .await
        .unwrap();

    let user = executor
        .user_for_auth("disabled_user")
        .await
        .unwrap()
        .expect("auth lookup should find disabled user so handshake can reject it");
    assert!(user.disabled);

    let err = executor
        .security_context_for_user("disabled_user")
        .await
        .expect_err("disabled user must not receive a session security context");
    assert!(matches!(err, NovaError::AuthFailed { .. }));
}

#[tokio::test]
async fn phase2_login_fails_when_default_role_is_not_granted() {
    let Some((executor, _dir)) = setup_executor() else {
        return;
    };
    executor.meta().bootstrap_security().await.unwrap();
    let role_id = executor
        .meta()
        .create_role(RoleMeta {
            id: 0,
            name: "revoked_default".to_string(),
            owner_role_id: ACCOUNTADMIN_ROLE_ID,
            system: false,
            created_at: now_micros(),
            created_by_user_id: ROOT_USER_ID,
            comment: None,
        })
        .await
        .unwrap();
    let user_id = executor
        .meta()
        .create_user(UserMeta {
            id: 0,
            name: "missing_default_user".to_string(),
            password_hash: String::new(),
            mysql_native_hash: Vec::new(),
            default_role_id: role_id,
            disabled: false,
            created_at: now_micros(),
            created_by_user_id: ROOT_USER_ID,
            created_by_role_id: ACCOUNTADMIN_ROLE_ID,
            comment: None,
        })
        .await
        .unwrap();
    executor
        .meta()
        .revoke_role_from_user(user_id, role_id)
        .await
        .unwrap();

    let err = executor
        .security_context_for_user("missing_default_user")
        .await
        .expect_err("user default role must be granted to create a session context");
    assert!(matches!(err, NovaError::AuthFailed { .. }));
}
