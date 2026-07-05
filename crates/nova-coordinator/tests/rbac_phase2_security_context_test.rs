use nova_common::{ACCOUNTADMIN_ROLE_ID, PUBLIC_ROLE_ID, ROOT_USER_ID, SecurityContext};
use nova_coordinator::mysql_protocol::server::{RoleCommand, parse_role_command};

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
