//! Child history survives interruption and pending mail survives eviction.

use super::*;
use crate::suite::settings_commits::COMMITTED_MODEL;
use crate::suite::settings_commits::PauseAfterCommit;
use codex_core::config::Config;
use codex_extension_api::ExtensionRegistryBuilder;
use codex_extension_api::MessageLifecycleContributor;
use codex_extension_api::MessageLifecycleInput;
use codex_protocol::ThreadId;
use codex_protocol::items::TurnItem;
use codex_protocol::protocol::AgentStatus;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::TurnAbortReason;
use core_test_support::ThreadIdle;
use core_test_support::streaming_sse::StreamingSseChunk;
use core_test_support::streaming_sse::start_streaming_sse_server;
use pretty_assertions::assert_eq;
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc;
use test_case::test_case;
use tokio::sync::oneshot;

#[derive(Default)]
struct Messages(Mutex<HashSet<uuid::Uuid>>);
impl MessageLifecycleContributor for Messages {
    fn on_publish(&self, input: &MessageLifecycleInput) {
        let mut pending = self.0.lock().expect("messages lock");
        let token = uuid::Uuid::new_v4();
        input.data.insert(token);
        assert!(pending.insert(token));
    }
    fn on_consumed(&self, input: &MessageLifecycleInput, _recipient: ThreadId) {
        let mut pending = self.0.lock().expect("messages lock");
        let token = input.data.remove::<uuid::Uuid>().expect("published token");
        assert!(pending.remove(token.as_ref()));
    }
}

#[test_case(ev_assistant_message("sampled", "Keep this answer."); "message")]
#[test_case(ev_function_call_with_namespace("sampled", MULTI_AGENT_V2_NAMESPACE, "send_message",
    r#"{"target":"/root","message":"Should never be sent."}"#); "tool")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupt_retains_one_sampled_item(mut done: serde_json::Value) -> Result<()> {
    skip_if_no_network!(Ok(()));
    done["item"]["id"] = json!("sampled");
    if done["item"]["type"] == "message" {
        done["item"]["phase"] = json!("final_answer");
    }
    let chunk = |gate, events| StreamingSseChunk {
        gate,
        body: sse(events),
    };
    let complete = |id| vec![chunk(None, vec![ev_completed(id)])];
    let call =
        |name, args| ev_function_call_with_namespace(name, MULTI_AGENT_V2_NAMESPACE, name, args);
    let (spawned, spawn_gate) = oneshot::channel();
    let (release_item, item_gate) = oneshot::channel();
    let (_finish_response, response_gate) = oneshot::channel();
    let (server, _) = start_streaming_sse_server(vec![
        vec![
            chunk(
                None,
                vec![call(
                    "spawn_agent",
                    r#"{"task_name":"worker","message":"Work","fork_turns":"none"}"#,
                )],
            ),
            chunk(Some(spawn_gate), vec![ev_completed("spawn")]),
        ],
        vec![
            chunk(None, vec![ev_response_created("child")]),
            chunk(
                Some(item_gate),
                vec![done.clone(), ev_assistant_message("later", "Discard this.")],
            ),
            chunk(Some(response_gate), vec![ev_completed("child")]),
        ],
        vec![chunk(
            None,
            vec![
                call("interrupt_agent", r#"{"target":"worker"}"#),
                ev_completed("interrupt"),
            ],
        )],
        complete("interrupt-result"),
        complete("resume"),
    ])
    .await;
    let test = test_codex()
        .with_model("gpt-5.6-sol")
        .with_config(|config| {
            config
                .features
                .enable(Feature::MultiAgentV2)
                .expect("enable the parent collaboration tools");
            config
                .features
                .disable(Feature::EnableRequestCompression)
                .expect("capture uncompressed request bodies");
            config
                .features
                .disable(Feature::WakeOnAgentMessages)
                .expect("keep the parent idle after interrupting the child");
            config.multi_agent_v2.interrupt_at_item_boundary = true;
        })
        .build_with_streaming_server(&server)
        .await?;
    let mut created = test.thread_manager.subscribe_thread_created();
    let (spawn, ()) = tokio::join!(test.submit_turn(FIRST_PROMPT), async {
        server.wait_for_request_count(/*count*/ 2).await;
        spawned
            .send(())
            .expect("release the parent after the child starts sampling");
    });
    spawn?;
    let child = test
        .thread_manager
        .get_thread(created.recv().await?)
        .await?;
    // The settings reply confirms the child accepted the interrupt while its item is held.
    child
        .update_thread_settings(ThreadSettingsOverrides::default())
        .await?;
    wait_for_event(&child, |event| matches!(event, EventMsg::TurnStarted(_))).await;
    let resumed = child.start_or_steer_turn(codex_core::TurnInputRequest::user_input(vec![
        codex_protocol::user_input::UserInput::Text {
            text: "Resume.".into(),
            text_elements: vec![],
        },
    ]));
    tokio::pin!(resumed);
    assert!(futures::poll!(resumed.as_mut()).is_pending());
    assert_eq!(child.agent_status().await, AgentStatus::Running);
    release_item.send(()).expect("release the sampled item");
    let next = |event: &EventMsg| match event {
        EventMsg::RawResponseItem(raw) => raw.item.id().is_some_and(|id| id.as_str() == "sampled"),
        EventMsg::TurnStarted(_) | EventMsg::TurnAborted(_) | EventMsg::TurnComplete(_) => true,
        _ => false,
    };
    assert!(matches!(
        wait_for_event(&child, next).await,
        EventMsg::RawResponseItem(_)
    ));
    assert!(matches!(
        wait_for_event(&child, next).await,
        EventMsg::TurnAborted(event) if event.reason == TurnAbortReason::Interrupted
    ));
    assert!(matches!(
        resumed.await?,
        codex_core::TurnInputSubmission::Started { .. }
    ));
    wait_for_event(&child, |event| matches!(event, EventMsg::TurnStarted(_))).await;
    wait_for_event(&child, |event| matches!(event, EventMsg::TurnComplete(_))).await;
    let requests = server.requests().await;
    assert_eq!(requests.len(), 5);
    let first: serde_json::Value = serde_json::from_slice(&requests[1])?;
    let resumed: serde_json::Value = serde_json::from_slice(&requests[4])?;
    let prefix = first["input"].as_array().context("child input")?;
    let items = resumed["input"].as_array().context("resumed input")?;
    assert_eq!(items.get(..prefix.len()), Some(prefix.as_slice()));
    assert_eq!(
        items
            .iter()
            .filter(|item| item["role"] == "user"
                && item["content"] == json!([{"type": "input_text", "text": "Resume."}]))
            .count(),
        1
    );
    let retained = &items[prefix.len()];
    if done["item"]["type"] == "message" {
        assert_eq!(retained["content"], done["item"]["content"]);
        assert_eq!(retained["phase"], done["item"]["phase"]);
    } else {
        assert_eq!(retained["arguments"], done["item"]["arguments"]);
        assert_eq!(retained["call_id"], "sampled");
        let output = &items[prefix.len() + 1];
        assert_eq!(output["call_id"], "sampled");
        assert_eq!(output["output"], "aborted before dispatch");
    }
    assert!(
        !items
            .iter()
            .any(|item| item.to_string().contains("Discard this."))
    );
    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_mail_survives_eviction_and_preserves_followup_order() -> Result<()> {
    const MAIL: &str = "Keep this note for later";
    const LATE_MAIL: &str = "Another note while unloaded";
    const FOLLOWUP: &str = "Read the pending note";
    const AFTER_FOLLOWUP: &str = "This note must follow the new task";
    let server = start_mock_server().await;
    mount_root_collaboration_call(
        &server,
        FIRST_PROMPT,
        "first-call",
        "spawn_agent",
        json!({ "message": FIRST_TASK, "task_name": "first", "fork_turns": "none" }),
    )
    .await;
    mount_completed_worker(&server, FIRST_TASK, "first-call").await;
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut extensions = ExtensionRegistryBuilder::<Config>::new();
    extensions.config_contributor(Arc::new(PauseAfterCommit {
        gate: Mutex::new(Some((entered_tx, release_rx))),
    }));
    extensions.thread_lifecycle_contributor(Arc::new(ThreadIdle));
    let messages = Arc::new(Messages::default());
    extensions.message_lifecycle_contributor(messages.clone());
    let test = test_codex()
        .with_model("gpt-5.6-sol")
        .with_extensions(Arc::new(extensions.build()))
        .with_config(|config| {
            config.features.enable(Feature::Collab).unwrap();
            config.features.enable(Feature::MultiAgentV2).unwrap();
            config.multi_agent_v2.max_concurrent_threads_per_session = 2;
            config.multi_agent_v2.hide_spawn_agent_metadata = true;
        })
        .build_with_auto_env(&server)
        .await?;
    let mut created = test.thread_manager.subscribe_thread_created();
    test.submit_turn(FIRST_PROMPT).await?;
    ThreadIdle::wait(&test.codex).await;
    let first_id = created.recv().await?;
    let first = test.thread_manager.get_thread(first_id).await?;
    wait_for_event(&first, |event| matches!(event, EventMsg::TurnComplete(_))).await;
    ThreadIdle::wait(&first).await;

    mount_root_collaboration_call(
        &server,
        "send a note",
        "queued-mail",
        "send_message",
        json!({ "target": "first", "message": MAIL }),
    )
    .await;
    test.submit_turn("send a note").await?;
    ThreadIdle::wait(&test.codex).await;
    // Wait for delivery through the submission queue before attempting eviction.
    submit_thread_settings(&first, ThreadSettingsOverrides::default()).await?;
    assert_eq!(
        first.agent_status().await,
        AgentStatus::Completed(Some("worker completed".into()))
    );
    drop(first);

    mount_root_collaboration_call(
        &server,
        "spawn while child is idle",
        "replacement-spawn",
        "spawn_agent",
        json!({ "message": SECOND_TASK, "task_name": "replacement", "fork_turns": "none" }),
    )
    .await;
    mount_completed_worker(&server, SECOND_TASK, "replacement-spawn").await;
    test.submit_turn("spawn while child is idle").await?;
    ThreadIdle::wait(&test.codex).await;
    let replacement_id = created.recv().await?;
    let replacement = test.thread_manager.get_thread(replacement_id).await?;
    wait_for_event(&replacement, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;
    ThreadIdle::wait(&replacement).await;
    assert!(test.thread_manager.get_thread(first_id).await.is_err());
    assert_eq!(test.thread_manager.list_thread_ids().await.len(), 2);
    mount_root_collaboration_call(
        &server,
        "send while unloaded",
        "late-mail",
        "send_message",
        json!({ "target": "first", "message": LATE_MAIL }),
    )
    .await;
    test.submit_turn("send while unloaded").await?;
    ThreadIdle::wait(&test.codex).await;
    assert!(test.thread_manager.get_thread(first_id).await.is_err());

    let retained = messages.0.lock().unwrap().clone();
    assert_eq!(retained.len(), 2);
    // Reload without starting a turn, then hold dispatch while both tools submit.
    // The older queued notes must precede the follow-up, and the newer note must follow it.
    test.thread_manager
        .ensure_multi_agent_v2_child_loaded(first_id)
        .await?;
    let resumed = test.thread_manager.get_thread(first_id).await?;
    resumed
        .submit(Op::ThreadSettings {
            thread_settings: ThreadSettingsOverrides {
                model: Some(COMMITTED_MODEL.to_string()),
                ..Default::default()
            },
            reply: None,
        })
        .await?;
    entered_rx.await?;
    mount_root_collaboration_call(
        &server,
        "drain the note",
        "followup",
        "followup_task",
        json!({ "target": "first", "message": FOLLOWUP }),
    )
    .await;
    let (at_sampling, pending_at_sampling) = mpsc::channel();
    mount_sse_once_match(
        &server,
        move |request: &wiremock::Request| {
            let matches =
                body_contains(request, FOLLOWUP) && !has_function_call_output(request, "followup");
            if matches {
                at_sampling
                    .send(messages.0.lock().unwrap().clone())
                    .unwrap();
            }
            matches
        },
        sse(vec![
            ev_response_created("worker-wait"),
            ev_function_call_with_namespace(
                "wait-note",
                MULTI_AGENT_V2_NAMESPACE,
                "wait_agent",
                r#"{"timeout_ms":1}"#,
            ),
            ev_completed("worker-wait"),
        ]),
    )
    .await;
    let delivered = mount_sse_once_match(
        &server,
        |request: &wiremock::Request| has_function_call_output(request, "wait-note"),
        sse(vec![
            ev_assistant_message("done", "Read all notes"),
            ev_completed("done"),
        ]),
    )
    .await;
    test.submit_turn("drain the note").await?;
    ThreadIdle::wait(&test.codex).await;
    mount_root_collaboration_call(
        &server,
        "send after followup",
        "after-followup",
        "send_message",
        json!({ "target": "first", "message": AFTER_FOLLOWUP }),
    )
    .await;
    test.submit_turn("send after followup").await?;
    ThreadIdle::wait(&test.codex).await;
    release_tx.send(())?;
    wait_for_event(&resumed, |event| matches!(event, EventMsg::TurnComplete(_))).await;
    ThreadIdle::wait(&resumed).await;
    assert!(
        pending_at_sampling.try_recv()?.is_disjoint(&retained),
        "both retained publications must be consumed before the first resumed sample"
    );
    let request = delivered
        .requests()
        .into_iter()
        .find(|request| request.function_call_output_text("wait-note").is_some())
        .expect("worker continuation after waiting for mail");
    let messages = request.inputs_of_type("agent_message");
    let expected = [MAIL, LATE_MAIL, FOLLOWUP, AFTER_FOLLOWUP];
    let received = messages
        .iter()
        .filter_map(|item| {
            expected
                .iter()
                .find(|text| item.to_string().contains(**text))
                .copied()
        })
        .collect::<Vec<_>>();
    assert_eq!(received, expected);
    assert_eq!(test.thread_manager.list_thread_ids().await.len(), 2);

    test.thread_manager
        .shutdown_all_threads_bounded(Duration::from_secs(5))
        .await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compact_active_child_returns_before_compaction_and_preserves_mail() -> Result<()> {
    skip_if_no_network!(Ok(()));

    const SPAWN_PROMPT: &str = "start a worker before compacting it";
    const COMPACT_PROMPT: &str = "compact the active worker";
    const QUEUE_PROMPT: &str = "send the worker a note during compaction";
    const FOLLOWUP_PROMPT: &str = "resume the worker after compaction";
    const MAIL: &str = "Keep this note for the next task";
    const FOLLOWUP: &str = "Continue checking the repository";
    const SPAWN_CALL_ID: &str = "spawn-before-compact";
    const COMPACT_CALL_ID: &str = "compact-active-worker";
    const SEND_CALL_ID: &str = "queue-during-compact";
    const FOLLOWUP_CALL_ID: &str = "resume-after-compact";

    let chunk = |gate, events| StreamingSseChunk {
        gate,
        body: sse(events),
    };
    let call = |call_id, name, arguments: Value| {
        ev_function_call_with_namespace(
            call_id,
            MULTI_AGENT_V2_NAMESPACE,
            name,
            &arguments.to_string(),
        )
    };
    let answer = |id, text| {
        let mut event = ev_assistant_message(id, text);
        event["item"]["phase"] = json!("final_answer");
        event
    };
    let answer_response = |id, text| {
        vec![chunk(
            None,
            vec![ev_response_created(id), answer(id, text), ev_completed(id)],
        )]
    };
    let gated_answer = |gate, id, text| {
        vec![
            chunk(None, vec![ev_response_created(id)]),
            chunk(Some(gate), vec![answer(id, text), ev_completed(id)]),
        ]
    };
    let call_response = |response_id, call_id, name, arguments, gate| {
        vec![
            chunk(
                None,
                vec![
                    ev_response_created(response_id),
                    call(call_id, name, arguments),
                ],
            ),
            chunk(gate, vec![ev_completed(response_id)]),
        ]
    };

    let (release_spawn, spawn_gate) = oneshot::channel();
    let (release_active_child, active_child_gate) = oneshot::channel();
    let (release_compaction_response_a, compaction_response_gate_a) = oneshot::channel();
    let (release_compaction_response_b, compaction_response_gate_b) = oneshot::channel();
    let (server, _) = start_streaming_sse_server(vec![
        call_response(
            "spawn-response",
            SPAWN_CALL_ID,
            "spawn_agent",
            json!({"task_name": "worker", "message": "Inspect the repository", "fork_turns": "none"}),
            Some(spawn_gate),
        ),
        gated_answer(active_child_gate, "active-child-turn", "The worker is still working."),
        answer_response("spawn-finished", "The worker started."),
        call_response(
            "compact-response",
            COMPACT_CALL_ID,
            "compact_agent",
            json!({"target": "worker"}),
            None,
        ),
        gated_answer(
            compaction_response_gate_a,
            "compact-or-owner-a",
            "The operation is complete.",
        ),
        gated_answer(
            compaction_response_gate_b,
            "compact-or-owner-b",
            "The operation is complete.",
        ),
        call_response(
            "send-response",
            SEND_CALL_ID,
            "send_message",
            json!({"target": "worker", "message": MAIL}),
            None,
        ),
        answer_response("send-finished", "The note is queued."),
        call_response(
            "followup-response",
            FOLLOWUP_CALL_ID,
            "followup_task",
            json!({"target": "worker", "message": FOLLOWUP}),
            None,
        ),
        answer_response("followup-owner-a", "The worker received the note."),
        answer_response("followup-owner-b", "The worker received the note."),
    ])
    .await;

    let mut extensions = ExtensionRegistryBuilder::<Config>::new();
    extensions.thread_lifecycle_contributor(Arc::new(ThreadIdle));
    let test = test_codex()
        .with_model("gpt-5.6-sol")
        .with_extensions(Arc::new(extensions.build()))
        .with_config(|config| {
            config
                .features
                .enable(Feature::Collab)
                .expect("enable collaboration tools");
            config
                .features
                .enable(Feature::MultiAgentV2)
                .expect("enable v2 collaboration tools");
            config
                .features
                .disable(Feature::WakeOnAgentMessages)
                .expect("keep queue-only mail from waking the child");
            config
                .features
                .disable(Feature::EnableRequestCompression)
                .expect("capture uncompressed model requests");
            config.model_provider.name = "OpenAI-compatible test provider".to_string();
        })
        .build_with_streaming_server(&server)
        .await?;

    let mut created = test.thread_manager.subscribe_thread_created();
    let spawn_turn = test.submit_turn(SPAWN_PROMPT);
    let (spawn_result, ()) = tokio::join!(spawn_turn, async {
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            server.wait_for_request_count(2),
        )
        .await
        .expect("spawn should start the child model request");
        release_spawn
            .send(())
            .expect("release root after child request starts");
    });
    spawn_result?;
    ThreadIdle::wait(&test.codex).await;
    let child = test
        .thread_manager
        .get_thread(created.recv().await?)
        .await?;
    wait_for_event(&child, |event| matches!(event, EventMsg::TurnStarted(_))).await;
    assert_eq!(child.agent_status().await, AgentStatus::Running);

    let compact_turn = test.submit_turn(COMPACT_PROMPT);
    tokio::pin!(compact_turn);
    {
        let requests_ready = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            server.wait_for_request_count(6),
        );
        tokio::pin!(requests_ready);
        tokio::select! {
            result = compact_turn.as_mut() => {
                panic!("owner compact turn finished before gated child compaction: {result:?}");
            }
            result = requests_ready.as_mut() => {
                result.expect("compact submission should start child compaction and return to owner");
            }
        }
    }

    let requests = server
        .requests()
        .await
        .into_iter()
        .map(|request| serde_json::from_slice::<Value>(&request).expect("model request JSON"))
        .collect::<Vec<_>>();
    let (owner_request_index, owner_output) = requests
        .iter()
        .enumerate()
        .skip(4)
        .take(2)
        .find_map(|(index, request)| {
            request["input"].as_array()?.iter().find_map(|item| {
                (item["type"] == "function_call_output" && item["call_id"] == COMPACT_CALL_ID)
                    .then(|| item["output"].as_str())
                    .flatten()
                    .map(|output| (index, output))
            })
        })
        .expect("owner continuation should receive the compact submission result");
    let submission: Value =
        serde_json::from_str(owner_output).expect("compact_agent returns a JSON submission result");
    let submission_id = submission["submission_id"]
        .as_str()
        .filter(|submission_id| !submission_id.is_empty())
        .expect("compact_agent returns its submission_id")
        .to_string();

    let mut compaction_gates = [
        Some(release_compaction_response_a),
        Some(release_compaction_response_b),
    ];
    let owner_gate_index = owner_request_index - 4;
    compaction_gates[owner_gate_index]
        .take()
        .expect("owner response gate")
        .send(())
        .expect("release owner response while child compaction stays gated");
    let compaction_gate = compaction_gates[1 - owner_gate_index]
        .take()
        .expect("child compaction response gate");

    let aborted = wait_for_event(&child, |event| {
        matches!(event, EventMsg::TurnAborted(event) if event.reason == TurnAbortReason::Replaced)
    })
    .await;
    assert!(matches!(aborted, EventMsg::TurnAborted(_)));
    release_active_child
        .send(())
        .expect("release the replaced child response");
    let (compact_turn_id, compact_item_id) =
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let mut turn_started_id = None;
            let mut compaction_item_id = None;
            while turn_started_id.is_none() || compaction_item_id.is_none() {
                let event = child.next_event().await.expect("child event stream");
                let event_id = event.id.clone();
                match event.msg {
                    EventMsg::TurnStarted(_) => turn_started_id = Some(event_id),
                    EventMsg::ItemStarted(item)
                        if matches!(item.item, TurnItem::ContextCompaction(_)) =>
                    {
                        compaction_item_id = Some(event_id);
                    }
                    EventMsg::Error(error) => panic!("child compaction failed: {error:?}"),
                    _ => {}
                }
            }
            (
                turn_started_id.expect("manual compact turn started"),
                compaction_item_id.expect("context compaction started"),
            )
        })
        .await
        .expect("child should enter manual compaction");
    assert_eq!(compact_turn_id, submission_id);
    assert_eq!(compact_item_id, submission_id);

    compact_turn.as_mut().await?;
    ThreadIdle::wait(&test.codex).await;
    assert_eq!(child.agent_status().await, AgentStatus::Running);

    test.submit_turn(QUEUE_PROMPT).await?;
    ThreadIdle::wait(&test.codex).await;
    assert_eq!(child.agent_status().await, AgentStatus::Running);
    assert_eq!(server.requests().await.len(), 8);

    compaction_gate
        .send(())
        .expect("release manual compaction after queueing mail");
    let compact_completed_id = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let event = child.next_event().await.expect("child event stream");
            if matches!(&event.msg, EventMsg::TurnComplete(_)) {
                break event.id;
            }
            if let EventMsg::Error(error) = &event.msg {
                panic!("child compaction failed: {error:?}");
            }
        }
    })
    .await
    .expect("manual compaction should finish after its response is released");
    assert_eq!(compact_completed_id, submission_id);
    ThreadIdle::wait(&child).await;

    test.submit_turn(FOLLOWUP_PROMPT).await?;
    ThreadIdle::wait(&test.codex).await;
    wait_for_event(&child, |event| matches!(event, EventMsg::TurnComplete(_))).await;
    ThreadIdle::wait(&child).await;

    let requests = server
        .requests()
        .await
        .into_iter()
        .map(|request| serde_json::from_slice::<Value>(&request).expect("model request JSON"))
        .collect::<Vec<_>>();
    let child_followup = requests
        .iter()
        .find(|request| {
            request["input"].as_array().is_some_and(|items| {
                items
                    .iter()
                    .any(|item| item["type"] == "agent_message" && item.to_string().contains(MAIL))
                    && items.iter().any(|item| {
                        item["type"] == "agent_message" && item.to_string().contains(FOLLOWUP)
                    })
            })
        })
        .expect("the worker's later task should receive queued mail");
    let input = child_followup["input"]
        .as_array()
        .expect("child follow-up input");
    assert_eq!(
        input
            .iter()
            .filter(|item| item["type"] == "agent_message" && item.to_string().contains(MAIL))
            .count(),
        1,
        "queued mail should be delivered once"
    );
    assert_eq!(
        input
            .iter()
            .filter(|item| {
                item["type"] == "agent_message" && item.to_string().contains(FOLLOWUP)
            })
            .count(),
        1,
        "follow-up task should be delivered once"
    );

    test.thread_manager
        .shutdown_all_threads_bounded(std::time::Duration::from_secs(5))
        .await;
    server.shutdown().await;
    Ok(())
}
