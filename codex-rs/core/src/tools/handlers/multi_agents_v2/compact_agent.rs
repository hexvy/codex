use super::analytics::ToolCallAnalytics;
use super::*;
use crate::tools::handlers::multi_agents_spec::create_compact_agent_tool_v2;
use codex_tools::ToolSpec;

pub(crate) struct Handler;

impl ToolExecutor<ToolInvocation> for Handler {
    fn tool_name(&self) -> ToolName {
        ToolName::plain("compact_agent")
    }

    fn spec(&self) -> ToolSpec {
        create_compact_agent_tool_v2()
    }

    fn handle<'a>(&'a self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'a>
    where
        ToolInvocation: 'a,
    {
        Box::pin(async move {
            let mut analytics = ToolCallAnalytics::new(&invocation, CollabAgentTool::CompactAgent);
            let result = handle_compact_agent(invocation, &mut analytics).await;
            analytics.finish(&result);
            result.map(boxed_tool_output)
        })
    }
}

async fn handle_compact_agent(
    invocation: ToolInvocation,
    analytics: &mut ToolCallAnalytics,
) -> Result<CompactAgentResult, FunctionCallError> {
    let ToolInvocation {
        session,
        turn,
        payload,
        ..
    } = invocation;
    let arguments = function_arguments(payload)?;
    let args: CompactAgentArgs = parse_arguments(&arguments)?;
    let agent_id = resolve_agent_target(&session, &turn, &args.target).await?;
    analytics.set_receiver(agent_id);
    let submission_id = session
        .services
        .agent_control
        .compact(session.thread_id, agent_id)
        .await
        .map_err(|err| collab_v2_agent_error(agent_id, err))?;
    Ok(CompactAgentResult { submission_id })
}

impl CoreToolRuntime for Handler {
    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CompactAgentArgs {
    target: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct CompactAgentResult {
    pub(crate) submission_id: String,
}

impl ToolOutput for CompactAgentResult {
    fn log_output(&self) -> String {
        tool_output_json_text(self, "compact_agent")
    }

    fn success_for_logging(&self) -> bool {
        true
    }

    fn to_response_item(&self, call_id: &str, payload: &ToolPayload) -> ResponseInputItem {
        tool_output_response_item(call_id, payload, self, Some(true), "compact_agent")
    }

    fn code_mode_result(&self, _payload: &ToolPayload) -> JsonValue {
        tool_output_code_mode_result(self, "compact_agent")
    }
}
