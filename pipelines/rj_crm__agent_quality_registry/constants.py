import os

GITLAB_URL = os.getenv("AGENT_QUALITY_GITLAB_URL", "https://git.apps.rio.gov.br")
GITLAB_PROJECT_ID = os.getenv("AGENT_QUALITY_GITLAB_PROJECT_ID", "1589")
GITLAB_TOKEN = os.getenv("AGENT_QUALITY_GITLAB_TOKEN", "")
GITLAB_PACKAGE_QA = os.getenv("AGENT_QUALITY_CANDIDATE_PACKAGE_NAME", "agent-quality-candidates")
GITLAB_PACKAGE_PROD = os.getenv("AGENT_QUALITY_PROD_BASELINE_PACKAGE_NAME", "agent-quality-prod-baselines")

BQ_PROJECT_ID = os.getenv("AGENT_QUALITY_BQ_PROJECT_ID", "rj-crm-registry")
BQ_DATASET_ID = os.getenv("AGENT_QUALITY_BQ_DATASET_ID", "brutos_salesforce")
BQ_QA_TABLE = os.getenv("AGENT_QUALITY_BQ_QA_TABLE", "agent_quality_qa_versions")
BQ_PROD_TABLE = os.getenv("AGENT_QUALITY_BQ_PROD_TABLE", "agent_quality_prod_baselines")
BQ_DETAIL_TABLE = os.getenv("AGENT_QUALITY_BQ_DETAIL_TABLE", "agent_quality_test_result_details")

SF_INSTANCE_URL = os.getenv("AGENT_QUALITY_SF_INSTANCE_URL") or os.getenv("SF_INSTANCE_URL", "")
SF_CLIENT_ID = os.getenv("AGENT_QUALITY_SF_CLIENT_ID", "")
SF_CLIENT_SECRET = os.getenv("AGENT_QUALITY_SF_CLIENT_SECRET", "")
SF_TOKEN_ENDPOINT = os.getenv(
    "AGENT_QUALITY_SF_TOKEN_ENDPOINT",
    f"{SF_INSTANCE_URL.rstrip('/')}/services/oauth2/token" if SF_INSTANCE_URL else "",
)
# Transitional fallback only. Prefer Client Credentials so the flow never stores
# an expiring access token in Infisical.
SF_ACCESS_TOKEN = os.getenv("SF_ACCESS_TOKEN", "")
SF_API_VERSION = os.getenv("AGENT_QUALITY_SF_API_VERSION", "v66.0")

ARTIFACT_SCHEMA = "pref-rio.agent-quality-artifact.v1"

SchemaFields = list[tuple[str, str, str]]

AGGREGATE_FIELDS: SchemaFields = [
    ("release_key", "STRING", "REQUIRED"),
    ("environment_scope", "STRING", "REQUIRED"),
    ("registry_package_name", "STRING", "REQUIRED"),
    ("registry_package_version", "STRING", "REQUIRED"),
    ("package_id", "INT64", "NULLABLE"),
    ("package_file_id", "INT64", "NULLABLE"),
    ("artifact_sha256", "STRING", "NULLABLE"),
    ("artifact_generated_at", "TIMESTAMP", "NULLABLE"),
    ("tested_at", "TIMESTAMP", "NULLABLE"),
    ("promoted_at", "TIMESTAMP", "NULLABLE"),
    ("agent_version_id", "STRING", "NULLABLE"),
    ("agent_version_number", "INT64", "NULLABLE"),
    ("agent_version_status", "STRING", "NULLABLE"),
    ("gitlab_project_id", "STRING", "NULLABLE"),
    ("gitlab_project_path", "STRING", "NULLABLE"),
    ("pipeline_id", "STRING", "NULLABLE"),
    ("pipeline_url", "STRING", "NULLABLE"),
    ("job_id", "STRING", "NULLABLE"),
    ("job_url", "STRING", "NULLABLE"),
    ("commit_sha", "STRING", "NULLABLE"),
    ("merge_request_iid", "STRING", "NULLABLE"),
    ("source_branch", "STRING", "NULLABLE"),
    ("target_branch", "STRING", "NULLABLE"),
    ("routing_total", "INT64", "NULLABLE"),
    ("routing_pass", "INT64", "NULLABLE"),
    ("routing_fail", "INT64", "NULLABLE"),
    ("routing_pass_rate", "FLOAT64", "NULLABLE"),
    ("harness_scenarios_total", "INT64", "NULLABLE"),
    ("harness_scenarios_pass", "INT64", "NULLABLE"),
    ("harness_scenarios_fail", "INT64", "NULLABLE"),
    ("harness_scenarios_pass_rate", "FLOAT64", "NULLABLE"),
    ("harness_turns_total", "INT64", "NULLABLE"),
    ("harness_turns_pass", "INT64", "NULLABLE"),
    ("harness_turns_fail", "INT64", "NULLABLE"),
    ("harness_turns_pass_rate", "FLOAT64", "NULLABLE"),
    ("metric_counters_json", "STRING", "NULLABLE"),
    ("quality_summary_json", "STRING", "NULLABLE"),
    ("ingested_at", "TIMESTAMP", "REQUIRED"),
]

DETAIL_FIELDS: SchemaFields = [
    ("result_key", "STRING", "REQUIRED"),
    ("release_key", "STRING", "REQUIRED"),
    ("environment_scope", "STRING", "REQUIRED"),
    ("test_source", "STRING", "REQUIRED"),
    ("tested_at", "TIMESTAMP", "NULLABLE"),
    ("agent_version_number", "INT64", "NULLABLE"),
    ("grid_run_id", "STRING", "NULLABLE"),
    ("grid_workbook_id", "STRING", "NULLABLE"),
    ("grid_worksheet_id", "STRING", "NULLABLE"),
    ("suite_name", "STRING", "NULLABLE"),
    ("runtime_suite_name", "STRING", "NULLABLE"),
    ("case_number", "STRING", "NULLABLE"),
    ("worksheet_row_id", "STRING", "NULLABLE"),
    ("assertion", "STRING", "NULLABLE"),
    ("scenario_id", "STRING", "NULLABLE"),
    ("service_name", "STRING", "NULLABLE"),
    ("turn_number", "INT64", "NULLABLE"),
    ("status", "STRING", "NULLABLE"),
    ("score", "FLOAT64", "NULLABLE"),
    ("latency_ms", "INT64", "NULLABLE"),
    ("expected_value", "STRING", "NULLABLE"),
    ("actual_value", "STRING", "NULLABLE"),
    ("message", "STRING", "NULLABLE"),
    ("judge_provider", "STRING", "NULLABLE"),
    ("judge_pass", "BOOL", "NULLABLE"),
    ("judge_rationale", "STRING", "NULLABLE"),
    ("safety_severity", "STRING", "NULLABLE"),
    ("safety_type", "STRING", "NULLABLE"),
    ("utterance", "STRING", "NULLABLE"),
    ("response", "STRING", "NULLABLE"),
]
