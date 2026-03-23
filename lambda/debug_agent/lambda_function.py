import base64
import gzip
import hashlib
import json
import os
import uuid
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

s3 = boto3.client("s3")
bedrock = boto3.client(
    "bedrock-runtime",
    region_name="us-east-2",
    config=Config(connect_timeout=3, read_timeout=20, retries={"max_attempts": 1, "mode": "standard"}),
)

RCA_BUCKET = os.environ.get("RCA_BUCKET", "xxxxx")
RCA_PREFIX = os.environ.get("RCA_PREFIX", "rca-reports/")
CACHE_PREFIX = os.environ.get("CACHE_PREFIX", "rca-cache/")
BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "xxxxx")
ENABLE_BEDROCK = os.environ.get("ENABLE_BEDROCK", "true").lower() == "true"
MAX_NEW_TOKENS = 220


def classify_rule_based(text: str) -> str:
    normalized = (text or "").lower()
    if "null value in column" in normalized or "not-null constraint" in normalized:
        return "null_constraint_violation"
    if "duplicate key value violates unique constraint" in normalized:
        return "duplicate_key"
    if "invalid input syntax" in normalized or "could not convert string to float" in normalized:
        return "type_casting_issue"
    if "syntax error at or near" in normalized:
        return "sql_syntax_error"
    if "connection timed out" in normalized or "could not connect" in normalized:
        return "network_failure"
    if "password authentication failed" in normalized:
        return "credentials_issue"
    if "nosuchkey" in normalized or "specified key does not exist" in normalized:
        return "s3_missing_object"
    return "unknown"


def parse_event(event: dict):
    if "awslogs" in event and "data" in event["awslogs"]:
        payload = json.loads(gzip.decompress(base64.b64decode(event["awslogs"]["data"])).decode("utf-8"))
        log_group = payload.get("logGroup", "")
        log_stream = payload.get("logStream", "")
        messages = [entry.get("message", "") for entry in payload.get("logEvents", [])]
        return log_group, log_stream, messages

    message = event.get("message", "ETL_ERROR: manual smoke test")
    return "manual-test", "manual-stream", [message]


def extract_signal(messages):
    joined = "\n".join(messages)
    lines = joined.splitlines()
    important = [
        line
        for line in lines
        if (
            "ETL_ERROR" in line
            or "ERROR" in line
            or "Traceback" in line
            or "Exception" in line
            or "NoSuchKey" in line
        )
    ]
    if not important:
        important = lines[-20:]
    return "\n".join(important)[-1200:]


def deterministic_fix(category: str) -> dict:
    mapping = {
        "null_constraint_violation": {
            "root_cause": "A required database field is null in the incoming dataset.",
            "suggested_fix": "Skip rows with missing primary keys before insert and log the rejected count.",
        },
        "duplicate_key": {
            "root_cause": "A previously seen primary key is being loaded again.",
            "suggested_fix": "Keep deduplication enabled and preserve ON CONFLICT handling in the ETL load.",
        },
        "s3_missing_object": {
            "root_cause": "The configured input object path does not exist in S3.",
            "suggested_fix": "Verify the S3 key and confirm the source file is uploaded to the expected prefix.",
        },
    }
    fallback = mapping.get(
        category,
        {
            "root_cause": "The failure was captured, but only rule-based context is available.",
            "suggested_fix": "Review the log sample and apply the category-specific remediation.",
        },
    )
    return {
        "failure_type": category,
        "root_cause": fallback["root_cause"],
        "suggested_fix": fallback["suggested_fix"],
        "confidence_score": "0.70",
    }


def should_call_llm(category: str) -> bool:
    return category in {
        "unknown",
        "network_failure",
        "sql_syntax_error",
        "type_casting_issue",
        "credentials_issue",
    }


def call_bedrock(signal_text: str, category: str) -> dict:
    prompt = f"""
You are an ETL root-cause and fix assistant.
Return STRICT JSON only:
{{
  "failure_type": "...",
  "root_cause": "...",
  "suggested_fix": "...",
  "confidence_score": "0.00-1.00"
}}

Rule category: {category}
Log signal:
{signal_text}
""".strip()

    body = {
        "messages": [{"role": "user", "content": [{"text": prompt}]}],
        "inferenceConfig": {"max_new_tokens": MAX_NEW_TOKENS, "temperature": 0.1},
    }

    response = bedrock.invoke_model(
        modelId=BEDROCK_MODEL_ID,
        contentType="application/json",
        accept="application/json",
        body=json.dumps(body),
    )
    payload = json.loads(response["body"].read())
    text = payload["output"]["message"]["content"][0]["text"]
    return json.loads(text)


def get_cache(signature: str):
    key = f"{CACHE_PREFIX}{signature}.json"
    try:
        obj = s3.get_object(Bucket=RCA_BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except ClientError:
        return None


def put_cache(signature: str, value: dict):
    key = f"{CACHE_PREFIX}{signature}.json"
    s3.put_object(
        Bucket=RCA_BUCKET,
        Key=key,
        Body=json.dumps(value).encode("utf-8"),
        ContentType="application/json",
    )


def lambda_handler(event, context):
    log_group, log_stream, messages = parse_event(event)
    joined = "\n".join(messages)
    category = classify_rule_based(joined)
    signal = extract_signal(messages)

    signature = hashlib.sha256(f"{category}|{signal}".encode("utf-8")).hexdigest()[:32]
    cached = get_cache(signature)

    llm_status = "not_called"
    result = deterministic_fix(category)

    if cached:
        llm_status = "cache_hit"
        result = cached
    elif ENABLE_BEDROCK and BEDROCK_MODEL_ID != "xxxxx" and should_call_llm(category):
        try:
            result = call_bedrock(signal, category)
            llm_status = "ok"
            put_cache(signature, result)
        except Exception as exc:
            llm_status = "throttled_or_failed"
            print("BEDROCK_ERROR:", str(exc))

    now = datetime.now(timezone.utc)
    report_key = f"{RCA_PREFIX}{now:%Y/%m/%d}/rca_{now:%H%M%S}_{uuid.uuid4().hex[:8]}.json"
    report = {
        "timestamp_utc": now.isoformat(),
        "log_group": log_group,
        "log_stream": log_stream,
        "event_count": len(messages),
        "rule_based_category": category,
        "llm_status": llm_status,
        "result": result,
        "sample_log_tail": joined[-4000:],
    }

    s3.put_object(
        Bucket=RCA_BUCKET,
        Key=report_key,
        Body=json.dumps(report, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

    print("RCA_SAVED", f"s3://{RCA_BUCKET}/{report_key}", "LLM_STATUS", llm_status)
    return {
        "statusCode": 200,
        "s3_key": report_key,
        "rule_based_category": category,
        "llm_status": llm_status,
    }
