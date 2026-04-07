"""Shared Lambda dispatch machinery for magg pipelines."""

import base64
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Lambda pricing (us-west-2)
LAMBDA_PRICE_X86 = 0.0000166667  # per GB-second
LAMBDA_PRICE_ARM = 0.0000133334  # per GB-second (20% cheaper)


def parse_billed_duration(log_result_b64: str) -> int | None:
    """Extract billed duration in ms from Lambda's base64-encoded log tail."""
    if not log_result_b64:
        return None
    try:
        log_text = base64.b64decode(log_result_b64).decode("utf-8", errors="replace")
        match = re.search(r"Billed Duration: (\d+) ms", log_text)
        if match:
            return int(match.group(1))
    except Exception:
        pass
    return None


def parse_max_memory(log_result_b64: str) -> int | None:
    """Extract max memory used in MB from Lambda's base64-encoded log tail."""
    if not log_result_b64:
        return None
    try:
        log_text = base64.b64decode(log_result_b64).decode("utf-8", errors="replace")
        match = re.search(r"Max Memory Used:\s*(\d+)\s*MB", log_text)
        if match:
            return int(match.group(1))
    except Exception:
        pass
    return None


def detect_architecture(lambda_client, function_name: str) -> tuple[str, float]:
    """Detect Lambda architecture and return ``(arch, price_per_gb_second)``."""
    try:
        response = lambda_client.get_function(FunctionName=function_name)
        architectures = response.get("Configuration", {}).get("Architectures", ["x86_64"])
        arch = architectures[0] if architectures else "x86_64"
        price = LAMBDA_PRICE_ARM if arch == "arm64" else LAMBDA_PRICE_X86
        return arch, price
    except Exception:
        return "x86_64", LAMBDA_PRICE_X86


def _invoke_one(lambda_client, function_name, key, event, max_retries=3):
    """Invoke a single Lambda function with retry on throttling."""
    wall_start = time.time()
    last_error = None

    for attempt in range(max_retries):
        try:
            response = lambda_client.invoke(
                FunctionName=function_name,
                InvocationType="RequestResponse",
                LogType="Tail",
                Payload=json.dumps(event),
            )

            function_error = response.get("FunctionError")
            payload_bytes = response["Payload"].read()

            if function_error:
                error_payload = payload_bytes.decode("utf-8")
                if "Task timed out" not in error_payload:
                    last_error = f"Lambda error ({function_error}): {error_payload[:200]}"
                    continue
                last_error = f"Lambda timeout: {error_payload[:200]}"

            result = json.loads(payload_bytes) if not function_error else {}
            log_b64 = response.get("LogResult", "")
            billed_ms = parse_billed_duration(log_b64)
            max_mem = parse_max_memory(log_b64)

            return {
                "key": key,
                "payload": result,
                "wall_time": time.time() - wall_start,
                "billed_ms": billed_ms,
                "max_memory_mb": max_mem,
                "error": last_error if function_error else None,
                "retries": attempt,
            }

        except Exception as e:
            last_error = str(e)
            retryable = [
                "TooManyRequestsException", "Rate exceeded",
                "Read timeout", "timed out", "UNEXPECTED_EOF",
            ]
            if any(x in last_error for x in retryable):
                time.sleep((2 ** attempt) + (time.time() % 1))
            else:
                break

    return {
        "key": key,
        "payload": {},
        "wall_time": time.time() - wall_start,
        "billed_ms": None,
        "max_memory_mb": None,
        "error": last_error,
        "retries": max_retries,
    }


def dispatch_lambda(
    events,
    function_name,
    *,
    region="us-west-2",
    max_workers=1000,
    max_retries=3,
    on_result=None,
    log_interval=50,
):
    """Dispatch Lambda invocations in parallel and collect results.

    Parameters
    ----------
    events : list of (key, event_dict)
        Each entry is a ``(key, payload)`` tuple. The key identifies the
        invocation (e.g. morton cell int, storm_id).
    function_name : str
        Lambda function name.
    region : str
        AWS region.
    max_workers : int
        Max concurrent invocations.
    max_retries : int
        Retry count for throttling/timeouts.
    on_result : callable, optional
        ``(result_dict) -> None`` callback after each completion.
    log_interval : int
        Print progress every N completions.

    Returns
    -------
    results : dict
        ``{key: result_dict}`` for each invocation.
    errors : list[str]
        Error messages for failed invocations.
    """
    import boto3
    from botocore.config import Config

    boto_config = Config(
        read_timeout=900,
        connect_timeout=10,
        retries={"max_attempts": 0},
        max_pool_connections=max_workers,
    )
    lambda_client = boto3.client("lambda", region_name=region, config=boto_config)

    results = {}
    errors = []
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(
                _invoke_one, lambda_client, function_name, key, event, max_retries
            ): key
            for key, event in events
        }

        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            key = result["key"]
            results[key] = result

            if result["error"]:
                errors.append(f"{key}: {result['error']}")

            if on_result:
                on_result(result)

            if i % log_interval == 0 or i == len(events):
                elapsed = time.time() - start_time
                rate = i / elapsed if elapsed > 0 else 0
                eta = (len(events) - i) / rate if rate > 0 else 0
                print(
                    f"      [{i:4d}/{len(events)}] "
                    f"{rate:.1f}/s, ETA {eta / 60:.1f}m"
                )

    return results, errors


def estimate_cost(
    billed_durations_ms,
    memory_mb=2048,
    architecture="x86_64",
):
    """Estimate Lambda cost from billed durations.

    Parameters
    ----------
    billed_durations_ms : list[int]
        Billed duration per invocation in milliseconds.
    memory_mb : int
        Lambda memory configuration.
    architecture : str
        ``"x86_64"`` or ``"arm64"``.

    Returns
    -------
    dict
        ``{gb_seconds, compute_cost, request_cost, total_cost}``
    """
    price = LAMBDA_PRICE_ARM if architecture == "arm64" else LAMBDA_PRICE_X86
    total_seconds = sum(billed_durations_ms) / 1000.0
    gb_seconds = total_seconds * (memory_mb / 1024.0)
    compute_cost = gb_seconds * price
    request_cost = len(billed_durations_ms) * 0.0000002
    return {
        "gb_seconds": gb_seconds,
        "compute_cost": compute_cost,
        "request_cost": request_cost,
        "total_cost": compute_cost + request_cost,
    }
