"""Validate that all pipeline deployments have valid labels."""

import sys
from pathlib import Path

import yaml


def validate_prefect_yaml(file_path: Path) -> list[str]:
    """Validate code_owner and severity in deployments that define them.
    
    Deployments without code_owner/severity are skipped (backward compatibility).
    If either label is present, both must be valid.
    """
    errors = []

    with open(file_path) as f:
        config = yaml.safe_load(f)

    if not config.get("deployments"):
        return errors

    valid_severities = {"low", "medium", "high", "critical"}

    for deployment in config["deployments"]:
        params = deployment.get("parameters", {})
        name = deployment.get("name", "unknown")

        has_code_owner = "code_owner" in params
        has_severity = "severity" in params

        if not has_code_owner and not has_severity:
            continue

        if has_code_owner:
            code_owner = params["code_owner"].strip()
            if not code_owner:
                errors.append(
                    f"Deployment '{name}': 'code_owner' must be a non-empty string"
                )

        if has_severity:
            severity = params["severity"].lower()
            if severity not in valid_severities:
                errors.append(
                    f"Deployment '{name}': 'severity' must be one of "
                    f"{valid_severities}, got '{severity}'"
                )

        if has_code_owner and not has_severity:
            errors.append(
                f"Deployment '{name}': has 'code_owner' but missing 'severity'"
            )

        if has_severity and not has_code_owner:
            errors.append(
                f"Deployment '{name}': has 'severity' but missing 'code_owner'"
            )

    return errors


if __name__ == "__main__":
    errors = []

    pipeline_dir = Path("pipelines")
    for yaml_file in sorted(pipeline_dir.glob("*/prefect.yaml")):
        file_errors = validate_prefect_yaml(yaml_file)
        for error in file_errors:
            errors.append(f"{yaml_file}: {error}")

    if errors:
        print("❌ Pipeline label validation failed:\n")
        for error in errors:
            print(f"  {error}")
        sys.exit(1)

    print("✅ All pipelines have valid labels")
    sys.exit(0)
