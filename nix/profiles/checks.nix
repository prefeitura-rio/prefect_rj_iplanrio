{ pkgs, ... }:
{
  profiles.checks = {
    extends = [ "base" ];
    module = {
      packages = with pkgs; [
        actionlint
      ];

      tasks = {
        "env:sync" = {
          exec = "nu scripts/checks.nu sync";
          execIfModified = [
            "pyproject.toml"
            "uv.lock"
            "packages/toolkit/pyproject.toml"
          ];
        };
        "lint:python" = {
          exec = "nu scripts/checks.nu lint python";
          after = [ "env:sync" ];
          execIfModified = [
            "src/**/*.py"
            "tests/**/*.py"
            "packages/toolkit/**/*.py"
            "pyproject.toml"
            "packages/toolkit/pyproject.toml"
            "uv.lock"
          ];
        };
        "lint:sql" = {
          exec = "nu scripts/checks.nu lint sql";
          after = [ "env:sync" ];
          execIfModified = [
            "packages/toolkit/toolkit/executor/templates/**"
            "packages/toolkit/.sqlfluff"
          ];
        };
        "lint:deps" = {
          exec = "nu scripts/checks.nu lint deps";
          execIfModified = [
            "pyproject.toml"
            "pipelines/*/pyproject.toml"
          ];
        };
        "lint:prefect" = {
          exec = "nu scripts/checks.nu lint prefect";
          execIfModified = [ "pipelines/*/prefect.yaml" ];
        };
        "lint:imports" = {
          exec = "nu scripts/checks.nu lint imports";
          after = [ "env:sync" ];
          execIfModified = [
            "pipelines/**/*.py"
            "pyproject.toml"
          ];
        };
        "lint:env" = {
          exec = "nu scripts/checks.nu lint env";
          execIfModified = [
            "pipelines/**/*.py"
            "src/**/*.py"
            "packages/toolkit/toolkit/settings.py"
          ];
        };
        "lint:ci" = {
          exec = "actionlint .github/workflows/*.y*ml";
          execIfModified = [
            ".github/workflows"
            ".github/actions"
          ];
        };
        "test:root" = {
          exec = "nu scripts/checks.nu test root";
          after = [ "env:sync" ];
          execIfModified = [
            "src/**/*.py"
            "tests/**/*.py"
            "packages/toolkit/**"
            "pyproject.toml"
            "uv.lock"
          ];
        };
        "test:toolkit" = {
          exec = "nu scripts/checks.nu test toolkit";
          after = [ "env:sync" ];
          execIfModified = [
            "packages/toolkit/**"
            "pyproject.toml"
            "uv.lock"
          ];
        };
        "lint:all".after = [
          "lint:python"
          "lint:sql"
          "lint:deps"
          "lint:prefect"
          "lint:imports"
          "lint:env"
          "lint:ci"
        ];
        "test:all".after = [
          "test:root"
          "test:toolkit"
        ];
      };
    };
  };
}
