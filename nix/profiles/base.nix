{
  lib,
  pkgs,
  config,
  ...
}:
{
  profiles.base.module = {
    env = {
      NU_LIB_DIRS = "${config.devenv.root}/scripts";
      UV_LINK_MODE = "copy";
      UV_PYTHON_DOWNLOADS = "never";
      UV_PROJECT_ENVIRONMENT = lib.mkForce "${config.devenv.root}/.venv";
      LD_LIBRARY_PATH = lib.makeLibraryPath (
        with pkgs;
        [
          stdenv.cc.cc.lib
          zlib
        ]
      );
    };

    packages = with pkgs; [
      duckdb
      infisical
      nushell
      (google-cloud-sdk.withExtraComponents (
        with google-cloud-sdk.components; [ gke-gcloud-auth-plugin ]
      ))
    ];

    languages.python = with pkgs; {
      enable = true;
      package = python314;
      lsp.package = basedpyright;
      uv.enable = true;
    };
  };
}
