{
  lib,
  pkgs,
  config,
  ...
}:
{
  profiles.base.module = {
    env = {
      LD_LIBRARY_PATH = lib.makeLibraryPath [
        pkgs.stdenv.cc.cc.lib
        pkgs.zlib
      ];
      NU_LIB_DIRS = "${config.devenv.root}/scripts";
      UV_LINK_MODE = "copy";
      UV_PYTHON_DOWNLOADS = "never";
      UV_PROJECT_ENVIRONMENT = lib.mkForce "${config.devenv.root}/.venv";
    };

    packages = with pkgs; [
      duckdb
      infisical
      nushell
      (google-cloud-sdk.withExtraComponents (
        with google-cloud-sdk.components;
        [ gke-gcloud-auth-plugin ]
      ))
    ];

    languages.python = {
      enable = true;
      package = pkgs.python314;
      uv.enable = true;
    };
  };
}
