{
  imports = [
    ./nix/profiles/base.nix
    ./nix/profiles/checks.nix
    ./nix/profiles/default.nix
    ./nix/profiles/deploy.nix
  ];

  name = "prefect-rj-iplanrio";
}
