{ inputs, ... }:
{
  imports = [ (inputs.import-tree ./nix/profiles) ];

  name = "prefect-rj-iplanrio";
}
