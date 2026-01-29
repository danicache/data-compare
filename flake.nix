{
  description = "A Go development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
        };
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            go
            gopls
            delve
            gemini-cli
          ];

          # Optional: Set environment variables for Go
          # shellHook = ''
          #   export GOPATH=$(pwd)/.go
          #   export GOCACHE=$(pwd)/.cache
          #   mkdir -p $GOPATH $GOCACHE
          # '';
        };

        formatter = pkgs.nixpkgs-fmt;
      }
    );
}
