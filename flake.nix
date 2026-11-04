{
  description = "tinytrader: services, deployment modules and an operator console";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";
    flake-parts.url = "github:hercules-ci/flake-parts";
    crane.url = "github:ipetkov/crane";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    inputs@{
      self,
      flake-parts,
      nixpkgs,
      ...
    }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [
        "aarch64-darwin"
        "x86_64-linux"
        "aarch64-linux"
      ];

      imports = [
        ./nix/rust.nix
        ./nix/generic.nix
        ./nix/web.nix
        ./nix/images.nix
        ./nix/watch.nix
        ./nix/darwin-check.nix
      ];

      flake = {
        nixosModules.services = ./infra/modules/services.nix;
        # The same service definitions as launchd agents or daemons on macOS (nix-darwin).
        darwinModules.services = ./infra/modules/darwin-services.nix;
        # For a deployment's own launchd jobs: `launchd.waitForNix argv` starts a
        # job once the Nix store is mounted (infra/lib/launchd.nix).
        lib.launchd = import ./infra/lib/launchd.nix;

        # The example host's services as plain systemd unit files, for Linux
        # distributions other than NixOS that have Nix installed: copy the
        # units to /etc/systemd/system and topology.json to /etc/tinytrader.
        # Generated from the same tinytrader.services definitions, so they match
        # what NixOS runs.
        systemdUnits.x86_64-linux.example =
          let
            pkgs = nixpkgs.legacyPackages.x86_64-linux;
            config = self.nixosConfigurations.example.config;
            units = config.systemd.units;
            ours = nixpkgs.lib.filterAttrs (name: _: nixpkgs.lib.hasPrefix "tinytrader-" name) units;
          in
          pkgs.linkFarm "tinytrader-example-units" (
            nixpkgs.lib.mapAttrsToList (name: unit: {
              inherit name;
              path = pkgs.writeText name unit.text;
            }) ours
            ++ [
              {
                # Copy to /etc/tinytrader/topology.json alongside the units.
                name = "topology.json";
                path = pkgs.writeText "topology.json" config.environment.etc."tinytrader/topology.json".text;
              }
            ]
          );

        # A minimal host running the quickstart services under systemd. It shows
        # the module in use and is evaluated by `nix flake check`.
        nixosConfigurations.example = nixpkgs.lib.nixosSystem {
          system = "x86_64-linux";
          modules = [
            self.nixosModules.services
            (
              { config, ... }:
              let
                packages = self.packages.x86_64-linux;
              in
              {
                networking.hostName = "example";
                tinytrader.services.ticker = {
                  enable = true;
                  package = packages.ticker;
                  observe.port = 9105;
                  environment.TICKER_DATA_DIR = "/var/lib/tinytrader/ticker";
                };
                # The data store on Linux: drives mounted under /mnt that carry
                # an approval marker are used as they appear (decision 0007);
                # others are offered for approval in the console's Storage.
                tinytrader.services.store = {
                  enable = true;
                  package = packages.store;
                  observe.port = 9120;
                  environment.STORE_DISCOVER = "/mnt";
                  writablePaths = [ "/mnt" ];
                  # Writes need this bearer token (docs/store-api.md); reads stay open.
                  secrets.STORE_WRITE_TOKEN = "/run/secrets/store-write-token";
                };
                tinytrader.services.console = {
                  enable = true;
                  package = packages.console;
                  observe.port = 8080;
                  # Reads the topology this module generates from the services above.
                  environment.CONSOLE_TOPOLOGY = config.tinytrader.topology.file;
                };
                # A service with a secret (decision 0004 in a deployment): handed
                # over as a credential, named to it by DATABENTO_API_KEY_FILE.
                tinytrader.services.databento-jobs = {
                  enable = true;
                  package = packages.databento-jobs;
                  observe.port = 9121;
                  secrets.DATABENTO_API_KEY = "/run/secrets/databento-api-key";
                  # The store's write token, for its uploads and filing.
                  secrets.JOBS_STORE_TOKEN = "/run/secrets/store-write-token";
                };
                # The host's own monitor (nix/watch.nix): its services and
                # disks, pushed to an ntfy-style URL when something is wrong.
                tinytrader.services.watch = {
                  enable = true;
                  package = packages.watch;
                  environment = {
                    WATCH_CONSOLE = "http://127.0.0.1:8080";
                    WATCH_STORE = "http://127.0.0.1:9120";
                    WATCH_NATIVE = "ticker store console databento-jobs";
                    WATCH_DISKS = "/ /mnt";
                  };
                  secrets.WATCH_NOTIFY = "/run/secrets/watch-notify";
                  secrets.WATCH_HEARTBEAT = "/run/secrets/watch-heartbeat";
                };
                tinytrader.topology.overrides.focus = "ticker";

                boot.loader.grub.enable = false;
                fileSystems."/" = {
                  device = "none";
                  fsType = "tmpfs";
                };
                system.stateVersion = "26.05";
              }
            )
          ];
        };
      };

      perSystem =
        { system, ... }:
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [ inputs.rust-overlay.overlays.default ];
          };
          # The toolchain named in rust-toolchain.toml, as rustup would install it.
          toolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
        in
        {
          _module.args = { inherit pkgs toolchain; };
          formatter = pkgs.nixfmt;

          devShells.default = pkgs.mkShell {
            packages = with pkgs; [
              nixfmt
              git
              jq
              sops
              age
              toolchain
              rust-analyzer
              typescript
            ];
          };
        };
    };
}
