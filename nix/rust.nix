# Rust workspace builds. Crane delegates to Cargo: Cargo.toml and Cargo.lock
# stay the source of truth, and one dependency build is shared by every crate.
{ inputs, ... }:
{
  perSystem =
    {
      pkgs,
      lib,
      toolchain,
      ...
    }:
    let
      craneLib = (inputs.crane.mkLib pkgs).overrideToolchain (_: toolchain);

      # Only what the Rust build reads, so docs and Nix edits don't trigger rebuilds.
      src = lib.fileset.toSource {
        root = ../.;
        fileset = lib.fileset.unions [
          ../Cargo.toml
          ../Cargo.lock
          ../libs/observe
          ../libs/fleet
          ../libs/schedule
          ../libs/dbn
          ../libs/producer
          ../libs/tinytrader
          ../libs/alpaca
          ../libs/openapi
          ../docs/api/openapi # the committed documents the tests compare with
          ../apps/tui
          ../apps/tt
          ../services/ticker
          ../services/databento-jobs
          ../services/store
          ../services/alpaca-news
          ../services/alpaca-history
          ../services/econ-calendar
          ../services/alpaca-account
          ../services/alpaca-data
          ../services/relay
          ../services/order-gateway
          ../services/session
          ../services/queue
          (lib.fileset.fileFilter (f: f.hasExt "rs" || f.name == "Cargo.toml") ../services/console)
          ../services/console/web
        ];
      };

      commonArgs = {
        inherit src;
        strictDeps = true;
        pname = "tinytrader-workspace";
        version = "0.1.0";
      };
      cargoArtifacts = craneLib.buildDepsOnly commonArgs;

      service =
        name: description:
        craneLib.buildPackage (
          commonArgs
          // {
            inherit cargoArtifacts;
            pname = name;
            cargoExtraArgs = "--locked -p ${name}";
            doCheck = false; # tests run once for the whole workspace in checks.rust-tests
            meta = {
              inherit description;
              mainProgram = name;
            };
          }
        );

      ticker = service "ticker" "Toy service streaming Coinbase public tickers to disk";
      console = service "console" "tinytrader console";
      databento-jobs = service "databento-jobs" "Databento batch jobs: price, submit, and download into the store";
      tinytrader-tui = service "tinytrader-tui" "Terminal view of a tinytrader deployment";
      tt = service "tt" "tinytrader from a terminal: purchases through the console (decision 0021)";
      store = service "store" "The data store: ingests provider downloads into the standard layout and catalogs the volumes";
      alpaca-news = service "alpaca-news" "Example service serving market news from Alpaca (Benzinga)";
      econ-calendar = service "econ-calendar" "Scheduled US economic releases and FOMC decisions";
      alpaca-data = service "alpaca-data" "Market data for any symbol on request: stock snapshots, bars and option chains from Alpaca";
      alpaca-account = service "alpaca-account" "Example service serving an Alpaca account's balances and positions";
      relay = service "relay" "Market data relay: one provider connection served to local clients (tinytrader.relay/1)";
      queue = service "queue" "The job queue: deployment-defined job templates run when their conditions hold (decision 0012)";
      order-gateway = service "order-gateway" "Order gateway: one broker account behind a loopback protocol (tinytrader.gateway/1)";
      alpaca-history = service "alpaca-history" "Alpaca's historical trades and quotes, a day at a time, into the store (decision 0014)";

    in
    {
      packages = {
        inherit
          ticker
          console
          databento-jobs
          tinytrader-tui
          tt
          store
          alpaca-news
          econ-calendar
          alpaca-account
          alpaca-data
          relay
          order-gateway
          queue
          alpaca-history
          ;
      };

      checks = {
        rust-tests = craneLib.cargoTest (commonArgs // { inherit cargoArtifacts; });
        rust-clippy = craneLib.cargoClippy (
          commonArgs
          // {
            inherit cargoArtifacts;
            cargoClippyExtraArgs = "--all-targets -- --deny warnings";
          }
        );
      };
    };
}
