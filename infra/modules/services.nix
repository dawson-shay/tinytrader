# tinytrader.services.<name>: run a tinytrader service as a hardened systemd unit,
# and describe the host's services for consoles and terminal views.
#
#   tinytrader.services.ticker = {
#     enable = true;
#     package = tinytrader.packages.x86_64-linux.ticker;
#     observe.port = 9105;
#   };
#
# Each service gets its own transient user, a private state directory
# (/var/lib/tinytrader/<name>), write access to nothing else unless granted
# through `writablePaths`, restart on failure (giving up after repeated fast
# failures), and resource limits with sensible defaults (`resources`). The
# journal is capped so logs can't fill the disk.
#
# A service with `observe.port` listens there (via TINYTRADER_LISTEN) and is
# listed in /etc/tinytrader/topology.json, which consoles and terminal views on
# this host read, so nothing is declared by hand. Health and metrics stay on
# loopback unless `observe.bind` says otherwise. To show services from other
# hosts, add their `tinytrader.topology.exported` lists to `extraServices`.
{ config, lib, options, ... }:
let
  inherit (lib) mkOption types;
  enabled = lib.filterAttrs (_: s: s.enable) config.tinytrader.services;
  observed = lib.filterAttrs (_: s: s.observe.port != null) enabled;
  host = config.networking.hostName;
  # "512M", "1G" or a byte count, as bytes.
  memoryBytes =
    size:
    let
      unit = lib.toUpper (lib.substring (lib.stringLength size - 1) 1 size);
      number = lib.toInt (lib.removeSuffix unit (lib.toUpper size));
      factor = {
        K = 1024;
        M = 1024 * 1024;
        G = 1024 * 1024 * 1024;
        T = 1024 * 1024 * 1024 * 1024;
      };
    in
    if factor ? ${unit} then number * factor.${unit} else lib.toInt size;
  cfg = config.tinytrader.topology;
  # Where a service's endpoints are reached: from another host via the
  # advertised name, locally via loopback when bound to all interfaces, else
  # at the specific address it is bound to.
  local =
    s:
    if
      lib.elem s.observe.bind [
        "0.0.0.0"
        "::"
      ]
    then
      "127.0.0.1"
    else
      s.observe.bind;
  entries =
    advertise:
    lib.mapAttrsToList (name: s: {
      id = name;
      inherit host;
      observe = "http://${if advertise == null then local s else advertise}:${toString s.observe.port}";
    }) observed;
in
{
  imports = [ ./service-options.nix ];

  options.tinytrader.topology = {
    name = mkOption {
      type = types.str;
      default = host;
      description = "Deployment name shown by consoles.";
    };
    advertise = mkOption {
      type = types.nullOr types.str;
      default = null;
      example = "vps";
      description = ''
        Address other hosts use to reach this host's services, such as its
        tailnet name. Needed only when another host's console shows them,
        and the services must then bind to an address that host can reach.
      '';
    };
    extraServices = mkOption {
      type = types.listOf types.attrs;
      default = [ ];
      description = "Services on other hosts to include, usually their `exported` lists.";
    };
    overrides = mkOption {
      type = types.attrs;
      default = { };
      description = "Further topology overrides (focus, panels, hide, labels).";
    };
    nodeExporter = mkOption {
      type = types.bool;
      default = true;
      description = ''
        Run Prometheus node_exporter on loopback (port 9100) and name it as this
        host's `exporter`, so consoles and terminal views show the machine's
        CPU, memory, swap, load and disks.
      '';
    };
    file = mkOption {
      type = types.str;
      readOnly = true;
      default = "/etc/tinytrader/topology.json";
      description = "Where the generated topology is written.";
    };
    exported = mkOption {
      type = types.listOf types.attrs;
      readOnly = true;
      default = if cfg.advertise == null then [ ] else entries cfg.advertise;
      description = "This host's observed services as other hosts reach them.";
    };
  };

  config = lib.mkIf (enabled != { }) {
    systemd.services = lib.mapAttrs' (
      name: svc:
      lib.nameValuePair "tinytrader-${name}" {
        description = "tinytrader ${name}";
        wantedBy = [ "multi-user.target" ];
        wants = [ "network-online.target" ];
        after = [ "network-online.target" ];
        # Stop restarting after five failures in a minute rather than loop forever.
        startLimitIntervalSec = 60;
        startLimitBurst = 5;
        environment =
          lib.optionalAttrs (svc.observe.port != null) {
            TINYTRADER_LISTEN = "${svc.observe.bind}:${toString svc.observe.port}";
          }
          // svc.environment
          # Each secret as a credential: <NAME>_FILE names it (%d is the
          # unit's credentials directory, readable by this service alone).
          // lib.mapAttrs' (var: _: lib.nameValuePair "${var}_FILE" "%d/${var}") svc.secrets;
        serviceConfig = {
          LoadCredential = lib.mapAttrsToList (var: path: "${var}:${path}") svc.secrets;
          ExecStart = lib.escapeShellArgs ([ (lib.getExe svc.package) ] ++ svc.args);
          Restart = "on-failure";
          RestartSec = 2;
          # Resources
          MemoryMax = svc.resources.memory;
          MemoryHigh = "${toString (memoryBytes svc.resources.memory * 85 / 100)}";
          CPUQuota = lib.mkIf (svc.resources.cpu != null) svc.resources.cpu;
          TasksMax = svc.resources.tasks;
          LimitNOFILE = svc.resources.openFiles;
          DynamicUser = true;
          StateDirectory = "tinytrader/${name}";
          WorkingDirectory = "/var/lib/tinytrader/${name}";
          ReadWritePaths = svc.writablePaths;
          # Sandboxing
          ProtectSystem = "strict";
          ProtectHome = true;
          PrivateTmp = true;
          PrivateDevices = true;
          NoNewPrivileges = true;
          CapabilityBoundingSet = "";
          ProtectKernelTunables = true;
          ProtectKernelModules = true;
          ProtectKernelLogs = true;
          ProtectControlGroups = true;
          ProtectClock = true;
          ProtectHostname = true;
          RestrictSUIDSGID = true;
          RestrictNamespaces = true;
          RestrictRealtime = true;
          LockPersonality = true;
          MemoryDenyWriteExecute = true;
          SystemCallArchitectures = "native";
          RestrictAddressFamilies = [
            "AF_INET"
            "AF_INET6"
            "AF_UNIX"
          ];
        };
      }
    ) enabled;

    services.prometheus.exporters.node = lib.mkIf cfg.nodeExporter {
      enable = lib.mkDefault true;
      listenAddress = lib.mkDefault "127.0.0.1";
      port = lib.mkDefault 9100;
    };

    # Logs can't fill the disk: cap the journal unless the host says otherwise.
    # Newer NixOS sets journald.conf through `settings` (`extraConfig` is
    # gone); older through `extraConfig`. Either way, the same two limits.
    services.journald =
      if options.services.journald ? settings then
        {
          settings.Journal = {
            SystemMaxUse = lib.mkDefault "1G";
            MaxRetentionSec = lib.mkDefault "1month";
          };
        }
      else
        {
          extraConfig = lib.mkDefault ''
            SystemMaxUse=1G
            MaxRetentionSec=1month
          '';
        };

    environment.etc."tinytrader/topology.json".text = builtins.toJSON (
      {
        inherit (cfg) name;
        hosts = [
          (
            {
              id = host;
              label = host;
              sub = "NixOS";
            }
            // lib.optionalAttrs cfg.nodeExporter {
              exporter = "http://127.0.0.1:${toString config.services.prometheus.exporters.node.port}/metrics";
            }
          )
        ];
      }
      // cfg.overrides
      // {
        services = entries null ++ cfg.extraServices ++ (cfg.overrides.services or [ ]);
      }
    );
  };
}
