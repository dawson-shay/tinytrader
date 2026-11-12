# checks.<system>.service-user: the NixOS module's `user`. The example
# host's store names a static system user (its files on the data volumes
# outlive the unit), so its unit runs as that user, not a DynamicUser, and
# the user and its group exist; the other services stay transient.
{ self, ... }:
{
  perSystem =
    { pkgs, lib, ... }:
    let
      config = self.nixosConfigurations.example.config;
      unit = name: config.systemd.services."tinytrader-${name}".serviceConfig;
      store = unit "store";
      ticker = unit "ticker";
      user = config.users.users.tinytrader-store or { };
    in
    {
      checks.service-user =
        assert lib.assertMsg (
          store.DynamicUser == false && store.User == "tinytrader-store" && store.Group == "tinytrader-store"
        ) "service-user: the store must run as its static user, not a DynamicUser";
        assert lib.assertMsg (
          (user.isSystemUser or false) && (user.group or null) == "tinytrader-store"
        ) "service-user: users.users.tinytrader-store must be a system user in its own group";
        assert lib.assertMsg (config.users.groups ? tinytrader-store) "service-user: its group must exist";
        assert lib.assertMsg (
          ticker.DynamicUser == true && !(ticker ? User)
        ) "service-user: a service with no `user` stays a DynamicUser";
        pkgs.runCommand "tinytrader-service-user-check" { } "touch $out";
    };
}
