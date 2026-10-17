//! Trading-rule profiles (decision 0016): the rules file, and the checks that apply it to
//! an opening order.
//!
//! The file lives in the deployment, read once at startup. Tinytrader's code names no
//! group, client, account or number; those come from the file. A file that does not parse,
//! or whose references do not resolve, stops the gateway before it serves anything.
//!
//! The checks are pure functions of numbers the session has already gathered. Each one
//! fails closed: an input it cannot know refuses the opening with that reason. A close is
//! never checked.
use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;

/// The rules file's schema, checked at startup.
pub const SCHEMA: &str = "tinytrader.gateway-profiles/1";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct File {
    schema: String,
    #[serde(default)]
    profiles: BTreeMap<String, ProfileFile>,
    #[serde(default)]
    bindings: Vec<BindingFile>,
    #[serde(default)]
    groups: Vec<GroupFile>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct ProfileFile {
    position_max_loss_pct: Option<u32>,
    stop_loss_pct: Option<u32>,
    daily_loss_pct: Option<u32>,
    #[serde(default)]
    limit_only: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BindingFile {
    account: String,
    client: String,
    profile: String,
    #[serde(default = "enabled_by_default")]
    enabled: bool,
}

fn enabled_by_default() -> bool {
    true
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GroupFile {
    name: String,
    underlyings: Vec<String>,
}

/// One profile's rules. A rule left out of the file is not applied.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Profile {
    /// Largest loss at risk, per correlation group, as a percentage of start-of-day equity.
    pub position_max_loss_pct: Option<u32>,
    /// The protective exit, as a percentage of start-of-day equity, per position.
    pub stop_loss_pct: Option<u32>,
    /// The day's losses (realised plus open) as a percentage of start-of-day equity.
    pub daily_loss_pct: Option<u32>,
    /// Openings must be limit orders.
    pub limit_only: bool,
}

/// What the file says about one `(account, client)` pair.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Applied {
    /// No rules: the client has only the account-wide limits. A binding switched off in the
    /// file is this too.
    None,
    /// A binding in force.
    Enabled(Profile),
}

/// The parsed, validated rules file.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Rules {
    profiles: BTreeMap<String, Profile>,
    /// `(account, client)` to the profile name and whether it is enabled.
    bindings: BTreeMap<(String, String), (String, bool)>,
    /// Underlying to its group's name.
    groups: BTreeMap<String, String>,
}

impl Rules {
    /// Parses and validates the file. Any error is a reason to refuse to start.
    pub fn parse(text: &str) -> Result<Self, String> {
        let file: File = toml::from_str(text).map_err(|e| format!("rules file: {e}"))?;
        if file.schema != SCHEMA {
            return Err(format!(
                "rules file: schema is {:?}, expected {SCHEMA:?}",
                file.schema
            ));
        }
        let mut profiles = BTreeMap::new();
        for (name, p) in &file.profiles {
            let pct = |label: &str, v: Option<u32>| match v {
                Some(n) if !(1..=100).contains(&n) => {
                    Err(format!("profile {name}: {label} must be 1 to 100, not {n}"))
                }
                _ => Ok(v),
            };
            profiles.insert(
                name.clone(),
                Profile {
                    position_max_loss_pct: pct("position_max_loss_pct", p.position_max_loss_pct)?,
                    stop_loss_pct: pct("stop_loss_pct", p.stop_loss_pct)?,
                    daily_loss_pct: pct("daily_loss_pct", p.daily_loss_pct)?,
                    limit_only: p.limit_only,
                },
            );
        }
        let mut bindings = BTreeMap::new();
        for b in &file.bindings {
            if !profiles.contains_key(&b.profile) {
                return Err(format!(
                    "binding {}/{}: no profile {:?}",
                    b.account, b.client, b.profile
                ));
            }
            let key = (b.account.clone(), b.client.clone());
            if bindings
                .insert(key, (b.profile.clone(), b.enabled))
                .is_some()
            {
                return Err(format!(
                    "binding {}/{} is listed twice",
                    b.account, b.client
                ));
            }
        }
        let mut groups = BTreeMap::new();
        let mut names = BTreeSet::new();
        for g in &file.groups {
            if g.name.is_empty() || !names.insert(g.name.clone()) {
                return Err(format!("group {:?}: name is empty or repeated", g.name));
            }
            for u in &g.underlyings {
                if groups.insert(u.clone(), g.name.clone()).is_some() {
                    return Err(format!("underlying {u} is in more than one group"));
                }
            }
        }
        Ok(Self {
            profiles,
            bindings,
            groups,
        })
    }

    /// The rules that apply to `client` on `account`. A binding that is switched off means
    /// the rules are off for that client, as if it were not in the file.
    pub fn binding(&self, account: &str, client: &str) -> Applied {
        match self.bindings.get(&(account.to_owned(), client.to_owned())) {
            Some((profile, true)) => Applied::Enabled(self.profiles[profile]),
            None | Some((_, false)) => Applied::None,
        }
    }

    /// The group an underlying belongs to. An underlying the file does not list is its own
    /// group.
    pub fn group_of(&self, underlying: &str) -> String {
        self.groups
            .get(underlying)
            .cloned()
            .unwrap_or_else(|| underlying.to_owned())
    }
}

/// The inputs to an opening order's checks. Each `Err` is the reason that input could not be
/// known; the check that needs it refuses with that reason.
#[derive(Clone, Copy, Debug)]
pub struct Opening<'a> {
    pub group: &'a str,
    /// This order's maximum loss, in cents.
    pub max_loss: Result<u64, &'static str>,
    /// The maximum loss already open in the same group, in cents.
    pub group_open: Result<u64, &'static str>,
    /// Start-of-day equity, in cents. Read once per session and journaled.
    pub start_equity: Option<u64>,
    /// The day's losses, realised plus open at current marks, in cents; negative for a gain.
    pub day_loss: Result<i64, &'static str>,
    /// Whether the order is a market order.
    pub market: bool,
}

/// A refused opening: the rule's name and a sentence the console can show as it is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refusal {
    pub rule: &'static str,
    pub message: String,
}

/// Applies `profile` to one opening. `Ok` passes it on to the account-wide limits.
pub fn check_opening(profile: &Profile, o: &Opening<'_>) -> Result<(), Refusal> {
    if profile.limit_only && o.market {
        return Err(refuse(
            "limit_only",
            "market orders are not allowed to open; send a limit".into(),
        ));
    }
    let needs_equity = profile.position_max_loss_pct.is_some() || profile.daily_loss_pct.is_some();
    if !needs_equity {
        return Ok(());
    }
    let Some(equity) = o.start_equity else {
        return Err(refuse(
            "equity_unknown",
            "start-of-day equity is unknown, so openings are refused".into(),
        ));
    };
    if let Some(pct) = profile.daily_loss_pct {
        let day = o.day_loss.map_err(|why| {
            refuse(
                "marks_unknown",
                format!("the day's P&L cannot be computed ({why}), so openings are refused"),
            )
        })?;
        // loss * 100 >= equity * pct, in integers; a negative loss is a gain and never refuses.
        if day > 0 && i128::from(day) * 100 >= i128::from(equity) * i128::from(pct) {
            return Err(refuse(
                "daily_loss",
                format!(
                    "the day's losses are {} ({} of start-of-day equity); your daily limit is {}%",
                    money(day as u64),
                    percent(day as u64, equity),
                    pct
                ),
            ));
        }
    }
    if let Some(pct) = profile.position_max_loss_pct {
        let new = o.max_loss.map_err(|why| {
            refuse(
                "max_loss",
                format!("this order's maximum loss cannot be computed ({why}), so it is refused"),
            )
        })?;
        let open = o.group_open.map_err(|why| refuse("max_loss", format!("the open maximum loss in the {} group cannot be computed ({why}), so openings there are refused", o.group)))?;
        let total = u128::from(new) + u128::from(open);
        if total * 100 > u128::from(equity) * u128::from(pct) {
            return Err(refuse(
                "position_size",
                format!(
                    "would put {} of equity at risk in the {} group; your limit is {}%",
                    percent_u128(total, equity),
                    o.group,
                    pct
                ),
            ));
        }
    }
    Ok(())
}

/// The rules for one order from one client on one account. A close is never refused here,
/// whatever the binding and whatever inputs are unknown: closes must always be possible.
pub fn check_order(applied: Applied, opening: bool, o: &Opening<'_>) -> Result<(), Refusal> {
    if !opening {
        return Ok(());
    }
    match applied {
        Applied::None => Ok(()),
        Applied::Enabled(profile) => check_opening(&profile, o),
    }
}

fn refuse(rule: &'static str, message: String) -> Refusal {
    Refusal { rule, message }
}

/// Dollars and cents, for a message.
fn money(cents: u64) -> String {
    format!("${}.{:02}", cents / 100, cents % 100)
}

/// A share of equity to one decimal place, for a message.
fn percent(part: u64, whole: u64) -> String {
    percent_u128(u128::from(part), whole)
}

fn percent_u128(part: u128, whole: u64) -> String {
    if whole == 0 {
        return "n/a".into();
    }
    let tenths = part * 1000 / u128::from(whole);
    format!("{}.{}%", tenths / 10, tenths % 10)
}

#[cfg(test)]
mod tests {
    use super::*;

    const FILE: &str = r#"
schema = "tinytrader.gateway-profiles/1"

[profiles.user]
position_max_loss_pct = 10
stop_loss_pct = 2
daily_loss_pct = 10
limit_only = true

[profiles.automated]

[[bindings]]
account = "paper-1"
client = "console"
profile = "user"

[[bindings]]
account = "paper-1"
client = "reactor"
profile = "automated"

[[bindings]]
account = "live-1"
client = "console"
profile = "user"
enabled = false

[[groups]]
name = "SPY"
underlyings = ["SPY", "XSP", "SPX"]
"#;

    /// Dollars, as cents.
    fn usd(dollars: u64) -> u64 {
        dollars * 100
    }

    fn rules() -> Rules {
        Rules::parse(FILE).unwrap()
    }

    fn err(text: &str) -> String {
        Rules::parse(text).unwrap_err()
    }

    /// A $100,000 account, with a $5,000 open loss in the group and nothing else.
    fn open(max_loss: u64) -> Opening<'static> {
        Opening {
            group: "SPY",
            max_loss: Ok(max_loss),
            group_open: Ok(usd(5_000)),
            start_equity: Some(usd(100_000)),
            day_loss: Ok(0),
            market: false,
        }
    }

    fn user() -> Profile {
        rules().binding("paper-1", "console").expect_enabled()
    }

    trait Expect {
        fn expect_enabled(self) -> Profile;
    }

    impl Expect for Applied {
        fn expect_enabled(self) -> Profile {
            match self {
                Applied::Enabled(p) => p,
                other => panic!("expected an enabled binding, got {other:?}"),
            }
        }
    }

    #[test]
    fn the_file_gives_bindings_profiles_and_groups() {
        let r = rules();
        assert_eq!(r.binding("paper-1", "console"), Applied::Enabled(user()));
        assert_eq!(
            r.binding("paper-1", "reactor"),
            Applied::Enabled(Profile::default())
        );
        assert_eq!(r.binding("paper-1", "other"), Applied::None);
        assert_eq!(r.binding("live-1", "console"), Applied::None);
        assert_eq!(r.group_of("XSP"), "SPY");
        assert_eq!(r.group_of("AAPL"), "AAPL");
    }

    #[test]
    fn a_file_that_names_the_wrong_schema_or_unknown_keys_is_refused() {
        assert!(err("schema = \"other/1\"").contains("expected"));
        let typo = FILE.replace("daily_loss_pct", "daily_loss_percent");
        assert!(err(&typo).contains("unknown field"));
        let extra = FILE.replace("[profiles.automated]", "[profiles.automated]\nstop_pct = 3");
        assert!(err(&extra).contains("unknown field"));
    }

    #[test]
    fn a_percentage_outside_1_to_100_is_refused() {
        assert!(
            err(&FILE.replace("position_max_loss_pct = 10", "position_max_loss_pct = 0"))
                .contains("1 to 100")
        );
        assert!(
            err(&FILE.replace("daily_loss_pct = 10", "daily_loss_pct = 101")).contains("1 to 100")
        );
    }

    #[test]
    fn a_binding_to_a_missing_profile_or_a_repeated_pair_is_refused() {
        assert!(
            err(&FILE.replace("profile = \"automated\"", "profile = \"nope\""))
                .contains("no profile")
        );
        let twice = format!(
            "{FILE}\n[[bindings]]\naccount = \"paper-1\"\nclient = \"console\"\nprofile = \"automated\"\n"
        );
        assert!(err(&twice).contains("listed twice"));
    }

    #[test]
    fn an_underlying_in_two_groups_is_refused() {
        let twice = format!("{FILE}\n[[groups]]\nname = \"IWM\"\nunderlyings = [\"SPY\"]\n");
        assert!(err(&twice).contains("more than one group"));
    }

    #[test]
    fn an_opening_under_both_limits_passes() {
        // $3,000 new on $5,000 open: $8,000 of $100,000 is 8%, under 10%.
        assert_eq!(check_opening(&user(), &open(usd(3_000))), Ok(()));
    }

    #[test]
    fn an_opening_over_the_group_limit_is_refused_with_its_numbers() {
        // $6,000 new on $5,000 open: $11,000 of $100,000 is 11%.
        let r = check_opening(&user(), &open(usd(6_000))).unwrap_err();
        assert_eq!(r.rule, "position_size");
        assert_eq!(
            r.message,
            "would put 11.0% of equity at risk in the SPY group; your limit is 10%"
        );
    }

    #[test]
    fn exactly_at_the_limit_passes_and_one_cent_over_does_not() {
        assert_eq!(check_opening(&user(), &open(usd(5_000))), Ok(()));
        assert!(check_opening(&user(), &open(usd(5_000) + 1)).is_err());
    }

    #[test]
    fn a_daily_loss_at_the_limit_refuses_even_with_room_in_the_group() {
        let mut o = open(1_00);
        o.day_loss = Ok(1_000_000);
        let r = check_opening(&user(), &o).unwrap_err();
        assert_eq!(r.rule, "daily_loss");
        assert!(
            r.message.contains("your daily limit is 10%"),
            "{}",
            r.message
        );
    }

    #[test]
    fn the_daily_loss_counts_open_losses_as_well_as_realised_ones() {
        // The day_loss input is realised plus open: $9,000 realised and $1,500 open at marks.
        let mut o = open(1_00);
        o.day_loss = Ok(900_000 + 150_000);
        assert_eq!(check_opening(&user(), &o).unwrap_err().rule, "daily_loss");
    }

    #[test]
    fn a_gain_never_refuses_on_the_daily_limit() {
        let mut o = open(1_00);
        o.day_loss = Ok(-5_000_000);
        assert_eq!(check_opening(&user(), &o), Ok(()));
    }

    #[test]
    fn an_unknown_input_refuses_with_its_reason_and_never_assumes() {
        let mut no_equity = open(1_00);
        no_equity.start_equity = None;
        assert_eq!(
            check_opening(&user(), &no_equity).unwrap_err().rule,
            "equity_unknown"
        );

        let mut no_marks = open(1_00);
        no_marks.day_loss = Err("no quote on leg SPY260101C00500000");
        assert_eq!(
            check_opening(&user(), &no_marks).unwrap_err().rule,
            "marks_unknown"
        );

        let mut no_loss = open(1_00);
        no_loss.max_loss = Err("max_loss_unbounded");
        let r = check_opening(&user(), &no_loss).unwrap_err();
        assert_eq!(
            (r.rule, r.message.contains("max_loss_unbounded")),
            ("max_loss", true)
        );

        let mut no_group = open(1_00);
        no_group.group_open = Err("max_loss_unknown");
        assert_eq!(
            check_opening(&user(), &no_group).unwrap_err().rule,
            "max_loss"
        );
    }

    #[test]
    fn a_market_order_is_refused_under_limit_only_and_passes_under_automated() {
        let mut o = open(1_00);
        o.market = true;
        assert_eq!(check_opening(&user(), &o).unwrap_err().rule, "limit_only");
        assert_eq!(check_opening(&Profile::default(), &o), Ok(()));
    }

    #[test]
    fn an_automated_profile_applies_no_position_or_loss_rule() {
        let mut o = open(usd(999_999));
        o.start_equity = None;
        o.day_loss = Err("no marks");
        assert_eq!(check_opening(&Profile::default(), &o), Ok(()));
    }

    #[test]
    fn a_disabled_binding_applies_no_rules() {
        // The live binding is in the file switched off: it is the same as no binding.
        assert_eq!(rules().binding("live-1", "console"), Applied::None);
        let mut o = open(1_00);
        o.market = true;
        o.start_equity = None;
        assert_eq!(
            check_order(rules().binding("live-1", "console"), true, &o),
            Ok(())
        );
    }

    #[test]
    fn closes_pass_under_every_rule_even_when_nothing_is_known() {
        // Unknown equity, unknown marks, unbounded loss, a market order, under a profile with
        // every rule on: a close still passes.
        let worst = Opening {
            group: "SPY",
            max_loss: Err("max_loss_unbounded"),
            group_open: Err("max_loss_unknown"),
            start_equity: None,
            day_loss: Err("no marks"),
            market: true,
        };
        assert_eq!(check_order(Applied::Enabled(user()), false, &worst), Ok(()));
        assert_eq!(check_order(Applied::None, false, &worst), Ok(()));
        // The same inputs refuse an opening under the profile.
        assert!(check_order(Applied::Enabled(user()), true, &worst).is_err());
    }

    #[test]
    fn a_client_with_no_binding_has_only_the_account_wide_limits() {
        let worst = open(usd(999_999));
        assert_eq!(check_order(Applied::None, true, &worst), Ok(()));
    }
}
