use anyhow::Context as _;
use clap::Clap;
use std::str::FromStr;

#[derive(Clap, Debug)]
pub(super) enum Args {
    /// Prints supported controllers
    List,
    /// Run controllers
    Run(Run),
    /// Print custom resources
    PrintCustomResources,
    /// Apply custom resources to cluster
    ApplyCustomResources,
}

#[derive(Clap, Debug)]
pub(super) struct Run {
    /// Specifies controllers to enable.
    /// This flag can be specified several times, and
    /// each occurence can consist of several semicolon-separated
    /// specs. Each spec can looks like `target=action` or `target`
    /// (which is shorthand for `target=enable`). Target can be
    /// either `*` (which selects all controllers) or a comma-separated
    /// list of names. Action can be either `enable` or `disable`.
    ///
    /// Specs are processed left-to-right. If a controller is covered
    /// by several specs, last wins. By default all controllers are considered
    /// to be enabled.
    ///
    /// If unknown controller is specified, an error will be thrown.
    #[clap(long, value_delimiter = ";")]
    pub(super) controllers: Vec<ControllerSpec>,
}

#[derive(Clone, Debug)]
pub(super) enum Target {
    All,
    List(Vec<String>),
}

impl FromStr for Target {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        if s == "*" {
            return Ok(Target::All);
        }
        let items = s.split(',').map(ToString::to_string).collect::<Vec<_>>();
        Ok(Target::List(items))
    }
}

#[derive(Clone, Debug)]
pub(super) enum Action {
    Enable,
    Disable,
}

impl FromStr for Action {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s {
            "enable" => Ok(Action::Enable),
            "disable" => Ok(Action::Disable),
            other => anyhow::bail!(
                "Unknown action {}, expected one of: 'enable', 'disable'",
                other
            ),
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct ControllerSpec {
    pub(super) target: Target,
    pub(super) action: Action,
}

impl FromStr for ControllerSpec {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        let mut iter = s.split('=');
        let target = iter
            .next()
            .unwrap()
            .parse()
            .context("failed to parse target")?;
        let action = match iter.next() {
            Some(a) => a.parse().context("failed to parse action")?,
            None => Action::Enable,
        };
        if iter.next().is_some() {
            anyhow::bail!("Spec must look like filter or filter=action");
        }
        Ok(ControllerSpec { target, action })
    }
}
