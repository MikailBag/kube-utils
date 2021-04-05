mod cli;
mod collect;
mod init;
mod watcher;

pub use self::collect::Collect;
use self::collect::ControllerDescriptionCollector;
use async_trait::async_trait;
use kube::api::Resource;

/// Type, wrapping several controllers and providing all
/// required infrastructure for them to work.
pub struct ControllerManager {
    controllers: Vec<ControllerDescription>,
}

impl ControllerManager {
    pub fn new() -> Self {
        ControllerManager {
            controllers: Vec::new(),
        }
    }

    /// Adds a controller
    /// # Panics
    /// Panics if `<C as Controller>::describe` is incorrect
    pub fn add<C: Controller>(&mut self) {
        let collector = ControllerDescriptionCollector::new();
        C::describe(&collector);
        let description = collector.finalize();
        self.controllers.push(description);
    }

    /// Controller manger entry point.
    ///
    /// This function parses command line arguments,
    /// launches web server and serves to completion
    #[tracing::instrument(skip(self))]
    pub async fn main(self) -> anyhow::Result<()> {
        let args: cli::Args = clap::Clap::parse();
        tracing::info!(args = ?args, "parsed command-line arguments");
        match args {
            cli::Args::List => {
                self.print_list();
                Ok(())
            }
            cli::Args::Run(args) => self.run(args),
        }
    }

    fn print_list(self) {
        println!("Supported controllers:");
        for c in &self.controllers {
            println!("\t{}", c.name);
        }
    }

    #[tracing::instrument(skip(self, args))]
    fn run(self, args: cli::Run) -> anyhow::Result<()> {
        let enabled_controllers = {
            let controllers = self
                .controllers
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>();
            init::process_controller_filters(&controllers, &args.controllers)?
        };
        tracing::info!(enabled_controllers = ?enabled_controllers, "Selected controllers to run");
        anyhow::bail!("TODO")
    }
}

/// Description of a controller
struct ControllerDescription {
    name: String,
}

// TODO: replace by kube's GVK
struct ResourceInfo {
    group: String,
    version: String,
    kind: String,
}

impl ResourceInfo {
    fn new<K: Resource<DynamicType = ()>>() -> Self {
        ResourceInfo {
            group: K::group(&()).into_owned(),
            version: K::version(&()).into_owned(),
            kind: K::version(&()).into_owned(),
        }
    }
}

/// Trait, implemented by a controller
#[async_trait]
pub trait Controller {
    /// Reports some information to given collector
    fn describe<C: Collect>(collector: &C);
}
