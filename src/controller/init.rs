use std::collections::HashSet;

use crate::controller::cli::{Action, ControllerSpec, Target};

pub(super) fn process_controller_filters(
    controllers: &[String],
    specs: &[ControllerSpec],
) -> anyhow::Result<Vec<String>> {
    let mut unknown_controllers = HashSet::new();

    for spec in specs {
        if let Target::List(list) = &spec.target {
            for item in list {
                unknown_controllers.insert(item.clone());
            }
        }
    }
    for known in controllers {
        unknown_controllers.remove(known);
    }
    if !unknown_controllers.is_empty() {
        anyhow::bail!(
            "Filters mention unknown controllers: {:?}",
            unknown_controllers
        )
    }

    let specs = {
        let mut s = specs.to_vec();
        s.insert(
            0,
            ControllerSpec {
                action: Action::Enable,
                target: Target::All,
            },
        );
        s
    };
    let mut current = HashSet::new();
    for spec in specs {
        match (&spec.target, &spec.action) {
            (Target::All, Action::Enable) => {
                current = controllers.iter().cloned().collect::<HashSet<_>>()
            }
            (Target::All, Action::Disable) => {
                current.clear();
            }
            (Target::List(list), Action::Enable) => {
                for item in list {
                    current.insert(item.clone());
                }
            }
            (Target::List(list), Action::Disable) => {
                for item in list {
                    current.remove(item);
                }
            }
        }
    }

    Ok(current.into_iter().collect())
}
