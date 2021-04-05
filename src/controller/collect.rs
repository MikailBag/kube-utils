use crate::controller::ControllerDescription;
use std::cell::RefCell;

struct CollectorState {
    name: Option<String>,
}

impl CollectorState {
    fn new() -> Self {
        CollectorState { name: None }
    }

    fn finalize(self) -> ControllerDescription {
        ControllerDescription {
            name: self.name.expect("Collect::name not called"),
        }
    }
}

pub(super) struct ControllerDescriptionCollector(RefCell<CollectorState>);

impl ControllerDescriptionCollector {
    pub(super) fn new() -> Self {
        ControllerDescriptionCollector(RefCell::new(CollectorState::new()))
    }

    pub(super) fn finalize(self) -> ControllerDescription {
        let state = self.0.replace(CollectorState::new());
        state.finalize()
    }
}

impl Collect for ControllerDescriptionCollector {
    fn name(&self, name: &str) {
        let prev = self.0.borrow_mut().name.replace(name.to_string());
        if !prev.is_none() {
            panic!("Collect::name called twice")
        }
    }
}

/// Sealed trait used in `Controller::describe` method.
pub trait Collect {
    fn name(&self, name: &str);
}
