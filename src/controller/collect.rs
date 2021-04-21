use k8s_openapi::{api::rbac::v1::PolicyRule, apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition};

use crate::controller::ControllerDescription;
use std::cell::RefCell;

struct CollectorState {
    name: Option<String>,
    crd: Option<CustomResourceDefinition>,
    crd_set: bool,
    rbac: Option<Vec<PolicyRule>>
}

impl CollectorState {
    fn new() -> Self {
        CollectorState {
            name: None,
            crd: None,
            crd_set: false,
            rbac: None,
        }
    }

    fn finalize(self) -> ControllerDescription {
        if !self.crd_set {
            panic!("Neither Collect::crd nor Collect::no_crd were called");
        }
        ControllerDescription {
            name: self.name.expect("Collect::name not called"),
            crd: self.crd,
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

    fn check_crd_flag(&self) {
        if self.0.borrow().crd_set {
            panic!("Collect::crd or Collect::no_crd called twice")
        }
        self.0.borrow_mut().crd_set = true;
    }
}

impl Collect for ControllerDescriptionCollector {
    fn name(&self, name: &str) -> &Self {
        let prev = self.0.borrow_mut().name.replace(name.to_string());
        if prev.is_some() {
            panic!("Collect::name called twice")
        }
        self
    }

    fn crd(&self, crd: CustomResourceDefinition) -> &Self {
        self.check_crd_flag();
        self.0.borrow_mut().crd.replace(crd);
        
        self
    }

    fn no_crd(&self) -> &Self {
        self.check_crd_flag();

        self
    }

    fn role(&self, rules: Vec<PolicyRule>) -> &Self {
        let prev = self.0.borrow_mut().rbac.replace(rules);
        if prev.is_some() {
            panic!("Collect::role called twice");
        }
        self
    }
}

/// Sealed trait used in `Controller::describe` method.
pub trait Collect {
    fn name(&self, name: &str) -> &Self;
    /// specifies that top-level resource is CustomResourceDefinition.
    /// passed CRD must be equal to `Resource` (it is validated that GVK match).
    fn crd(&self, crd: CustomResourceDefinition) -> &Self;
    /// specifies that top-level resource is not CRD
    fn no_crd(&self) -> &Self;
    /// Receives body of the role that is sufficient for the controller
    /// to run.
    fn role(&self, rules: Vec<PolicyRule>) -> &Self;
}
