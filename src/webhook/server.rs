use crate::webhook::{
    apis::{AdmissionReviewRequest, AdmissionReviewResponse, Status},
    Decision,
};
use k8s_openapi::Resource;

use super::Rejection;

/// Type that is able to review admission requests
pub trait Review: 'static {
    /// Resource to review
    type Resource: kube::api::Meta + serde::ser::Serialize + serde::de::DeserializeOwned + Clone;

    /// Review a resource.
    /// # Errors
    /// If the returned error does not have a Rejection in its context stack
    /// '500 server error' review is returned and the error is logged.
    fn review(&self, resource: Self::Resource) -> anyhow::Result<Self::Resource>;
}

trait DynReview {
    fn api_version(&self) -> &'static str;
    fn kind(&self) -> &'static str;

    fn review(&self, resource: &serde_json::Value) -> anyhow::Result<serde_json::Value>;
}

struct DynReviewer<R: Review>(R);
impl<R: Review> DynReview for DynReviewer<R> {
    fn api_version(&self) -> &'static str {
        R::Resource::API_VERSION
    }

    fn kind(&self) -> &'static str {
        R::Resource::KIND
    }

    fn review(&self, resource: &serde_json::Value) -> anyhow::Result<serde_json::Value> {
        let resource: R::Resource = match serde_json::from_value(resource.clone()) {
            Ok(r) => r,
            Err(err) => {
                let err = Rejection(Status {
                    code: Some(400),
                    message: Some(format!("Input object is incorrect: {}", err)),
                });
                return Err(anyhow::Error::msg(err));
            }
        };

        let modified = self.0.review(resource.clone())?;
        Ok(serde_json::to_value(&modified)?)
    }
}

/// High-level admission workflow
pub struct Server {
    reviewers: Vec<Box<dyn DynReview>>,
}

impl Server {
    /// Returns a builder that can be used to configure the server
    pub fn builder() -> ServerBuilder {
        ServerBuilder(Self { reviewers: vec![] })
    }

    fn find_reviewer(&self, api_version: &str, kind: &str) -> Option<&dyn DynReview> {
        for reviewer in &self.reviewers {
            if reviewer.api_version() == api_version && reviewer.kind() == kind {
                return Some(&**reviewer);
            }
        }
        None
    }

    /// Common part for mutation and validation
    fn decide(&self, review: &AdmissionReviewRequest) -> Decision {
        let obj: kube::api::Object<serde_json::Value, serde_json::Value> =
            match serde_json::from_value(review.request.object.clone()) {
                Ok(o) => o,
                Err(err) => {
                    return Decision::reject_with_message(&format!(
                        "Input object does not conform to k8s conventions: {}",
                        err
                    ))
                }
            };

        let reviewer = match self.find_reviewer(&obj.types.api_version, &obj.types.kind) {
            Some(reviewer) => reviewer,
            _ => {
                return Decision::reject_with_message(&format!(
                    "Unexpected input object type: {}/{}",
                    obj.types.api_version, obj.types.kind
                ))
            }
        };

        let result = reviewer.review(&review.request.object);
        Decision::from_result(result).unwrap_or_else(|err| {
            tracing::error!("{:#}", err);
            Decision::reject_with_status(Status {
                code: Some(500),
                message: Some("Internal error".to_string()),
            })
        })
    }

    /// Entrypoint for validation requests
    pub fn validation(&self, review: &AdmissionReviewRequest) -> AdmissionReviewResponse {
        let mut decision = self.decide(review);
        if decision.has_mutations(&review.request.object) {
            decision = Decision::reject_with_message(&format!("Mutations were not applied"));
        }
        decision.finish(review)
    }

    /// Entrypoint for mutation requests
    pub fn mutation(&self, review: &AdmissionReviewRequest) -> AdmissionReviewResponse {
        let decision = self.decide(review);
        decision.finish(review)
    }
}

pub struct ServerBuilder(Server);

impl ServerBuilder {
    /// Adds a new reviewer
    /// # Panics
    /// Panics if there already is a reviewer for the same apiVersion & kind.
    pub fn add_reviewer<R: Review>(&mut self, reviewer: R) -> &mut Self {
        let new = DynReviewer::<R>(reviewer);
        if self
            .0
            .find_reviewer(new.api_version(), new.kind())
            .is_some()
        {
            panic!(
                "Reviewer already registered for {}/{}",
                new.api_version(),
                new.kind()
            )
        }
        self.0.reviewers.push(Box::new(new));

        self
    }

    /// Finishes construction, returning built Server instance.
    pub fn build(self) -> Server {
        self.0
    }
}
