use apis::{AdmissionReviewRequest, AdmissionReviewResponse, Status};
use std::fmt;

pub mod apis;

#[derive(Debug)]
pub struct Rejection(pub apis::Status);

impl fmt::Display for Rejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Rejection")
            .field("code", &self.0.code)
            .field("message", &self.0.message)
            .finish()
    }
}

enum Choice {
    Allow { modified: Option<serde_json::Value> },
    Reject(Rejection),
}

/// High-level builder for webhook decisions
pub struct Decision(Choice, Vec<String>);

impl Decision {
    fn from_choice(choice: Choice) -> Self {
        Decision(choice, vec![])
    }

    pub fn allow() -> Self {
        Decision::from_choice(Choice::Allow { modified: None })
    }
    pub fn allow_and_modify(modified: serde_json::Value) -> Self {
        Decision::from_choice(Choice::Allow {
            modified: Some(modified),
        })
    }

    pub fn reject_with_message(message: &str) -> Decision {
        Decision::from_choice(Choice::Reject(Rejection(Status {
            code: Some(400),
            message: Some(message.to_string()),
        })))
    }

    pub fn reject_with_status(status: Status) -> Decision {
        Decision::from_choice(Choice::Reject(Rejection(status)))
    }

    pub fn reject_silently() -> Decision {
        Decision::from_choice(Choice::Reject(Rejection(Status {
            code: None,
            message: None,
        })))
    }

    pub fn add_warning(&mut self, warn: &str) -> &mut Self {
        self.1.push(warn.to_string());
        self
    }

    pub fn finish(self, req: &AdmissionReviewRequest) -> AdmissionReviewResponse {
        let response = match self.0 {
            Choice::Allow { modified } => {
                let patch = modified.map(|modified| {
                    let patch = json_patch::diff(&req.request.object, &modified);
                    let patch =
                        serde_json::to_string(&patch).expect("failed to serialize a json patch");
                    apis::Patch {
                        patch_type: apis::PatchType::JsonPatch,
                        patch: base64::encode(&patch),
                    }
                });

                apis::Response {
                    allowed: true,
                    uid: req.request.uid.clone(),
                    status: None,
                    patch,
                    warnings: self.1,
                }
            }
            Choice::Reject(Rejection(status)) => apis::Response {
                allowed: false,
                uid: req.request.uid.clone(),
                status: if status.is_empty() {
                    None
                } else {
                    Some(status)
                },
                patch: None,
                warnings: self.1,
            },
        };
        AdmissionReviewResponse {
            kind: apis::Kind,
            api_version: apis::ApiVersion,
            response,
        }
    }
}

pub trait ToAllow {
    /// Must return Decision::Allow
    fn make_decision(self) -> Decision;
}

impl ToAllow for () {
    fn make_decision(self) -> Decision {
        Decision::allow()
    }
}

impl ToAllow for serde_json::Value {
    fn make_decision(self) -> Decision {
        Decision::allow_and_modify(self)
    }
}

impl Decision {
    pub fn from_result<T: ToAllow>(res: anyhow::Result<T>) -> anyhow::Result<Decision> {
        match res {
            Ok(ok) => {
                let dec = ok.make_decision();
                assert!(matches!(dec.0, Choice::Allow {..}));
                Ok(dec)
            }
            Err(err) => match err.downcast::<Rejection>() {
                Ok(rejection) => Ok(Decision::from_choice(Choice::Reject(rejection))),
                Err(internal_error) => Err(internal_error),
            },
        }
    }
}
