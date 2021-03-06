use crate::webhook::apis::{
    AdmissionReviewRequest, AdmissionReviewResponse, ApiVersion, Kind, Patch, PatchType, Response,
    Status,
};
use std::fmt;

/// Put this into anyhow::Error to reject operation with provided Status
#[derive(Debug)]
pub struct Rejection(pub Status);

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

    pub fn has_mutations(&self, original: &serde_json::Value) -> bool {
        let modified = match &self.0 {
            Choice::Allow { modified } => modified,
            _ => return false,
        };
        let modified = match modified.as_ref() {
            Some(m) => m,
            None => return false,
        };
        let patch = json_patch::diff(&original, &modified);
        !patch.0.is_empty()
    }

    pub fn finish(self, req: &AdmissionReviewRequest) -> AdmissionReviewResponse {
        let response = match self.0 {
            Choice::Allow { modified } => {
                let patch = modified.map(|modified| {
                    let patch = json_patch::diff(&req.request.object, &modified);
                    let patch =
                        serde_json::to_string(&patch).expect("failed to serialize a json patch");
                    Patch {
                        patch_type: PatchType::JsonPatch,
                        patch: base64::encode(&patch),
                    }
                });

                Response {
                    allowed: true,
                    uid: req.request.uid.clone(),
                    status: None,
                    patch,
                    warnings: self.1,
                }
            }
            Choice::Reject(Rejection(status)) => Response {
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
            kind: Kind,
            api_version: ApiVersion,
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
                assert!(matches!(dec.0, Choice::Allow { .. }));
                Ok(dec)
            }
            Err(err) => match err.downcast::<Rejection>() {
                Ok(rejection) => Ok(Decision::from_choice(Choice::Reject(rejection))),
                Err(internal_error) => Err(internal_error),
            },
        }
    }
}
