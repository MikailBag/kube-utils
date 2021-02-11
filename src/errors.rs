//! Error utilities

/// Describes error reason
#[non_exhaustive]
#[derive(PartialEq, Eq)]
pub enum ErrorClass {
    NotFound,
    Conflict,
    AlreadyExists,
    /// Catch-all variant. Since this enum is non-exhaustive,
    /// it should be handled with `_` pattern
    Unknown,
}

/// Tries to classify k8s error
pub fn classify(error_reason: &str) -> ErrorClass {
    match error_reason {
        "NotFound" => ErrorClass::NotFound,
        "Conflict" => ErrorClass::Conflict,
        "AlreadyExists" => ErrorClass::AlreadyExists,
        _ => ErrorClass::Unknown,
    }
}

/// Tries to classify k8s error, wrapped in kube Error
pub fn classify_kube(error: &kube::Error) -> ErrorClass {
    match error {
        kube::Error::Api(api) => classify(&api.reason),
        _ => ErrorClass::Unknown,
    }
}
