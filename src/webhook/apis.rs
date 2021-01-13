//! Low-level types

#[derive(serde::Serialize, Debug, Clone)]
pub struct Status {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl Status {
    pub fn is_empty(&self) -> bool {
        self.code.is_none() && self.message.is_none()
    }
}

macro_rules! define_const_string {
    ($string:literal, $name: ident) => {
        #[derive(Debug, Copy, Clone)]
        pub struct $name;

        impl serde::ser::Serialize for $name {
            fn serialize<S: serde::ser::Serializer>(
                &self,
                serializer: S,
            ) -> Result<S::Ok, S::Error> {
                $string.serialize(serializer)
            }
        }

        impl<'de> serde::de::Deserialize<'de> for $name {
            fn deserialize<D: serde::de::Deserializer<'de>>(
                deserializer: D,
            ) -> Result<Self, D::Error> {
                let s = String::deserialize(deserializer)?;
                if s != $string {
                    return Err(<D::Error as serde::de::Error>::custom(format_args!(
                        "expected string '{}', got '{}'",
                        $string, s
                    )));
                }
                Ok(Self)
            }
        }
    };
}

define_const_string!("admission.k8s.io/v1", ApiVersion);
define_const_string!("AdmissionReview", Kind);

#[derive(serde::Deserialize, Debug, Clone)]
pub struct AdmissionReviewRequest {
    #[serde(rename = "apiVersion")]
    pub api_version: ApiVersion,
    pub kind: Kind,
    pub request: Request,
}

#[derive(serde::Deserialize, Debug, Clone)]
pub struct Request {
    pub uid: String,
    pub object: serde_json::Value,
    pub namespace: String,
}

#[derive(serde::Serialize, Debug, Clone)]
pub struct AdmissionReviewResponse {
    #[serde(rename = "apiVersion")]
    pub api_version: ApiVersion,
    pub kind: Kind,
    pub response: Response,
}

#[derive(serde::Serialize, Debug, Clone)]
pub struct Response {
    pub uid: String,
    pub allowed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<Status>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    pub patch: Option<Patch>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

#[derive(serde::Serialize, Debug, Clone)]
pub struct Patch {
    #[serde(rename = "patchType")]
    pub patch_type: PatchType,
    pub patch: String,
}

#[derive(serde::Serialize, Debug, Clone, Copy)]
pub enum PatchType {
    #[serde(rename = "JSONPatch")]
    JsonPatch,
}
