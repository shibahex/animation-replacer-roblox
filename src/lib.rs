use rbx_dom_weak::{Instance, WeakDom};

/// A module for uploading animations
pub mod animation;

/// A module for handling parsing and editing studio files
pub mod studio;

/// A module for handling parsing and editing on scripts, in studio files.
pub mod script;

pub use animation::uploader::AnimationUploader;
pub use studio::workplace_parser::StudioParserBuilder;

/// Represents an animation with its instance and ID.
#[derive(Debug, Clone)]
pub struct Animation<'a> {
    pub instance: &'a Instance,
    pub animation_id: String,
}

impl<'a> Animation<'a> {
    pub fn new(instance: &'a Instance, animation_id: String) -> Self {
        Self {
            instance,
            animation_id,
        }
    }

    pub fn with_info(instance: &'a Instance, animation_id: String) -> Self {
        Self {
            instance,
            animation_id,
        }
    }
}
/// Parser for Roblox Studio files with optional authentication.
pub struct StudioParser {
    pub roblosecurity: Option<String>,
    pub dom: WeakDom,
}

/// Represents a script with its instance, source code and type.
pub struct Script<'a> {
    pub instance: &'a mut Instance,
    pub source: String,
    pub script_type: ScriptType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScriptType {
    Script,
    LocalScript,
    ModuleScript,
    Unknown(String), // fallback for non-standard classes
}
