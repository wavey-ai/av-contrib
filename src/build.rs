//! The build that a binary of this crate came from, as `build.rs` found it:
//! the commit, whether the tree had changes that were not committed, and
//! when the build was made. A build is named by the pair ([`GIT`],
//! [`UNIX`]): two dirty builds of one commit differ in the time.
//!
//! The same form as av-mesh's, so the tools read both alike:
//! `av-contrib --version` prints `av-contrib 0.1.0 (3f2a9c1d77e0 1763900000)`
//! and `GET /api/node` carries the fields in its `build` block.

/// `git rev-parse HEAD` (40 hex), or `NEEDLETAIL_BUILD_GIT` when it was set
/// at build time, or `unknown` when the sources were not a checkout.
pub const GIT_FULL: &str = env!("AV_CONTRIB_BUILD_GIT_FULL");

/// The tree had changes that were not committed (`git status --porcelain`
/// was not empty).
pub const DIRTY: bool = matches!(env!("AV_CONTRIB_BUILD_DIRTY").as_bytes(), b"true");

/// `SOURCE_DATE_EPOCH` when it was set at build time, else the time
/// `build.rs` ran, in Unix seconds.
pub const UNIX: u64 = decimal(env!("AV_CONTRIB_BUILD_UNIX"));

/// The first 12 hex of [`GIT_FULL`], plus `-dirty` when [`DIRTY`]:
/// `3f2a9c1d77e0`, `3f2a9c1d77e0-dirty`. Safe in a path.
pub const GIT: &str = env!("AV_CONTRIB_BUILD_GIT");

/// The crate's version.
pub const CRATE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// `0.1.0 (3f2a9c1d77e0 1763900000)`: what `--version` prints after the
/// binary's name.
pub const VERSION: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    " (",
    env!("AV_CONTRIB_BUILD_GIT"),
    " ",
    env!("AV_CONTRIB_BUILD_UNIX"),
    ")"
);

/// The decimal number `text`, which `build.rs` wrote.
const fn decimal(text: &str) -> u64 {
    let bytes = text.as_bytes();
    let mut value = 0_u64;
    let mut index = 0;
    while index < bytes.len() {
        assert!(bytes[index].is_ascii_digit(), "build.rs wrote a number");
        value = value * 10 + (bytes[index] - b'0') as u64;
        index += 1;
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_version_names_the_commit_and_the_time() {
        assert_eq!(VERSION, format!("{CRATE_VERSION} ({GIT} {UNIX})"));
        assert_eq!(DIRTY, GIT.ends_with("-dirty"));
        let short = GIT.strip_suffix("-dirty").unwrap_or(GIT);
        assert!(GIT_FULL.starts_with(short), "{GIT_FULL} {GIT}");
        assert!(short.len() <= 12);
        if GIT_FULL != "unknown" {
            assert_eq!(short.len(), GIT_FULL.len().min(12));
        }
        assert!(!GIT.contains(['/', ' ']), "the id is safe in a path: {GIT}");
    }

    #[test]
    fn a_number_from_the_build_script_is_read_at_compile_time() {
        assert_eq!(decimal("0"), 0);
        assert_eq!(decimal("1763900000"), 1_763_900_000);
    }
}
