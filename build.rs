//! The build id of `av_contrib::build`: the commit, whether the tree had
//! changes that were not committed, and when the build was made.
//!
//! A build is named by the pair (commit, time): two dirty builds of one
//! commit differ in the time. `NEEDLETAIL_BUILD_GIT` names the commit in
//! place of `git rev-parse HEAD` (a second build of the same sources for a
//! local roll); `SOURCE_DATE_EPOCH` fixes the time.

use std::path::Path;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

/// Characters of the commit in the short id.
const SHORT_GIT_LEN: usize = 12;

/// The standard output of `git <args>`, without its last newline; `None`
/// when git is not there, or this is not a checkout.
fn git(args: &[&str]) -> Option<String> {
    // `--no-optional-locks`: a build never takes the index lock from a git
    // command that the developer is running.
    let output = Command::new("git")
        .arg("--no-optional-locks")
        .args(args)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    Some(text.trim_end_matches(['\n', '\r']).to_owned())
}

fn main() {
    println!("cargo:rerun-if-env-changed=NEEDLETAIL_BUILD_GIT");
    println!("cargo:rerun-if-env-changed=SOURCE_DATE_EPOCH");
    for path in ["build.rs", "src", "Cargo.toml", "Cargo.lock"] {
        println!("cargo:rerun-if-changed={path}");
    }
    // A worktree's `.git` is a file, so git names the real HEAD and ref
    // files. A path that is not there is left out: cargo reads a missing
    // file as changed on every build.
    let mut git_files = vec!["HEAD".to_owned(), "packed-refs".to_owned()];
    git_files.extend(git(&["symbolic-ref", "-q", "HEAD"]));
    for name in git_files {
        if let Some(path) = git(&["rev-parse", "--git-path", &name]) {
            if Path::new(&path).exists() {
                println!("cargo:rerun-if-changed={path}");
            }
        }
    }

    let git_full = std::env::var("NEEDLETAIL_BUILD_GIT")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| value.trim().to_owned())
        .or_else(|| git(&["rev-parse", "HEAD"]).filter(|head| !head.is_empty()))
        .unwrap_or_else(|| "unknown".to_owned());
    let dirty = git(&["status", "--porcelain"]).is_some_and(|status| !status.is_empty());
    let mut short: String = git_full.chars().take(SHORT_GIT_LEN).collect();
    if dirty {
        short.push_str("-dirty");
    }
    let unix = std::env::var("SOURCE_DATE_EPOCH")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |since| since.as_secs())
        });

    println!("cargo:rustc-env=AV_CONTRIB_BUILD_GIT_FULL={git_full}");
    println!("cargo:rustc-env=AV_CONTRIB_BUILD_GIT={short}");
    println!("cargo:rustc-env=AV_CONTRIB_BUILD_DIRTY={dirty}");
    println!("cargo:rustc-env=AV_CONTRIB_BUILD_UNIX={unix}");
}
