use std::env::var;

fn main() {
    let profile = var("PROFILE").unwrap();
    println!("cargo:rustc-env=PROFILE={profile}");
}
