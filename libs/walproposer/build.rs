//! Links with walproposer, pgcommon, pgport and runs bindgen on walproposer.h
//! to generate Rust bindings for it.

use std::{env, path::PathBuf, process::Command};

use anyhow::{anyhow, Context};
use bindgen::CargoCallbacks;

fn main() -> anyhow::Result<()> {
    // Tell cargo to invalidate the built crate whenever the wrapper changes
    println!("cargo:rerun-if-changed=bindgen_deps.h");

    // Finding the location of built libraries and Postgres C headers:
    // - if POSTGRES_INSTALL_DIR is set look into it, otherwise look into `<project_root>/pg_install`
    // - if there's a `bin/pg_config` file use it for getting include server, otherwise use `<project_root>/pg_install/{PG_MAJORVERSION}/include/postgresql/server`
    let pg_install_dir = if let Some(postgres_install_dir) = env::var_os("POSTGRES_INSTALL_DIR") {
        postgres_install_dir.into()
    } else {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../pg_install")
    };

    let pg_install_abs = std::fs::canonicalize(pg_install_dir)?;
    let walproposer_lib_dir = pg_install_abs.join("build/walproposer-lib");
    let walproposer_lib_search_str = walproposer_lib_dir
        .to_str()
        .ok_or(anyhow!("Bad non-UTF path"))?;

    let pgxn_neon = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../pgxn/neon");
    let pgxn_neon = std::fs::canonicalize(pgxn_neon)?;
    let pgxn_neon = pgxn_neon.to_str().ok_or(anyhow!("Bad non-UTF path"))?;

    // #[cfg(debug_assertions)]
    // {
    //     println!("cargo:rustc-link-arg=-fsanitize=address");
    //     println!("cargo:rustc-link-arg=-fsanitize=undefined");
    //
    //     //setting dynamically the symbols
    //     let libasan_path = Command::new("gcc")
    //         .arg("-print-file-name=libasan.so")
    //         .output()
    //         .context("failed to execute `gcc -print-file-name=libasan.so`")?;
    //
    //     if !libasan_path.status.success() {
    //         println!("stdout: {}", String::from_utf8_lossy(&libasan_path.stdout));
    //         println!("stderr: {}", String::from_utf8_lossy(&libasan_path.stderr));
    //         panic!("`gcc -print-file-name=libasan.so` failed")
    //     }
    //
    //     let libubsan_path = Command::new("gcc")
    //         .arg("-print-file-name=libubsan.so")
    //         .output()
    //         .context("failed to execute `gcc -print-file-name=libubsan.so`")?;
    //
    //     if !libasan_path.status.success() {
    //         println!("stdout: {}", String::from_utf8_lossy(&libubsan_path.stdout));
    //         println!("stderr: {}", String::from_utf8_lossy(&libubsan_path.stderr));
    //         panic!("`gcc -print-file-name=libubsan.so` failed")
    //     }
    //
    //     let libasan_path = String::from_utf8(libasan_path.stdout).unwrap();
    //     let libubsan_path = String::from_utf8(libubsan_path.stdout).unwrap();
    //     println!(
    //         "cargo:rustc-env=LD_PRELOAD={}:{}",
    //         libasan_path,libubsan_path
    //     );
    // }

    println!("cargo:rustc-link-lib=static=pgport");
    println!("cargo:rustc-link-lib=static=pgcommon");
    println!("cargo:rustc-link-lib=static=walproposer");
    println!("cargo:rustc-link-search={walproposer_lib_search_str}");

    let pg_config_bin = pg_install_abs.join("v16").join("bin").join("pg_config");
    let inc_server_path: String = if pg_config_bin.exists() {
        let output = Command::new(pg_config_bin)
            .arg("--includedir-server")
            .output()
            .context("failed to execute `pg_config --includedir-server`")?;

        if !output.status.success() {
            println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
            println!("stderr: {}", String::from_utf8_lossy(&output.stderr));
            panic!("`pg_config --includedir-server` failed")
        }

        String::from_utf8(output.stdout)
            .context("pg_config output is not UTF-8")?
            .trim_end()
            .into()
    } else {
        let server_path = pg_install_abs
            .join("v16")
            .join("include")
            .join("postgresql")
            .join("server")
            .into_os_string();
        server_path
            .into_string()
            .map_err(|s| anyhow!("Bad postgres server path {s:?}"))?
    };

    // The bindgen::Builder is the main entry point
    // to bindgen, and lets you build up options for
    // the resulting bindings.
    let bindings = bindgen::Builder::default()
        // The input header we would like to generate
        // bindings for.
        .header("bindgen_deps.h")
        // Tell cargo to invalidate the built crate whenever any of the
        // included header files changed.
        .parse_callbacks(Box::new(CargoCallbacks))
        .allowlist_type("WalProposer")
        .allowlist_type("WalProposerConfig")
        .allowlist_type("walproposer_api")
        .allowlist_function("WalProposerCreate")
        .allowlist_function("WalProposerStart")
        .allowlist_function("WalProposerBroadcast")
        .allowlist_function("WalProposerPoll")
        .allowlist_function("WalProposerFree")
        .allowlist_var("DEBUG5")
        .allowlist_var("DEBUG4")
        .allowlist_var("DEBUG3")
        .allowlist_var("DEBUG2")
        .allowlist_var("DEBUG1")
        .allowlist_var("LOG")
        .allowlist_var("INFO")
        .allowlist_var("NOTICE")
        .allowlist_var("WARNING")
        .allowlist_var("ERROR")
        .allowlist_var("FATAL")
        .allowlist_var("PANIC")
        .allowlist_var("WPEVENT")
        .allowlist_var("WL_LATCH_SET")
        .allowlist_var("WL_SOCKET_READABLE")
        .allowlist_var("WL_SOCKET_WRITEABLE")
        .allowlist_var("WL_TIMEOUT")
        .allowlist_var("WL_SOCKET_CLOSED")
        .allowlist_var("WL_SOCKET_MASK")
        .clang_arg("-DWALPROPOSER_LIB")
        .clang_arg(format!("-I{pgxn_neon}"))
        .clang_arg(format!("-I{inc_server_path}"))
        // Finish the builder and generate the bindings.
        .generate()
        // Unwrap the Result and panic on failure.
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap()).join("bindings.rs");
    bindings
        .write_to_file(out_path)
        .expect("Couldn't write bindings!");

    Ok(())
}
