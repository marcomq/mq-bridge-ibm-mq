fn main() {
    println!("cargo:rustc-link-search=native=/opt/mqm/lib64");
    println!("cargo:rustc-link-arg=-Wl,-rpath,/opt/mqm/lib64");
}
