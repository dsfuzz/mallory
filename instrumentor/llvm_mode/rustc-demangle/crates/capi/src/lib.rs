extern crate rustc_demangle;

use std::os::raw::c_char;
use std::ptr;

/// C-style interface for demangling.
/// Demangles symbol given in `mangled` argument into `out` buffer
///
/// Unsafe as it handles buffers by raw pointers.
///
/// Returns 0 if successful, 1 otherwise.

#[no_mangle]
pub unsafe extern "C" fn rustc_demangle(
    mangled: *const c_char,
    demangled_status: *mut i32,
) -> *const c_char {
    let mangled_str = match std::ffi::CStr::from_ptr(mangled).to_str() {
        Ok(s) => s,
        Err(_) => {
            *demangled_status = 1;
            return ptr::null()
        },
    };
    match rustc_demangle::try_demangle(mangled_str) {
        Ok(demangle) => {
            let demangled = demangle.to_string();
            let c_string = std::ffi::CString::new(demangled).unwrap();
            *demangled_status = 0;
            return c_string.into_raw()
        }
        Err(_) =>{
            *demangled_status = 1;
            return ptr::null()
        }
    }
}

#[cfg(test)]
mod tests {
    use std;
    use std::os::raw::c_char;
    #[test]
    fn demangle_c_str_large() {
        let mangled = "_ZN4testE\0";
        let mut demangled_status = -1;
        let res = unsafe {
            super::rustc_demangle(
                mangled.as_ptr() as *const c_char,
                &mut demangled_status,
            )
        };
        assert_eq!(demangled_status, 1);
    }

    #[test]
    fn demangle_c_str_exact() {
        let mangled = "_ZN4testE\0";
        let mut demangled_status = -1;
        let res = unsafe {
            super::rustc_demangle(
                mangled.as_ptr() as *const c_char,
                &mut demangled_status,
            )
        };
        assert_eq!(demangled_status, 1);
    }
}
