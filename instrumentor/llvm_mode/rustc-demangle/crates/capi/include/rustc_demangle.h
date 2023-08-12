#ifndef RUST_DEMANGLE_H_
#define RUST_DEMANGLE_H_

#ifdef __cplusplus
extern "C" {
#endif

// Demangles symbol given in `mangled` argument into `out` buffer
//
// Returns 0 if successful, 1 otherwise
char* rustc_demangle(const char *mangled, int* demangled_status);

#ifdef __cplusplus
}
#endif

#endif // RUSTC_DEMANGLE_H_
