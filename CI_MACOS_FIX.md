# macOS CI Build Fix

## Problem
The GitHub Actions CI workflow (`ci.yml`) was failing when building for macOS. The issue was that environment variables needed for building the rdkafka Rust crate with its native dependencies were not being properly propagated to the cargo build step.

## Root Cause
While the "Install dependencies on macOS" step correctly set environment variables like `OPENSSL_DIR`, `PKG_CONFIG_PATH`, `LDFLAGS`, and `CPPFLAGS` and added them to `$GITHUB_ENV`, these variables needed to be explicitly available during the cargo build process. The rdkafka-sys crate's build script requires these variables to locate OpenSSL and librdkafka during compilation.

## Solution
Added an explicit `env:` block to the "Build with Cargo" step that references the environment variables set in previous steps:

```yaml
- name: Build with Cargo
  env:
    OPENSSL_DIR: ${{ env.OPENSSL_DIR }}
    PKG_CONFIG_PATH: ${{ env.PKG_CONFIG_PATH }}
    LDFLAGS: ${{ env.LDFLAGS }}
    CPPFLAGS: ${{ env.CPPFLAGS }}
  run: |
    # ... build commands
```

This ensures that:
1. `OPENSSL_DIR` - Points to the OpenSSL installation (required by openssl-sys crate)
2. `PKG_CONFIG_PATH` - Tells pkg-config where to find .pc files for OpenSSL, librdkafka, zstd, and lz4
3. `LDFLAGS` - Tells the linker where to find the shared libraries
4. `CPPFLAGS` - Tells the C compiler where to find header files

## Additional Improvements
Added debug output to print the environment variable values during the build, which will help with troubleshooting if similar issues arise in the future.

## Testing
The fix should be validated by running the CI workflow on a push to the main branch or by creating a tag. The workflow will:
1. Install dependencies via Homebrew
2. Set environment variables
3. Verify dependencies are installed correctly
4. Build the project with the environment variables properly set
5. Create release artifacts

## References
- CLAUDE.md - Documents the required environment variables for macOS builds
- `.github/workflows/ci.yml` - The updated CI workflow file
