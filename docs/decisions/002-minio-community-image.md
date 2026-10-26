# ADR 002: MinIO from the pgsty community image

**Status:** accepted (D1-AM, 2026-10-26)

## Context
SPEC.md names MinIO as the S3-compatible store. When this was checked on 2026-10-26:
- The `minio/minio` GitHub repository was archived (last push April 2026). Its last release tag is `RELEASE.2025-10-15T17-29-55Z`.
- The official `minio/minio` and `minio/mc` images couldn't be pulled anonymously from Docker Hub or Quay.

## Decision
Use the community-maintained fork images, pinned by tag:
- Server: `pgsty/minio:RELEASE.2026-08-04T00-00-00Z`. It is multi-arch with arm64 and had about 2.7M pulls.
- Client: `pgsty/mc:RELEASE.2026-09-16T00-00-00Z`, used by the one-off `minio-init` service to create the `warehouse` bucket.

The S3 API, `minio server` command and `mc` CLI are unchanged, so no project code depends on the fork.

## Alternatives considered
- **Build MinIO from the last official source tag** in a local Dockerfile. This uses official code but no longer receives fixes, and it makes the first build slower.
- **Another S3-compatible store** (SeaweedFS, Garage). This departs from SPEC.md.

## Consequences
- The project depends on a third-party image. If it disappears, switch to the build-from-source option. Only the `image:` lines in `docker-compose.yml` change.
- Data is reached through the S3 API with path-style access, so moving to AWS S3 is still only a configuration change, as SPEC.md section 3.3 intends.
