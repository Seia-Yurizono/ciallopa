# syntax=docker/dockerfile:1
# Runtime image for the static musl release binary (amd64 or arm64, matching the build platform;
# both base images below are multi-arch indexes). Build context: a directory holding
# `ciallopa` and `config.example.toml` (an unpacked release tar.gz).
#   docker build -f packaging/docker/Dockerfile -t ciallopa <context>

# Pin both inputs so the smoke-tested and published builds use the same base images.
# Refresh the digests together with the release smoke test; see docs/DEPLOYMENT.md.
# Writable mount points owned by the runtime user (named volumes inherit this ownership).
FROM busybox:1.37-musl@sha256:5cec3fc171c87218698e85a52af7087de727372aae264a787b8112901a5b0092 AS dirs
RUN mkdir -p /out/etc/ciallopa /out/data && chmod 700 /out/etc/ciallopa /out/data

FROM gcr.io/distroless/static-debian13:nonroot@sha256:d62067879de890a1def9da0e7766b7d20810a5e5555cef8506b752437d7a3e88
ARG VERSION=dev
ARG REVISION=
LABEL org.opencontainers.image.title="ciallopa" \
      org.opencontainers.image.description="Lightweight AI gateway" \
      org.opencontainers.image.url="https://github.com/Seia-Yurizono/ciallopa" \
      org.opencontainers.image.source="https://github.com/Seia-Yurizono/ciallopa" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}"
COPY --chmod=755 ciallopa /usr/bin/ciallopa
COPY --chmod=644 config.example.toml /usr/share/ciallopa/
COPY --from=dirs --chown=65532:65532 /out/etc/ciallopa /etc/ciallopa
COPY --from=dirs --chown=65532:65532 /out/data /var/lib/ciallopa/.ciallopa
# `~/.ciallopa/...` paths in the config resolve to /var/lib/ciallopa/.ciallopa/... (as in the .deb).
# `ciallopa init` writes listen = "0.0.0.0:8317" here, so the published port reaches the server.
ENV CIALLOPA_CONFIG=/etc/ciallopa/config.toml \
    CIALLOPA_INIT_LISTEN=0.0.0.0:8317 \
    HOME=/var/lib/ciallopa
WORKDIR /var/lib/ciallopa
USER 65532:65532
EXPOSE 8317
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --start-interval=2s --retries=3 \
    CMD ["/usr/bin/ciallopa", "healthcheck"]
ENTRYPOINT ["/usr/bin/ciallopa"]
CMD ["serve"]
