FROM ubuntu:21.04
COPY ./example-lock /usr/bin/example-lock
ENV RUST_LOG=info,kube_utils=debug
ENTRYPOINT [ "/usr/bin/example-lock" ]