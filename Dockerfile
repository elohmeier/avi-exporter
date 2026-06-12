FROM alpine:3.22 AS certs
RUN apk --no-cache add ca-certificates

FROM scratch
ARG TARGETPLATFORM
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY ${TARGETPLATFORM}/avi-exporter /app
EXPOSE 9290
ENTRYPOINT ["/app"]
