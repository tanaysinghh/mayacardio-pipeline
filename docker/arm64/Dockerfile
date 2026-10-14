FROM eclipse-temurin:17-jre-jammy

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 procps iproute2 \
    && rm -rf /var/lib/apt/lists/*

COPY dist/device-1.0.0-all.jar dist/biosignal-mock-standin.jar dist/pipeline-tools.jar dist/cloud-1.0.0-boot.jar /opt/pipeline/lib/
COPY scripts/pipeline scripts/measure-resources.sh scripts/biosignal-mock scripts/check_ground_truth docker/arm64/entrypoint.sh /opt/pipeline/bin/
RUN chmod 0755 /opt/pipeline/bin/* && ln -s /opt/pipeline/bin/pipeline /usr/local/bin/pipeline \
    && ln -s /opt/pipeline/bin/biosignal-mock /usr/local/bin/biosignal-mock \
    && ln -s /opt/pipeline/bin/check_ground_truth /usr/local/bin/check_ground_truth

ENV PIPELINE_HOME=/opt/pipeline \
    PIPELINE_ALLOW_ROOT=1 \
    PIPELINE_CONFIG=/etc/pipeline/pipeline.conf

ENTRYPOINT ["/opt/pipeline/bin/entrypoint.sh"]
