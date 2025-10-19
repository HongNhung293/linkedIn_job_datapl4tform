
# FROM spark:latest

# USER root

# RUN apt-get update && apt-get install -y curl \
#     && rm -rf /var/lib/apt/lists/*


# RUN cd /opt/spark/jars && \
#     curl -sLO https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.18.41/s3-2.18.41.jar && \
#     curl -sLO https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.367/aws-java-sdk-1.12.367.jar && \
#     curl -sLO https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.2.0/delta-core_2.12-2.2.0.jar && \
#     curl -sLO https://repo1.maven.org/maven2/io/delta/delta-storage/2.2.0/delta-storage-2.2.0.jar

# USER spark

FROM spark:3.5.7-scala2.12-java11-ubuntu

USER root

RUN set -ex; \
    apt-get update; \
    apt-get install -y python3 python3-pip; \
    apt-get install -y r-base r-base-dev; \
    rm -rf /var/lib/apt/lists/*

ENV R_HOME=/usr/lib/R

USER spark