FROM cnfldemos/cp-server-connect-datagen:0.5.0-6.2.1

# install debezium postgresql connector from confluent hub
RUN confluent-hub install --no-prompt debezium/debezium-connector-postgresql:1.7.1

# install lenses S3 connector as a community connector - https://docs.confluent.io/home/connect/community.html
USER root
RUN mkdir -p /usr/local/share/kafka/plugins/kafka-connect-aws-s3 && \
  curl -SsL https://github.com/lensesio/stream-reactor/releases/download/3.0.0/kafka-connect-aws-s3-3.0.0-2.5.0-all.tar.gz \
    | tar -C /usr/local/share/kafka/plugins/kafka-connect-aws-s3 --warning=no-unknown-keyword -xzf -

# update connect plugin path
ENV CONNECT_PLUGIN_PATH=$CONNECT_PLUGIN_PATH,/usr/local/share/kafka/plugins
USER appuser