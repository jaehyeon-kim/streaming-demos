## Clone the Factor House Local Repository
git clone https://github.com/factorhouse/factorhouse-local.git

## Download Kafka/Flink Connectors and Spark Iceberg Dependencies
./factorhouse-local/resources/setup-env.sh

export KPOW_SUFFIX="-ce"
export FLEX_SUFFIX="-ce"
export LICENSE_PREFIX="community"

unset KPOW_SUFFIX
unset FLEX_SUFFIX
export LICENSE_PREFIX="trial"


export KPOW_LICENSE=/home/jaehyeon/.license/kpow/$LICENSE_PREFIX-license.env
export FLEX_LICENSE=/home/jaehyeon/.license/flex/$LICENSE_PREFIX-license.env

docker compose -p kpow -f ../factorhouse-local/compose-kpow.yml up -d \
  && docker compose -p stripped -f ./compose-stripped.yml up -d \
  && docker compose -p flex -f ./compose-flex.yml up -d

python recsys-engine/eda_recommender.py 

docker compose -p flex -f ./compose-flex.yml down \
  && docker compose -p stripped -f ./compose-stripped.yml down \
  && docker compose -p kpow -f ../factorhouse-local/compose-kpow.yml down
