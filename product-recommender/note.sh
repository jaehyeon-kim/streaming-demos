unset KPOW_SUFFIX
unset FLEX_SUFFIX
export LICENSE_PREFIX="trial"
export KPOW_LICENSE=/home/jaehyeon/.license/kpow/$LICENSE_PREFIX-license.env
export FLEX_LICENSE=/home/jaehyeon/.license/flex/$LICENSE_PREFIX-license.env

docker compose -p kpow -f ./factorhouse-local/compose-kpow.yml up -d \
  && docker compose -p flex -f ./factorhouse-local/compose-flex.yml up -d


USE_EXT=false docker compose -p flex -f ./factorhouse-local/compose-flex.yml up -d redis

python product-recommender/recsys-engine/eda_recommender.py --init-redis

USE_EXT=false docker compose -p flex -f ./factorhouse-local/compose-flex.yml down