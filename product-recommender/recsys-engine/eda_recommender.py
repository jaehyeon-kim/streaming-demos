import argparse
import logging
import time
import random
import sys
import pickle
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import numpy as np
from faker import Faker


from src.logger_config import setup_logger
from src.bandit_manager import BanditManager
from src.models import User
from src.infra import KafkaProducer
from src.bandit_simulator import TimeContextGenerator, GroundTruth

setup_logger()
logger = logging.getLogger(__name__)

# resolve paths
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = Path(sys.modules["__main__"].__file__).parent / "src" / "data"


class EventDrivenRecommender:
    def __init__(
        self,
        data_path: Path,
        redis_host: str,
        redis_port: int,
        redis_pass: str,
        bootstrap: str,
        registry_url: str,
        topic_name: str,
    ):
        self.data_path = data_path

        # Initialize State Manager (Redis DAO)
        self.mgr = BanditManager(
            data_path,
            redis_host=redis_host,
            redis_port=redis_port,
            redis_password=redis_pass,
        )

        # Metadata Containers
        self.artifact = {}
        self.product_features_map = {}

        # Kafka Producer
        self.producer = KafkaProducer(
            bootstrap=bootstrap,
            registry_url=registry_url,
            topic_name=topic_name,
        )

        # LinUCB Hyperparameter
        self.alpha = 1.0

    def load_artifacts(self):
        """Loads scaling artifacts and product features for simulation."""
        logger.info("Loading artifacts...")

        # Load Preprocessing Artifacts (Scaler, User Columns)
        with open(self.data_path / "preprocessing_artifacts.pkl", "rb") as f:
            self.artifact = pickle.load(f)

        # Load Product Features (For Ground Truth / Oracle)
        df_prod = pd.read_csv(self.data_path / "product_features.csv")
        self.product_features_map = df_prod.set_index("product_id").to_dict("index")

        logger.info(f"Loaded artifacts and {len(self.product_features_map)} products.")

    def calculate_linucb_score(
        self, A_inv: np.ndarray, b: np.ndarray, x: np.ndarray
    ) -> float:
        """
        Calculates Disjoint LinUCB Score: (x.T @ A_inv @ b) + alpha * sqrt(x.T @ A_inv @ x)
        """
        # Mean (Theta)
        theta = A_inv @ b
        mean_score = x.dot(theta)

        # Confidence Interval
        variance = x.dot(A_inv).dot(x)
        confidence_bound = self.alpha * np.sqrt(variance)

        return mean_score + confidence_bound

    def recommend(self, user_context: dict, top_k: int = 5):
        """
        Fetches models from Redis, computes scores locally, returns sorted list.
        """
        # Vectorize Context based on Schema (Schema comes from Manager)
        # Note: self.mgr.feature_schema ensures we match the training order
        x_list = [user_context.get(col, 0) for col in self.mgr.feature_schema]
        x = np.array(x_list)

        # Fetch Latest Models from Redis (Batch Get)
        # We use the keys from our product map
        product_ids = list(self.product_features_map.keys())
        # Ensure IDs are strings for Redis lookup if Manager expects strings
        product_ids_str = [str(pid) for pid in product_ids]

        # We fetch all models, which is inefficient for large number of products
        # Consider a two stage architecture in that case
        # 1. Candidate Generation (Retrieval) using a lightweight algorithm (e.g., Vector Search / ANN, or simple category filtering)
        # 2. Ranking (LinUCB) by passing only a subset of IDs
        models = self.mgr.get_models(product_ids_str)

        # Score all products
        scores = {}
        for pid_str, (A, b) in models.items():
            # Convert back to int key for consistency with simulator if needed,
            # or keep as str. The product_features_map uses the type from CSV (likely int).
            # Let's align on int for the return list.
            pid_int = int(pid_str)
            scores[pid_int] = self.calculate_linucb_score(A, b, x)

        # Sort
        ranked_products = sorted(scores, key=scores.get, reverse=True)[:top_k]
        return ranked_products

    def update(
        self, user_context: dict, item_id: int, reward: int, simulated_time: datetime
    ):
        """
        Sends feedback event to Kafka (Mock).
        """
        # Re-Vectorize (Flink needs the raw vector to update A += xxT)
        x_list = [user_context.get(col, 0) for col in self.mgr.feature_schema]

        # Construct Payload
        event = {
            "event_id": f"evt_{int(time.time() * 1000)}",
            "product_id": str(item_id),
            "reward": reward,
            "context_vector": x_list,
            "timestamp": int(simulated_time.timestamp() * 1000),
        }

        # Send
        self.producer.produce(key=str(item_id), value_dict=event)
        self.producer.flush()


def main():
    parser = argparse.ArgumentParser(description="Run EDA Recommender")
    parser.add_argument(
        "--steps", type=int, default=None, help="Number of users to simulate"
    )
    parser.add_argument("--seed", type=int, default=1237, help="Random seed.")
    # Kafka Config
    parser.add_argument("--bootstrap", type=str, default="localhost:9092")
    parser.add_argument("--registry-url", type=str, default="http://localhost:8081")
    parser.add_argument("--topic-name", type=str, default="feedback-events")
    # Redis Config
    parser.add_argument("--redis-host", type=str, default="localhost")
    parser.add_argument("--redis-port", type=int, default=6379)
    parser.add_argument("--redis-pass", type=str, default="redis-pass")
    # Toggle for Bootstrapping (Optional)
    parser.add_argument(
        "--pretrain", action="store_true", help="Seed Redis from CSV before running."
    )
    args = parser.parse_args()

    # Setup
    ANCHOR_DATE = datetime(2026, 1, 1)

    fake = Faker()
    Faker.seed(args.seed)
    random.seed(args.seed)

    # Load User Pool
    users_path = DATA_DIR / "users.csv"
    if not users_path.exists():
        logger.error(f"User file not found at {users_path}")
        return
    users_df = pd.read_csv(users_path)
    user_pool = users_df.to_dict("records")
    logger.info(f"Loaded {len(user_pool)} users")

    # Initialize Helpers
    time_gen = TimeContextGenerator(fake)
    ground_truth = GroundTruth()

    # Initialize Recommender (Serving Layer)
    recsys = EventDrivenRecommender(
        data_path=DATA_DIR,
        bootstrap=args.bootstrap,
        registry_url=args.registry_url,
        topic_name=args.topic_name,
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        redis_pass=args.redis_pass,
    )
    recsys.load_artifacts()

    # OPTIONAL: Bootstrap Redis State
    if args.pretrain:
        recsys.mgr.initialize()

    print(
        f"\n--- STARTING EVENT-DRIVEN LOOP ({args.steps if args.steps is not None else 'infinite'} visits) ---\n"
    )

    s = 0
    while True:
        # Pick Random User
        user_data = random.choice(user_pool)

        # Backfill mandatory fields for User dataclass (excluded in CSV)
        user_data["created_at"] = ANCHOR_DATE - timedelta(days=90)
        user_data["updated_at"] = ANCHOR_DATE - timedelta(days=90)
        user_obj = User.from_dict(user_data)

        # Simulate Time
        simulated_time = fake.date_time_between(
            start_date=ANCHOR_DATE - timedelta(days=7), end_date=ANCHOR_DATE
        )

        # Generate Context (Using class attribute loaded in load_artifacts)
        user_static_feats = User.generate_user_feature(user_obj, recsys.artifact)
        time_feats = time_gen.get_context(dt=simulated_time)
        full_context = {**user_static_feats, **time_feats}

        # Serving (Read Redis)
        recommendations = recsys.recommend(full_context)

        # Simulation (User Reaction)
        chosen_item = recommendations[0]
        reward = 0

        for item_id in recommendations:
            # Fetch product features from the pre-loaded map
            item_features = recsys.product_features_map[item_id]
            if ground_truth.will_click(full_context, item_features, fake):
                chosen_item = item_id
                reward = 1
                break

        # Feedback (Write Kafka)
        recsys.update(full_context, chosen_item, reward, simulated_time)

        # Log
        print(
            f"User {str(user_obj.user_id).rjust(4, '0')} ({user_obj.age} yo) @ {simulated_time.strftime('%a %H:%M')} "
            f"-> Recs: [{', '.join([str(r).rjust(3, '0') for r in recommendations])}] -> Clicked: {str(chosen_item).rjust(3, '0')} ({'✅' if reward else '❌'})"
        )

        # Control break condition
        s += 1
        if args.steps is not None and s >= args.steps:
            break
        time.sleep(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n--- END SIMULATION ---")
