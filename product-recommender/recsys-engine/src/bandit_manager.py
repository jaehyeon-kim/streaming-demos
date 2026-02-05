import json
import pickle
import logging
from pathlib import Path
from typing import List, Dict, Tuple

import numpy as np
import pandas as pd
import redis

from src.logger_config import setup_logger

setup_logger()
logger = logging.getLogger(__name__)


class BanditManager:
    def __init__(
        self, data_path: Path, redis_host: str, redis_port: int, redis_password: str
    ):
        self.data_path = data_path
        self.r = redis.Redis(
            host=redis_host,
            port=redis_port,
            password=redis_password,
            decode_responses=True,
        )
        self.feature_schema = []
        self.product_ids = []

        # Load schema and products immediately
        self._load_metadata()

    def _load_metadata(self):
        """Internal helper to load column schema and product list."""
        # Load Schema
        artifact_path = self.data_path / "preprocessing_artifacts.pkl"
        if artifact_path.exists():
            with open(artifact_path, "rb") as f:
                artifact = pickle.load(f)
            user_cols = artifact["user_columns"]
            time_cols = [
                "is_morning",
                "is_afternoon",
                "is_evening",
                "is_weekend",
                "is_weekday",
            ]
            self.feature_schema = user_cols + time_cols

        # Load Products
        prod_path = self.data_path / "product_features.csv"
        if prod_path.exists():
            df_prod = pd.read_csv(prod_path)
            self.product_ids = df_prod["product_id"].astype(str).tolist()

    def initialize(self):
        """
        Calculates initial LinUCB matrices and seeds Redis.
        Logic:
            A = I + X.T @ X
            b = X.T @ y
        Optimized: Stores A_inv instead of A to speed up inference.
        """
        logger.info("Initializing Bandit State in Redis...")

        num_features = len(self.feature_schema)
        # Regularization lambda=1.0 (Identity Matrix)
        identity_A = np.eye(num_features)
        zeros_b = np.zeros(num_features)

        # Load History
        log_path = self.data_path / "training_log.csv"
        df_log = pd.DataFrame()
        if log_path.exists():
            df_log = pd.read_csv(log_path)
            df_log["product_id"] = df_log["product_id"].astype(str)
            logger.info(f"Loaded {len(df_log)} historical events for seeding.")

        pipeline = self.r.pipeline()
        count = 0

        for pid in self.product_ids:
            A = identity_A.copy()
            b = zeros_b.copy()

            if not df_log.empty:
                product_data = df_log[df_log["product_id"] == pid]
                if not product_data.empty:
                    X = product_data[self.feature_schema].values
                    y = product_data["response"].values

                    # Accumulate A (Covariance)
                    A += X.T @ X
                    b += X.T @ y

            # INVERT A HERE (The Optimization)
            # Since we initialized with Identity, it is guaranteed invertible.
            A_inv = np.linalg.inv(A)

            # Store A_inv instead of A
            model_data = {"A_inv": A_inv.tolist(), "b": b.tolist()}
            pipeline.set(f"linucb:{pid}", json.dumps(model_data))
            count += 1

        pipeline.execute()
        logger.info(f"Seeded {count} models to Redis.")

    def get_models(
        self, product_ids: List[str]
    ) -> Dict[str, Tuple[np.ndarray, np.ndarray]]:
        """
        Batch fetches models for specific product IDs (or all if not provided).
        """
        keys = [f"linucb:{pid}" for pid in product_ids]

        # Redis MGET (1 Network Call)
        json_models = self.r.mget(keys)

        models = {}
        for pid, json_str in zip(product_ids, json_models):
            if json_str:
                data = json.loads(json_str)
                A_inv = np.array(data["A_inv"])
                b = np.array(data["b"])
                models[pid] = (A_inv, b)
            else:
                # Handle missing keys (Cold start fallback)
                d = len(self.feature_schema)
                models[pid] = (np.eye(d), np.zeros(d))

        return models
