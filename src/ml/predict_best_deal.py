import os
import joblib
import pandas as pd


MODEL_PATH = "src/ml/best_deal_model.pkl"


def model_exists():
    return os.path.exists(MODEL_PATH)
