import os
import logging
from datetime import datetime
from pathlib import Path

import numpy as np
import torch
from fastapi import FastAPI, HTTPException, Request
from sklearn.linear_model import LinearRegression


LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "ml_service.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ml_service")

MODEL_PATH = Path(os.getenv("MODEL_PATH", "models/Winter_Wheat.pt"))
model = None

def load_model():
    global model
    try:
        if MODEL_PATH.exists():
            model = torch.jit.load(MODEL_PATH, map_location=torch.device('cpu'))
            model.eval()
            logger.info(f"Модель загружена: {MODEL_PATH}")
        else:
            logger.warning(f"Файл модели не найден: {MODEL_PATH}")
    except Exception as e:
        logger.error(f"Ошибка загрузки модели: {e}")

load_model()


def predict_with_model(data: list, num_params: int = 24) -> float:
    global model
    
    if model is None:
        raise ValueError("Модель не загружена")
    
    arr = np.array(data, dtype=np.float32)
    seq_length = len(data) // num_params
    
    input_tensor = torch.tensor(arr).reshape(1, num_params, seq_length)
    
    with torch.no_grad():
        prediction = model(input_tensor)
    
    return float(prediction.item())


def train_linear_regression(X: list, y: list) -> LinearRegression:
    X_array = np.array(X).reshape(-1, 1)
    y_array = np.array(y)
    
    lr = LinearRegression()
    lr.fit(X_array, y_array)
    
    return lr


app = FastAPI(title="ML Service", version="1.0.0")


@app.get("/")
def root():
    logger.info("GET /")
    return {
        "service": "ML Service",
        "status": "running",
        "model_loaded": model is not None,
        "model_path": str(MODEL_PATH),
        "timestamp": datetime.now().isoformat()
    }


@app.get("/health")
def health():
    logger.info("GET /health")
    return {
        "status": "OK",
        "model_loaded": model is not None,
        "timestamp": datetime.now().isoformat()
    }


@app.get("/model/info")
def model_info():
    """Информация о модели"""
    logger.info("GET /model/info")
    return {
        "model_path": str(MODEL_PATH),
        "model_loaded": model is not None
    }


@app.post("/model/reload")
def reload_model():
    """Перезагрузить модель"""
    logger.info("POST /model/reload")
    load_model()
    return {
        "status": "OK",
        "model_loaded": model is not None
    }


# сценарий 3
@app.post("/predict")
async def predict(request: Request):
    """ Прогноз урожайности с помощью сверточной сети. """
    body = await request.json()
    
    region = body.get("region")
    district = body.get("district")
    year = body.get("year")
    data = body.get("data")
    num_of_params = body.get("num_of_params")
    productive = body.get("productive")
    
    logger.info(f"POST /predict - {region}, {district}, {year}")
    logger.info(f"data type: {type(data)}, data length: {len(data) if data else 'None'}")
    
    if model is None:
        logger.error("Модель не загружена")
        raise HTTPException(503, "Модель не загружена")
    
    if data is None:
        logger.error("data is None!")
        raise HTTPException(400, "data is required")
    
    try:
        prediction = predict_with_model(data, num_of_params)
        
        error = abs(prediction - productive)
        error_percent = (error / productive) * 100 if productive != 0 else 0
        
        logger.info(f"OK - pred: {prediction:.2f}, actual: {productive:.2f}, error: {error_percent:.2f}%")
        
        return {
            "status": "OK",
            "region": region,
            "district": district,
            "year": year,
            "prediction": prediction,
            "actual": productive,
            "error": error,
            "error_percent": error_percent
        }
    except Exception as e:
        logger.error(f"Ошибка предсказания: {e}")
        raise HTTPException(500, str(e))


# сценарий 4
@app.post("/regression")
async def regression(request: Request):
    """ Прогноз урожайности с помощью линейной регрессии. """
    body = await request.json()
    
    region = body.get("region")
    district = body.get("district")
    target_year = body.get("target_year")
    data = body.get("data")
    
    logger.info(f"POST /regression - {region}, {district}, {target_year}")
    
    if len(data) < 2:
        raise HTTPException(400, "Нужно минимум 2 точки для обучения")
    
    try:
        train_data = data[:-1]
        test_point = data[-1]
        
        X_train = [item["ndvi_max"] for item in train_data]
        y_train = [item["productive"] for item in train_data]
        
        lr = train_linear_regression(X_train, y_train)
        
        prediction = float(lr.predict([[test_point["ndvi_max"]]])[0])
        
        error = abs(prediction - test_point["productive"])
        error_percent = (error / test_point["productive"]) * 100 if test_point["productive"] != 0 else 0
        
        logger.info(f"OK - pred: {prediction:.2f}, actual: {test_point['productive']:.2f}, error: {error_percent:.2f}%")
        
        return {
            "status": "OK",
            "region": region,
            "district": district,
            "year": target_year,
            "prediction": prediction,
            "actual": test_point["productive"],
            "error": error,
            "error_percent": error_percent,
            "slope": float(lr.coef_[0]),
            "intercept": float(lr.intercept_)
        }
    except Exception as e:
        logger.error(f"Ошибка регрессии: {e}")
        raise HTTPException(500, str(e))