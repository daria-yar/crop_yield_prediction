import os
import io
import base64
import logging
from datetime import datetime
from pathlib import Path

import requests
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse


LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "webmaster.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("webmaster")

COLLECTOR_URL = os.getenv("COLLECTOR_URL", "http://collector-service:8001")
ML_URL = os.getenv("ML_URL", "http://ml-service:8002")


def call_collector(endpoint: str, params: dict) -> dict:
    url = f"{COLLECTOR_URL}{endpoint}"
    logger.info(f"Запрос к Collector: {url}, params={params}")
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Ошибка запроса к Collector: {e}")
        raise HTTPException(502, f"Ошибка связи с Collector: {e}")


def call_ml_predict(payload: dict) -> dict:
    url = f"{ML_URL}/predict"
    logger.info(f"Запрос к ML Service: {url}")
    
    try:
        response = requests.post(url, json=payload, timeout=60)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Ошибка запроса к ML Service: {e}")
        raise HTTPException(502, f"Ошибка связи с ML Service: {e}")


def call_ml_regression(payload: dict) -> dict:
    url = f"{ML_URL}/regression"
    logger.info(f"Запрос к ML Service: {url}")
    
    try:
        response = requests.post(url, json=payload, timeout=60)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Ошибка запроса к ML Service: {e}")
        raise HTTPException(502, f"Ошибка связи с ML Service: {e}")


def fig_to_base64(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
    buf.seek(0)
    img_base64 = base64.b64encode(buf.read()).decode('utf-8')
    plt.close(fig)
    return img_base64


app = FastAPI(title="Web Master Service", version="1.0.0")


@app.get("/")
def root():
    logger.info("GET /")
    return {
        "service": "Web Master",
        "status": "running",
        "collector_url": COLLECTOR_URL,
        "ml_url": ML_URL,
        "timestamp": datetime.now().isoformat()
    }


@app.get("/health")
def health():
    logger.info("GET /health")
    
    services_status = {}
    
    try:
        response = requests.get(f"{COLLECTOR_URL}/health", timeout=5)
        services_status["collector"] = response.json().get("status", "unknown")
    except:
        services_status["collector"] = "unavailable"
    
    try:
        response = requests.get(f"{ML_URL}/health", timeout=5)
        services_status["ml_service"] = response.json().get("status", "unknown")
    except:
        services_status["ml_service"] = "unavailable"
    
    return {
        "status": "OK",
        "services": services_status,
        "timestamp": datetime.now().isoformat()
    }


# сценарий 1
@app.get("/scenario1")
def scenario1_timeseries(region: str, district: str, year: int, param: str):
    logger.info(f"GET /scenario1 - {region}, {district}, {year}, {param}")
    
    collector_data = call_collector("/timeseries", {
        "region": region,
        "district": district,
        "year": year,
        "param": param
    })
    
    timeseries = collector_data["timeseries"]
    
    fig, ax = plt.subplots(figsize=(12, 5))
    
    days = np.arange(1, len(timeseries) + 1)
    ax.plot(days, timeseries, linewidth=1.5, color='teal')
    
    ax.set_title(f"{param} - {district}, {year}", fontsize=12)
    ax.set_xlabel("День года")
    ax.set_ylabel(param)
    ax.grid(True, alpha=0.3)
    
    img_base64 = fig_to_base64(fig)
    
    logger.info(f"OK - график построен для {param}")
    
    return {
        "status": "OK",
        "region": region,
        "district": district,
        "year": year,
        "param": param,
        "data_length": len(timeseries),
        "image": img_base64
    }


# сценарий 2
@app.get("/scenario2")
def scenario2_correlation(region: str, district: str):
    logger.info(f"GET /scenario2 - {region}, {district}")
    
    collector_data = call_collector("/correlation", {
        "region": region,
        "district": district
    })
    
    data = collector_data["data"]
    
    years = [item["year"] for item in data]
    ndvi_max = [item["ndvi_max"] for item in data]
    productive = [item["productive"] for item in data]
    
    fig, ax = plt.subplots(figsize=(10, 6))
    scatter = ax.scatter(ndvi_max, productive, c=years, cmap='viridis', s=100, edgecolors='black')
    
    for i, year in enumerate(years):
        ax.annotate(str(year), (ndvi_max[i], productive[i]), 
                    textcoords="offset points", xytext=(5, 5), fontsize=8)
    
    z = np.polyfit(ndvi_max, productive, 1)
    p = np.poly1d(z)
    x_line = np.linspace(min(ndvi_max), max(ndvi_max), 100)
    ax.plot(x_line, p(x_line), "--", color='red', alpha=0.7, label=f'Тренд: y={z[0]:.1f}x+{z[1]:.1f}')
    
    ax.set_title(f"Зависимость урожайности от NDVI max\n{district}", fontsize=12)
    ax.set_xlabel("NDVI max")
    ax.set_ylabel("Урожайность, ц/га")
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    cbar = plt.colorbar(scatter, ax=ax)
    cbar.set_label('Год')
    img_base64 = fig_to_base64(fig)

    correlation = np.corrcoef(ndvi_max, productive)[0, 1]
    
    logger.info(f"OK - график корреляции построен, r={correlation:.3f}")
    
    return {
        "status": "OK",
        "region": region,
        "district": district,
        "count": len(data),
        "correlation": correlation,
        "image": img_base64
    }


# сценарий 3
@app.get("/scenario3")
def scenario3_predict(region: str, district: str, year: int):
    logger.info(f"GET /scenario3 - {region}, {district}, {year}")
    
    collector_data = call_collector("/predict_data", {
        "region": region,
        "district": district,
        "year": year
    })
    
    ml_payload = {
        "region": region,
        "district": district,
        "year": year,
        "data": collector_data["data"],
        "num_of_params": collector_data.get("num_of_params", 24),
        "productive": collector_data["productive"]
    }
    
    ml_result = call_ml_predict(ml_payload)
    
    logger.info(f"OK - предсказание: {ml_result['prediction']:.2f}, реальное: {ml_result['actual']:.2f}")
    
    return {
        "status": "OK",
        "region": region,
        "district": district,
        "year": year,
        "prediction": ml_result["prediction"],
        "actual": ml_result["actual"],
        "error": ml_result["error"],
        "error_percent": ml_result["error_percent"]
    }


# сценарий 4
@app.get("/scenario4")
def scenario4_regression(region: str, district: str, year: int, history: int = 5):
    logger.info(f"GET /scenario4 - {region}, {district}, {year}, history={history}")
    
    collector_data = call_collector("/regression_data", {
        "region": region,
        "district": district,
        "year": year,
        "history": history
    })
    
    ml_payload = {
        "region": region,
        "district": district,
        "target_year": year,
        "data": collector_data["data"]
    }
    
    ml_result = call_ml_regression(ml_payload)
    
    logger.info(f"OK - предсказание: {ml_result['prediction']:.2f}, реальное: {ml_result['actual']:.2f}")
    
    return {
        "status": "OK",
        "region": region,
        "district": district,
        "year": year,
        "history": history,
        "prediction": ml_result["prediction"],
        "actual": ml_result["actual"],
        "error": ml_result["error"],
        "slope": ml_result["slope"],
        "intercept": ml_result["intercept"]
    }