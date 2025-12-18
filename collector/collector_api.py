import os
import json
import logging
from datetime import datetime
from pathlib import Path

import numpy as np
import requests
from fastapi import FastAPI, HTTPException


LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "collector.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("collector")

STORAGE_URL = os.getenv("STORAGE_URL", "http://storage-service:8000")
CONFIG_PATH = Path("config.json")

def load_config():
    try:
        if not CONFIG_PATH.exists():
            logger.error(f"Файл конфигурации не найден: {CONFIG_PATH.absolute()}")
            return {}
            
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            logger.info("Конфигурация успешно загружена")
            return data
    except Exception as e:
        logger.error(f"Ошибка загрузки конфигурации: {e}")
        return {}

config = load_config()



def get_list_of_params():
    params = config.get("LIST_OF_PARAMS") or config.get("settings", {}).get("LIST_OF_PARAMS")
    
    if params is None:
        logger.warning("LIST_OF_PARAMS не найден в конфиге!")
        return []
        
    if isinstance(params, dict):
        return list(params.keys())
    return list(params)


def get_list_of_stat_params():
    params = config.get("LIST_OF_STAT_PARAMS", {})

    if params is None:
        logger.warning("LIST_OF_STAT_PARAMS не найден в конфиге!")
        return []

    if isinstance(params, dict):
        return list(params.keys())
    return list(params)


def get_norm_coef_meteo() -> np.ndarray:
    params = config.get("LIST_OF_PARAMS", {})
    list_of_params = get_list_of_params()

    if isinstance(params, dict):
        return np.array([params.get(p, 1) for p in list_of_params])
    return np.ones(len(list_of_params))


def get_norm_coef_stat() -> np.ndarray:
    params = config.get("LIST_OF_STAT_PARAMS", {})
    list_of_params = get_list_of_stat_params()

    if isinstance(params, dict):
        return np.array([params.get(p, 1) for p in list_of_params])
    return np.ones(len(list_of_params))


def get_len_of_param():
    return config.get("LEN_OF_PARAM", 365)


def extract_param(data: list, param_name: str) -> list:
    list_of_params = get_list_of_params()

    if param_name not in list_of_params:
        raise ValueError(f"Неизвестный параметр: {param_name}")
    
    param_index = list_of_params.index(param_name)
    len_of_params = get_len_of_param()
    start = param_index * len_of_params
    end = start + len_of_params
    return data[start:end]


def get_ndvi_max(data: list) -> float:
    ndvi_values = extract_param(data, 'ndvi')
    return max(ndvi_values)


def merge_two_years(data_prev: list, data_curr: list) -> np.ndarray:
    list_of_params = get_list_of_params()
    len_of_param = get_len_of_param()
    num_params = len(list_of_params)
    
    prev_arr = np.array(data_prev).reshape(num_params, len_of_param)
    curr_arr = np.array(data_curr).reshape(num_params, len_of_param)
    merged = np.concatenate([prev_arr, curr_arr], axis=1)
    
    return merged


def add_stat_params(merged: np.ndarray, stat_values: dict) -> np.ndarray:
    two_years_length = merged.shape[1]
    stat_params = get_list_of_stat_params()
    
    stat_rows = []
    for key in stat_params:
        if key in stat_values:
            value = stat_values[key]
            stat_rows.append(np.full(two_years_length, value))
    
    if stat_rows:
        stat_arr = np.array(stat_rows)  # (3, 730)
        result = np.concatenate([merged, stat_arr], axis=0)  # (24, 730)
        return result
    
    return merged


def normalize_and_cut(data: np.ndarray) -> list:
    norm_coef_meteo = get_norm_coef_meteo()
    norm_coef_stat = get_norm_coef_stat()
    norm_coefs = np.concatenate([norm_coef_meteo, norm_coef_stat]).reshape(-1, 1)
    
    normalized = data / norm_coefs
    
    start = config.get("CUT_START", 275)
    end = config.get("CUT_END", 520)
    cut = normalized[:, start:end]
    
    return cut.flatten().tolist()


def call_storage(endpoint: str, params: dict) -> dict:
    url = f"{STORAGE_URL}{endpoint}"
    logger.info(f"Запрос к Storage: {url}, params={params}")
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if data.get("status") != "OK":
            error_msg = data.get("message", "Неизвестная ошибка Storage")
            logger.error(f"Storage вернул ошибку: {error_msg}")
            raise HTTPException(502, f"Ошибка Storage: {error_msg}")
        
        return data
    except requests.RequestException as e:
        logger.error(f"Ошибка запроса к Storage: {e}")
        raise HTTPException(502, f"Ошибка связи с Storage: {e}")



app = FastAPI(title="Collector Service", version="1.0.0")


@app.get("/")
def root():
    logger.info("GET /")
    return {
        "service": "Collector",
        "status": "running",
        "storage_url": STORAGE_URL,
        "timestamp": datetime.now().isoformat()
    }


@app.get("/health")
def health():
    logger.info("GET /health")

    try:
        response = requests.get(f"{STORAGE_URL}/health", timeout=5)
        storage_status = response.json().get("status", "unknown")
    except:
        storage_status = "unavailable"
    
    return {
        "status": "OK",
        "storage_status": storage_status,
        "timestamp": datetime.now().isoformat()
    }


@app.get("/params")
def get_params():
    """Список доступных параметров"""
    logger.info("GET /params")
    return {"params": get_list_of_params()}


# сценарий 1
@app.get("/timeseries")
def get_timeseries(region: str, district: str, year: int, param: str):
    """ Получаем временной ряд одного параметра для графика """
    logger.info(f"GET /timeseries - {region}, {district}, {year}, {param}")
    list_of_params = get_list_of_params()    
    
    if param not in list_of_params:
        raise HTTPException(400, f"Неизвестный параметр: {param}. Доступные: {list_of_params}")
    
    storage_data = call_storage("/meteo/row", {
        "region": region,
        "district": district,
        "year": year
    })
    
    data = storage_data["data"]
    timeseries = extract_param(data, param)
    
    logger.info(f"OK - извлечено {len(timeseries)} значений для {param}")
    
    return {
        "status": "OK",
        "region": region,
        "district": district,
        "year": year,
        "param": param,
        "timeseries": timeseries
    }


# сценарий 2
@app.get("/correlation")
def get_correlation(region: str, district: str):
    """
    Данные для корреляции максимума NDVI и урожайности.
    Возвращает пары (ndvi_max, урожайность) для всех лет для выбранного района
    """
    logger.info(f"GET /correlation - {region}, {district}")
    
    storage_data = call_storage("/meteo/all_years", {
        "region": region,
        "district": district
    })
    
    rows = storage_data["rows"]
    
    result = []
    for row in rows:
        ndvi_max = get_ndvi_max(row["meteo_data"])
        result.append({
            "year": row["year"],
            "ndvi_max": ndvi_max,
            "productive": row["productive"]
        })
    
    logger.info(f"OK - обработано {len(result)} лет")
    
    return {
        "status": "OK",
        "region": region,
        "district": district,
        "count": len(result),
        "data": result
    }


# сценарий 3
@app.get("/predict_data")
def predict_data(region: str, district: str, year: int):
    """
    Данные для прогноза модели: объединяет временные ряды за 2 года, нормализует
    и обрезает до нужного размера
    """
    logger.info(f"GET /predict_data - {region}, {district}, {year}")
    
    storage_data = call_storage("/meteo/with_yield", {
        "region": region,
        "district": district,
        "year": year
    })
    
    meteo_prev = storage_data["meteo_data_prev"]
    meteo_curr = storage_data["meteo_data"]
    productive = storage_data["productive"]
    
    merged = merge_two_years(meteo_prev, meteo_curr)
    logger.info(f"Объеденены 2 года, размер полученного ряда: {merged.shape}")
    
    stat_values = {
        "mean_prod": storage_data["mean_productive"],
        "trend": storage_data["trend"],
        "disp": storage_data["prod_disperssion_norm"]
    }
    with_stats = add_stat_params(merged, stat_values)
    logger.info(f"С stat-параметрами: {with_stats.shape}")
    
    processed = normalize_and_cut(with_stats)
    logger.info(f"После обработки: {len(processed)} значений")

    days_count = config.get("CUT_END", 520) - config.get("CUT_START", 275)
    
    return {
        "status": "OK",
        "region": region,
        "district": district,
        "year": year,
        "data": processed,
        "num_of_params": len(processed) // days_count,
        "productive": productive
    }


# сценарий 4
@app.get("/regression_data")
def get_regression_data(region: str, district: str, year: int, history: int = 5):
    """
    Данные для линейной регрессии.
    Возвращает пары (ndvi_max, урожайность) за указанный год и предыдущие history лет
    """
    logger.info(f"GET /regression_data - {region}, {district}, {year}, history={history}")
    
    storage_data = call_storage("/meteo/multi_year", {
        "region": region,
        "district": district,
        "year": year,
        "history": history
    })
    
    years = storage_data["years"]
    meteo_rows = storage_data["meteo_rows"]
    yields = storage_data["yields"]
    
    result = []
    for i, y in enumerate(years):
        ndvi_max = get_ndvi_max(meteo_rows[i])
        result.append({
            "year": y,
            "ndvi_max": ndvi_max,
            "productive": yields[i]
        })
    
    logger.info(f"OK - обработано {len(result)} лет")
    
    return {
        "status": "OK",
        "region": region,
        "district": district,
        "target_year": year,
        "count": len(result),
        "data": result
    }