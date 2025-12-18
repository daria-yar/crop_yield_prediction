import json
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, HTTPException

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "storage.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("storage")


DATA_DIR = Path("source")
CONFIG_PATH = Path("config.json")

def load_config():
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            logger.info("Конфигурация загружена")
            return json.load(f)
    except Exception as e:
        logger.error(f"Ошибка загрузки конфигурации: {e}")
        return {}

config = load_config()


def get_file_prefix(region: str):
    return config.get("region_mapping", {}).get(region)

def get_district_id(region: str, district: str):
    region_data = config.get(region)
    if region_data is None:
        logger.debug(f"Область не найдена в конфиге: '{region}'")
        logger.debug(f"Доступные области: {list(config.keys())}")
        return None
    
    district_id = region_data.get(district)
    if district_id is None:
        logger.debug(f"Район не найден: '{district}'")
        logger.debug(f"Доступные районы: {list(region_data.keys())}")
        return None
    
    return district_id

def load_scalar(region: str):
    prefix = get_file_prefix(region)
    if not prefix:
        raise ValueError(f"Неизвестная область: {region}")
    filepath = DATA_DIR / f"{prefix}_scalar.csv"
    logger.debug(f"Загрузка scalar: {filepath}")
    return pd.read_csv(filepath, header=0)

def load_meteo(region: str):
    prefix = get_file_prefix(region)
    if not prefix:
        raise ValueError(f"Неизвестная область: {region}")
    filepath = DATA_DIR / f"{prefix}.csv"
    logger.debug(f"Загрузка meteo: {filepath}")
    return pd.read_csv(filepath, header=0)

def find_row_index(scalar_df, district_id: int, year: int):
    mask = (scalar_df["id_dist"].astype(int) == int(district_id)) & (scalar_df["year"].astype(int) == int(year))
    indices = scalar_df.index[mask].tolist()
    logger.info(f"Поиск district_id={district_id}, year={year}, найдено индексов: {len(indices)}")
    return indices[0] if indices else None

def find_district_rows(scalar_df, district_id: int):
    mask = scalar_df["id_dist"].astype(int) == int(district_id)
    indices = scalar_df.index[mask].tolist()
    logger.info(f"Поиск всех строк district_id={district_id}, найдено: {len(indices)}")
    return indices


app = FastAPI(title="Storage Service", version="1.0.0")

@app.get("/")
def root():
    logger.info("GET / - корневой запрос")
    return {
        "service": "Storage",
        "status": "running",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/health")
def health():
    logger.info("GET /health")
    return {"status": "OK", "timestamp": datetime.now().isoformat()}

@app.get("/districts")
def get_districts():
    logger.info("GET /districts")
    return {region: list(districts.keys()) for region, districts in config.items()}

@app.get("/years")
def get_years(region: str, district: str):
    logger.info(f"GET /years - {region}, {district}")
    
    district_id = get_district_id(region, district)
    if not district_id:
        logger.warning(f"Район не найден: {district}")
        raise HTTPException(404, f"Район '{district}' не найден")
    
    try:
        scalar_df = load_scalar(region)
        indices = find_district_rows(scalar_df, district_id)
        years = sorted(scalar_df.loc[indices, "year"].tolist())
        logger.info(f"Найдено {len(years)} лет для {district}")
        return {"region": region, "district": district, "years": years}
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        raise HTTPException(500, str(e))



# сценарий 1
@app.get("/meteo/row")
def get_meteo_row(region: str, district: str, year: int):
    """ Возвращает строку метеоданных для графика (район и год) """
    logger.info(f"GET /meteo/row - {region}, {district}, {year}")
    
    district_id = get_district_id(region, district)
    if not district_id:
        logger.warning(f"Район не найден: {district}")
        raise HTTPException(404, f"Район '{district}' не найден")
    
    try:
        scalar_df = load_scalar(region)
        meteo_df = load_meteo(region)
        
        row_idx = find_row_index(scalar_df, district_id, year)
        if row_idx is None:
            logger.warning(f"Данные не найдены: {district}, {year}")
            raise HTTPException(404, f"Данные не найдены для {district}, {year}")
        
        data = meteo_df.iloc[row_idx].tolist()
        logger.info(f"OK - возвращена строка {row_idx}, длина {len(data)}")
        
        return {
            "status": "OK",
            "region": region,
            "district": district,
            "year": year,
            "row_index": row_idx,
            "data": data
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        raise HTTPException(500, str(e))


# сценарий 2
@app.get("/meteo/all_years")
def get_all_years(region: str, district: str):
    """ Возвращает все строки района для корреляции максимум NDVI-урожайность """
    logger.info(f"GET /meteo/all_years - {region}, {district}")
    
    district_id = get_district_id(region, district)
    if not district_id:
        logger.warning(f"Район не найден: {district}")
        raise HTTPException(404, f"Район '{district}' не найден")
    
    try:
        scalar_df = load_scalar(region)
        meteo_df = load_meteo(region)
        
        indices = find_district_rows(scalar_df, district_id)
        if not indices:
            raise HTTPException(404, f"Данные не найдены для {district}")
        
        rows = []
        for idx in indices:
            rows.append({
                "year": int(scalar_df.loc[idx, "year"]),
                "productive": float(scalar_df.loc[idx, "productive"]),
                "meteo_data": meteo_df.iloc[idx].tolist()
            })
        
        rows = sorted(rows, key=lambda x: x["year"])
        logger.info(f"OK - возвращено {len(rows)} строк")
        
        return {
            "status": "OK",
            "region": region,
            "district": district,
            "count": len(rows),
            "rows": rows
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        raise HTTPException(500, str(e))


# сценарий 3
@app.get("/meteo/with_yield")
def get_meteo_with_yield(region: str, district: str, year: int):
    """ Возвращает метеоданные за 2 года и урожайность для прогноза модели """
    logger.info(f"GET /meteo/with_yield - {region}, {district}, {year}")
    
    district_id = get_district_id(region, district)
    if not district_id:
        logger.warning(f"Район не найден: {district}")
        raise HTTPException(404, f"Район '{district}' не найден")
    
    try:
        scalar_df = load_scalar(region)
        meteo_df = load_meteo(region)
        
        row_idx = find_row_index(scalar_df, district_id, year)
        if row_idx is None:
            logger.warning(f"Данные не найдены: {district}, {year}")
            raise HTTPException(404, f"Данные не найдены для {district}, {year}")
        
        scalar_row = scalar_df.iloc[row_idx]
        meteo_data = meteo_df.iloc[row_idx].tolist()

        row_idx_prev = find_row_index(scalar_df, district_id, year - 1)
        if row_idx_prev is None:
            logger.warning(f"Данные не найдены: {district}, {year - 1}")
            raise HTTPException(404, f"Данные не найдены для {district}, {year - 1}")
        
        meteo_data_prev = meteo_df.iloc[row_idx_prev].tolist()
        
        logger.info(f"OK - строка {row_idx}, урожайность {scalar_row['productive']}")
        
        return {
            "status": "OK",
            "region": region,
            "district": district,
            "year": year,
            "row_index": row_idx,
            "meteo_data": meteo_data,
            "meteo_data_prev": meteo_data_prev,
            "productive": float(scalar_row["productive"]),
            "mean_productive": float(scalar_row["mean_productive"]),
            "trend": float(scalar_row["trend"]),
            "prod_disperssion_norm": float(scalar_row["prod_disperssion_norm"])
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        raise HTTPException(500, str(e))


# сценарий 4
@app.get("/meteo/multi_year")
def get_multi_year(region: str, district: str, year: int, history: int = 5):
    """ Возвращает данные за year и history предыдущих лет """
    logger.info(f"GET /meteo/multi_year - {region}, {district}, {year}, history={history}")
    
    district_id = get_district_id(region, district)
    if not district_id:
        logger.warning(f"Район не найден: {district}")
        raise HTTPException(404, f"Район '{district}' не найден")
    
    try:
        scalar_df = load_scalar(region)
        meteo_df = load_meteo(region)
        required_years = list(range(year - history, year + 1))
        
        meteo_rows = []
        yields = []
        found_years = []
        
        for y in required_years:
            row_idx = find_row_index(scalar_df, district_id, y)
            if row_idx is None:
                logger.warning(f"Нет данных за {y}")
                raise HTTPException(
                    404,
                    f"Недостаточно данных: нет года {y}. Нужны: {required_years}"
                )
            
            meteo_rows.append(meteo_df.iloc[row_idx].tolist())
            yields.append(float(scalar_df.loc[row_idx, "productive"]))
            found_years.append(y)
        
        logger.info(f"OK - возвращено {len(found_years)} лет")
        
        return {
            "status": "OK",
            "region": region,
            "district": district,
            "target_year": year,
            "years": found_years,
            "meteo_rows": meteo_rows,
            "yields": yields
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        raise HTTPException(500, str(e))