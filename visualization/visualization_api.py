import os
import logging
from pathlib import Path
from datetime import datetime

import requests
from flask import Flask, render_template, request, jsonify

# Настройка логирования
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "visualization.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("visualization")

app = Flask(__name__)

WEBMASTER_URL = os.getenv("WEBMASTER_URL", "http://localhost:8003")
logger.info(f"Visualization Service запущен, WEBMASTER_URL={WEBMASTER_URL}")


def call_webmaster(endpoint: str, params: dict) -> dict:
    url = f"{WEBMASTER_URL}{endpoint}"
    logger.info(f"Запрос к WebMaster: {url}, params={params}")
    
    try:
        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()
        result = response.json()
        logger.info(f"Ответ получен: status={result.get('status', 'unknown')}")
        return result
    
    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code if e.response else 500
        logger.error(f"HTTP ошибка {status_code} от WebMaster: {e}")

        if status_code == 502:
            return {"status": "error", "message": "502 Bad Gateway - данные не найдены"}
        elif status_code == 404:
            return {"status": "error", "message": "Данные не найдены для выбранных параметров"}
        elif status_code == 500:
            return {"status": "error", "message": "500 Internal Server Error"}
        else:
            return {"status": "error", "message": f"HTTP ошибка: {status_code}"}
        
    except requests.exceptions.Timeout:
        logger.error("Таймаут запроса к WebMaster")
        return {"status": "error", "message": "Timeout - превышено время ожидания"}
    except requests.exceptions.ConnectionError:
        logger.error("Ошибка соединения с WebMaster")
        return {"status": "error", "message": "Ошибка соединения с сервером"}
    except requests.RequestException as e:
        logger.error(f"Ошибка запроса: {e}")
        return {"status": "error", "message": str(e)}


@app.route("/")
def index():
    logger.info("GET /")
    return render_template("index.html")


@app.route("/scenario1")
def scenario1_page():
    logger.info("GET /scenario1")
    return render_template("scenario1.html")


@app.route("/scenario2")
def scenario2_page():
    logger.info("GET /scenario2")
    return render_template("scenario2.html")


@app.route("/scenario3")
def scenario3_page():
    logger.info("GET /scenario3")
    return render_template("scenario3.html")


@app.route("/scenario4")
def scenario4_page():
    logger.info("GET /scenario4")
    return render_template("scenario4.html")



@app.route("/api/scenario1", methods=["POST"])
def api_scenario1():
    """API для сценария 1"""
    data = request.json
    logger.info(f"POST /api/scenario1 - {data}")
    result = call_webmaster("/scenario1", {
        "region": data.get("region"),
        "district": data.get("district"),
        "year": data.get("year"),
        "param": data.get("param")
    })
    return jsonify(result)


@app.route("/api/scenario2", methods=["POST"])
def api_scenario2():
    """API для сценария 2"""
    data = request.json
    logger.info(f"POST /api/scenario2 - {data}")
    result = call_webmaster("/scenario2", {
        "region": data.get("region"),
        "district": data.get("district")
    })
    return jsonify(result)


@app.route("/api/scenario3", methods=["POST"])
def api_scenario3():
    """API для сценария 3"""
    data = request.json
    logger.info(f"POST /api/scenario3 - {data}")
    result = call_webmaster("/scenario3", {
        "region": data.get("region"),
        "district": data.get("district"),
        "year": data.get("year")
    })
    return jsonify(result)


@app.route("/api/scenario4", methods=["POST"])
def api_scenario4():
    """API для сценария 4"""
    data = request.json
    logger.info(f"POST /api/scenario4 - {data}")
    result = call_webmaster("/scenario4", {
        "region": data.get("region"),
        "district": data.get("district"),
        "year": data.get("year"),
        "history": data.get("history", 5)
    })
    return jsonify(result)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)