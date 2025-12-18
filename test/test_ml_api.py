import requests
import numpy as np

def test_ml_service():
    collector_url = "http://localhost:8001/predict_data"
    collector_params = {
        "region": "Пензенская область",
        "district": "Белинский район",
        "year": 2020,
    }
    
    response = requests.get(collector_url, params=collector_params)
    response.raise_for_status()
    collector_data = response.json()
    
    print(f"Получено от Collector: {collector_data['data_length']} значений")
    print(f"Реальная урожайность: {collector_data['productive']}")
    
    ml_url = "http://localhost:8002/predict"
    ml_payload = {
        "region": collector_params["region"],
        "district": collector_params["district"],
        "year": collector_params["year"],
        "data": collector_data["data"],
        "productive": collector_data["productive"]
    }
    
    response = requests.post(ml_url, json=ml_payload)
    response.raise_for_status()
    result = response.json()
    
    print(f"Предсказание: {result['prediction']:.2f}")
    print(f"Реальное: {result['actual']:.2f}")
    print(f"Ошибка: {result['error']:.2f}")
    
    return result


def test_regression():
    collector_url = "http://localhost:8001/regression_data"
    collector_params = {
        "region": "Пензенская область",
        "district": "Белинский район",
        "year": 2020,
        "history": 5
    }
    
    response = requests.get(collector_url, params=collector_params)
    response.raise_for_status()
    collector_data = response.json()
    
    print(f"Получено {collector_data['count']} точек для регрессии:")
    for item in collector_data["data"]:
        print(f"  {item['year']}: NDVI_max={item['ndvi_max']:.3f}, yield={item['productive']:.2f}")
    
    ml_url = "http://localhost:8002/regression"
    ml_payload = {
        "region": collector_params["region"],
        "district": collector_params["district"],
        "target_year": collector_params["year"],
        "data": collector_data["data"]
    }
    
    response = requests.post(ml_url, json=ml_payload)
    response.raise_for_status()
    result = response.json()
    
    print(f"Предсказание: {result['prediction']:.2f}")
    print(f"Реальное: {result['actual']:.2f}")
    print(f"Ошибка: {result['error']:.2f}")
    
    return result


if __name__ == "__main__":
    test_ml_service()
    print("\n\n")
    test_regression()