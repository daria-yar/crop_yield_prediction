import requests
import matplotlib.pyplot as plt
import numpy as np


list_of_params = [
    'ndvi',
    'ndvi_historical',
    'mean_temp',
    'mean_temp_historical',
    'mean_temp_acc',
    'mean_temp_acc_historical',
    'mean_prec',
    'mean_prec_historical',
    'mean_prec_acc',
    'mean_prec_acc_historical',
    'mean_rh',
    'mean_rh_historical',
    'mean_p',
    'mean_snod',
    'mean_snod_historical',
    'mean_snowc',
    'mean_snowc_historical',
    'mean_sdswr',
    'mean_sdlwr',
    'mean_tmpgr10',
    'mean_soilw10',
    'mean_prod',
    'trend',
    'disp'
]


def draw_param_plot():
    url = "http://localhost:8001/timeseries"
    params = {
        "region": "Пензенская область",
        "district": "Белинский район",
        "year": 2020,
        "param": "mean_temp"
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        series = data["timeseries"]
        
        plt.figure(figsize=(12, 6))
        plt.plot(series, color='#2ecc71', linewidth=2, label=f'Параметр: {params["param"]}')
        plt.title(f"Профиль озимой пшеницы: {params['district']} ({params['year']})", fontsize=14)
        plt.xlabel("День года", fontsize=12)
        plt.ylabel("Значение параметра", fontsize=12)
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.legend()

        filename = f"{params['district']}_{params['year']}_{params['param']}.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        
    except Exception as e:
        print(e)


def draw_plots_for_model():
    url = "http://localhost:8001/predict_data"
    params = {
        "region": "Пензенская область",
        "district": "Белинский район",
        "year": 2020,
    }
    
    CUT_START = 275
    CUT_END = 520
    points_per_param = CUT_END - CUT_START
    
    norm_coefs = {
        'ndvi': 1, 'ndvi_historical': 1,
        'mean_temp': 40, 'mean_temp_historical': 40,
        'mean_temp_acc': 5000, 'mean_temp_acc_historical': 5000,
        'mean_prec': 10, 'mean_prec_historical': 10,
        'mean_prec_acc': 1000, 'mean_prec_acc_historical': 1000,
        'mean_rh': 100, 'mean_rh_historical': 100,
        'mean_p': 1000,
        'mean_snod': 1, 'mean_snod_historical': 1,
        'mean_snowc': 100, 'mean_snowc_historical': 100,
        'mean_sdswr': 400, 'mean_sdlwr': 400,
        'mean_tmpgr10': 50, 'mean_soilw10': 50,
        'mean_prod': 80, 'trend': 20, 'disp': 6
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        json_data = response.json()
        
        full_series = np.array(json_data["data"])
        num_params = len(full_series) // points_per_param        
        fig, axes = plt.subplots(num_params, 1, figsize=(12, 4 * num_params))
        if num_params == 1:
            axes = [axes]
        
        for i in range(num_params):
            ax = axes[i]
            
            start = i * points_per_param
            end = start + points_per_param
            param_data = full_series[start:end]
            
            p_name = list_of_params[i] if i < len(list_of_params) else f"Param_{i}"
            coef = norm_coefs.get(p_name, 1)
            param_data_denorm = param_data * coef
            
            ax.plot(param_data_denorm, linewidth=1.5, color='teal')
            ax.set_title(f"{p_name} (×{coef})", fontsize=10, loc='left')
            ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        filename = f"All_params_{params['district']}_{params['year']}.png"
        plt.savefig(filename, dpi=150, bbox_inches='tight')
        
    except Exception as e:
        print(f"Ошибка: {e}")
        if 'response' in locals() and response.status_code != 200:
            print(f"Ответ сервера: {response.text}")

if __name__ == "__main__":
    draw_plots_for_model()