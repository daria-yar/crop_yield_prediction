// Районы по регионам
const districtsByRegion = {
    "Пензенская область": [
        "Башмаковский район",
        "Бековский район",
        "Белинский район",
        "Бессоновский район",
        "Вадинский район",
        "Городищенский район",
        "Земетчинский район",
        "Иссинский район",
        "Каменский район",
        "Камешкирский район",
        "Колышлейский район",
        "Кузнецкий район",
        "Лопатинский район",
        "Лунинский район",
        "Малосердобинский район",
        "Мокшанский район",
        "Наровчатский район",
        "Неверкинский район",
        "Нижнеломовский район",
        "Никольский район",
        "Пачелмский район",
        "Пензенский район",
        "Сердобский район",
        "Сосновоборский район",
        "Спасский район",
        "Тамалинский район",
        "Шемышейский район"
    ],
    "Тамбовская область": [
        "Гавриловский район",
        "Жердевский район",
        "Мордовский район",
        "Моршанский район",
        "Мучкапский район",
        "Никифоровский район",
        "Рассказовский район",
        "Ржаксинский район",
        "Сампурский район",
        "Токаревский район",
        "Уваровский район",
        "Уметский район"
    ],
    "Тульская область": [
        "Алексинский район",
        "Богородицкий район",
        "Дубенский район",
        "Ефремовский район",
        "Плавский район",
        "Суворовский район",
        "Узловский район",
        "Щекинский район",
        "Ясногорский район"
    ]
};

// Функция обновления списка районов
function updateDistricts(regionSelectId, districtSelectId) {
    const regionSelect = document.getElementById(regionSelectId);
    const districtSelect = document.getElementById(districtSelectId);
    
    regionSelect.addEventListener('change', function() {
        const region = this.value;
        
        // Очищаем список районов
        districtSelect.innerHTML = '';
        
        if (region && districtsByRegion[region]) {
            districtSelect.disabled = false;
            
            const defaultOption = document.createElement('option');
            defaultOption.value = '';
            defaultOption.textContent = 'Выберите район';
            districtSelect.appendChild(defaultOption);
            
            districtsByRegion[region].forEach(district => {
                const option = document.createElement('option');
                option.value = district;
                option.textContent = district;
                districtSelect.appendChild(option);
            });
        } else {
            districtSelect.disabled = true;
            const defaultOption = document.createElement('option');
            defaultOption.value = '';
            defaultOption.textContent = 'Сначала выберите регион';
            districtSelect.appendChild(defaultOption);
        }
    });
}