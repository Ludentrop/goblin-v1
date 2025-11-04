#!/bin/bash

# Определяем корневую директорию проекта (на уровень выше scripts/)
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Переходим в корень проекта — для стабильности и относительных импортов в Python
cd "$PROJECT_ROOT" || exit 1

# Активируем виртуальное окружение
source venv/bin/activate

# Запускаем скрипт с полным путём (или через модуль, если нужно)
python3 src/data_inputs/fetch_hist_data.py BBG004730N88 5 1d
