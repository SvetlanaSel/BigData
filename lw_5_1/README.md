# Лабораторная работа 5.1. Анализ данных землетрясений с использованием Hadoop

[![Docker](https://img.shields.io/badge/Docker-Ready-blue)]()
[![Hadoop](https://img.shields.io/badge/Hadoop-3.3.4-green)]()
[![Python](https://img.shields.io/badge/Python-3.8+-blue)]()

## 📋 Постановка задачи

**Цель.** Получить практические навыки развертывания одноузлового кластера Hadoop, освоить базовые операции с распределенной файловой системой HDFS, выполнить загрузку и обработку данных, а также проанализировать и визуализировать результаты.

**Аналитическая задача.** Топ-10 режиссеров по среднему рейтингу (вариант 13)

**Источник данных.** https://www.kaggle.com/datasets/carolzhangdc/imdb-5000-movie-dataset

---

## 🏗 Архитектура системы

```
┌──────────────────────────────────────────────────────────┐
│              Docker Container (Ubuntu 20.04)             │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────┐    │
│  │  NameNode    │    │   DataNode   │    │Resource  │    │
│  │  (HDFS)      │◄──►│   (HDFS)     │    │ Manager  │    │
│  └──────────────┘    └──────────────┘    └──────────┘    │
│         │                                             │  │
│         │                                             │  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────┐    │
│  │ Secondary    │    │   DataNode   │    │   Node   │    │
│  │   NameNode   │    │   (HDFS)     │    │ Manager  │    │
│  └──────────────┘    └──────────────┘    └──────────┘    │
│                                                          │
│  ┌──────────────────────────────────────────────────┐    │
│  │           HDFS File System                       │    │
│  │    /user/hadoop/input/database.csv               │    │
│  └──────────────────────────────────────────────────┘    │
│                                                          │
│  ┌──────────────────────────────────────────────────┐    │
│  │         Python / PySpark Analysis                │    │
│  │    • Pandas для быстрого анализа                 │    │
│  │    • PySpark для больших данных                  │    │
│  │    • Jupyter для визуализации                    │    │
│  └──────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────┘
                │
                │ HTTP
                ▼
        http://localhost:9870 (HDFS UI)
        http://localhost:8088 (YARN UI)
```

---

## 🔧 Технологический стек

| Технология | Версия | Назначение |
|-----------|--------|-----------|
| **Hadoop** | 3.3.4 | Распределенная обработка данных |
| **HDFS** | 3.3.4 | Распределенная файловая система |
| **YARN** | 3.3.4 | Управление ресурсами кластера |
| **Python** | 3.8+ | Язык программирования |
| **Pandas** | 2.0+ | Анализ и обработка данных |
| **PySpark** | 3.4+ | Обработка больших данных |
| **Jupyter** | latest | Интерактивный анализ |
| **Matplotlib** | 3.7+ | Визуализация данных |
| **Docker** | latest | Контейнеризация окружения |
| **Ubuntu** | 20.04 | Базовая ОС контейнера |

---

## 🚀 Запуск проекта

### Шаг 1. Клонирование и подготовка

```bash
# Клонировать репозиторий
git clone <repository_url>
cd lw_5_1

# Убедиться, что файлы на месте
ls -la hadoop/
ls -la scripts/
ls -la notebooks/
```

**Проверка:** Должны увидеть 6 файлов в директории `hadoop/`:
- `workers`
- `core-site.xml`
- `hdfs-site.xml`
- `yarn-site.xml`
- `mapred-site.xml`
- `log4j.properties`

### Шаг 2. Запуск Docker контейнера

```bash
# Запустить контейнер
# docker compose up --build # if new data rewrite database.csv
docker compose up -d

<img width="745" height="166" alt="image" src="https://github.com/user-attachments/assets/d8d560e8-5a29-41a8-8957-4ea99dd22f3e" />

# Просмотреть логи (подождите 60-90 секунд)
docker compose logs -f hadoop

<img width="808" height="118" alt="image" src="https://github.com/user-attachments/assets/abc9ecf9-9e1b-4d02-ba0a-4666455e8c0d" />

```


### Шаг 3. Подключение к контейнеру

```bash
# Открыть терминал внутри контейнера
docker compose exec hadoop bash

# Проверить, что вы внутри контейнера
hostname  # должен показать: hadoop
```
<img width="974" height="84" alt="image" src="https://github.com/user-attachments/assets/48c980f2-c980-4fe8-b2e2-09450b77c5d3" />


### Шаг 4. Проверка компонентов Hadoop

```bash
# Проверить статус всех процессов
jps

# Должны увидеть:
# - NameNode
# - DataNode
# - SecondaryNameNode
# - ResourceManager
# - NodeManager
# - Jps
```
<img width="805" height="240" alt="image" src="https://github.com/user-attachments/assets/fc09b72c-4fe3-4b26-b80c-f4c1e3117726" />


---

## 📁 Работа с HDFS

### Шаг 1. Создание директорий

```bash
# Создать директории для входных и выходных данных
hdfs dfs -mkdir -p /user/hadoop/input
hdfs dfs -mkdir -p /user/hadoop/output

# Проверить созданные директории
hdfs dfs -ls /user/hadoop/
```

**Проверка.** Должны увидеть директории `input` и `output`.

<img width="974" height="171" alt="image" src="https://github.com/user-attachments/assets/8698a4f7-e23a-4ea2-a1bb-fbde48b0be9e" />


### Шаг 2. Загрузка данных

```bash
# Загрузить dataset в HDFS
hdfs dfs -put /opt/data/movie.csv /user/hadoop/input/movie.csv

# Проверить загрузку
hdfs dfs -ls -h /user/hadoop/input/

# Просмотреть размер файла
hdfs dfs -du -h /user/hadoop/input/
```

**Проверка.** Файл `movie.csv` должен быть в HDFS. 

<img width="794" height="249" alt="image" src="https://github.com/user-attachments/assets/a30f526e-401e-40ad-8f37-2d9a2d6276d7" />


### Шаг 3. Просмотр данных в HDFS

```bash
# Просмотреть первые строки файла из HDFS
hdfs dfs -cat /user/hadoop/input/movie.csv | head -20

# Проверить статистику HDFS
hdfs dfsadmin -report
```


### Шаг 4. Веб-интерфейсы (открыть в браузере)

```bash
# Открыть в браузере (на хосте, не в контейнере):
```

- **HDFS NameNode UI:** http://localhost:9870
  - Навигация. Browse the file system → `/user/hadoop/input/` → `movie.csv`
  
- **YARN ResourceManager UI:** http://localhost:8088

**Проверка.** В браузере должны открыться веб-интерфейсы Hadoop. В HDFS UI можно увидеть загруженный файл.

<img width="1280" height="447" alt="image" src="https://github.com/user-attachments/assets/ba05eb54-747a-42eb-b4e9-30e6a6e690ec" />

<img width="1280" height="437" alt="image" src="https://github.com/user-attachments/assets/dca6963a-a891-4c53-bc3d-25a26730b8a8" />

---

## 🔍 Анализ данных

### Вариант 1. Pandas (быстрый анализ)

```bash
cd /opt/scripts

# Запустить анализ
python3 analyze_pandas.py


```

**Проверка результата:** 

<img width="654" height="203" alt="image" src="https://github.com/user-attachments/assets/106ac824-7e99-4412-8244-69ba68d81303" />

<img width="648" height="233" alt="image" src="https://github.com/user-attachments/assets/c076ca44-83c9-4305-a6bd-70d9a9bf749a" />

<img width="653" height="322" alt="image" src="https://github.com/user-attachments/assets/4ba50b51-0313-4873-941f-6d40974b5688" />

<img width="581" height="46" alt="image" src="https://github.com/user-attachments/assets/08b1a14f-1b0e-4b4a-be09-fb82e977bcaa" />



### Вариант 2. PySpark (для больших данных)

```bash
cd /opt/scripts

# Запустить анализ через Spark
python3 analyze_spark.py

```

**Проверка результата.** 

<img width="694" height="338" alt="image" src="https://github.com/user-attachments/assets/bd4aabd6-def4-4e10-81c1-e50b1da3771c" />

<img width="613" height="303" alt="image" src="https://github.com/user-attachments/assets/30f1a67f-cbd8-426c-bdcc-9c1c19704594" />

<img width="634" height="341" alt="image" src="https://github.com/user-attachments/assets/74c3499f-cb43-445e-9dab-e6b603aaf2ed" />

<img width="590" height="274" alt="image" src="https://github.com/user-attachments/assets/791f351b-08ae-4ba6-bf0e-79a875c75b60" />



### Вариант 3. Jupyter Notebook (визуализация)

```bash
# Запустить Jupyter без токена (удобно для разработки)
cd /opt
bash scripts/start_jupyter.sh

# Открыть браузер: http://localhost:8888
# (доступ без токена - открывается сразу)

# Открыть notebook
# Выполнить все ячейки (Run All)
```

**Проверка:** 

<img width="937" height="603" alt="image" src="https://github.com/user-attachments/assets/8b0e01cf-62fe-4c1a-a8f1-1beee62e3b2a" />


**Проект готов к использованию! 🎉**






