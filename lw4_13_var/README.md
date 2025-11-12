# Лабораторная работа 4-1. Сравнение подходов хранения больших данных
## Цель работы
Cравнить производительность и эффективность различных подходов к хранению и обработке больших данных на примере реляционной базы данных PostgreSQL и документо-ориентированной базы данных MongoDB.
## Индивидуальные задания
## Вариант 13
### Для PostgreSQL
Группировка данных.Создать таблицу webevents (user_id, event_type, timestamp). Сгруппировать данные для подсчета количества событий каждого типа для каждого пользователя.
### Для MongoDB
Группировка данных. Создать коллекцию webevents. Написать агрегационный запрос с $group для подсчета количества событий каждого типа для каждого пользователя.
### Анализ в Jupyter Notebook
Сравнить производительность операций группировки (GROUP BY vs $group). Сделать вывод о применимости для построения аналитических отчетов.
## Подготовка окружения
Загрузка файла docker-compose.yml:
<img width="1619" height="966" alt="image" src="https://github.com/user-attachments/assets/35f9b270-b494-4584-8839-2a55ef6c196e" />
Запуск всех сервисов:
<img width="1547" height="269" alt="image" src="https://github.com/user-attachments/assets/26d486cf-7459-4a39-88a0-b5e047523d48" />
## Решение индивидуальных заданий
### 1. Установка и импорт необходимых библиотек
#### Установка библиотек:
``` 
!pip install pandas numpy pymongo psycopg2-binary sqlalchemy matplotlib seaborn
```
Результат выполнения
<img width="897" height="429" alt="image" src="https://github.com/user-attachments/assets/92c097c6-c0c6-4dc0-9bab-80141687d9df" />
### Импорт библиотек:
```
import pandas as pd
import numpy as np
from pymongo import MongoClient
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
import time
import warnings
warnings.filterwarnings('ignore')

# Настройка для отображения графиков
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")
```
### 2. Создание функций для проверки подключения к базам данных
```
def check_mongo_connection(client):
    """Проверка подключения к MongoDB"""
    try:
        client.server_info()
        print("✅ Успешное подключение к MongoDB")
        return True
    except Exception as e:
        print(f"❌ Ошибка подключения к MongoDB: {e}")
        return False

def check_postgres_connection(conn_params):
    """Проверка подключения к PostgreSQL"""
    try:
        conn = psycopg2.connect(**conn_params)
        print("✅ Успешное подключение к PostgreSQL")
        return conn
    except Exception as e:
        print(f"❌ Ошибка подключения к PostgreSQL: {e}")
        return None

def measure_time(func, *args, **kwargs):
    """Измерение времени выполнения функции"""
    start_time = time.time()
    result = func(*args, **kwargs)
    end_time = time.time()
    return result, end_time - start_time
```
### 3. Генерация данных
```
np.random.seed(42)

# Параметры данных
n_events = 11000
n_users = 10000
event_types = ['view', 'click', 'purchase', 'add_to_cart', 'login']

print(f"Генерация веб-событий:")
print(f"- События: {n_events:,}")
print(f"- Пользователи: {n_users:,}")
print(f"- Типы событий: {event_types}")

# Генерация данных событий
events_data = []
base_time = datetime.now() - timedelta(days=30)

for i in range(n_events):
    user_id = np.random.randint(0, n_users)
    event_type = np.random.choice(event_types, p=[0.5, 0.25, 0.1, 0.1, 0.05])
    time_offset = np.random.uniform(0, 30*24*3600)  # Случайное время в пределах 30 дней
    timestamp = base_time + timedelta(seconds=time_offset)
    
    events_data.append({
        'user_id': user_id,
        'event_type': event_type,
        'timestamp': timestamp
    })

# Создание DataFrame
webevents_df = pd.DataFrame(events_data)

# Сортировка по времени
webevents_df = webevents_df.sort_values('timestamp').reset_index(drop=True)

print(f"\nСоздан DataFrame webevents_df: {len(webevents_df):,} записей")

print("\nПример данных веб-событий:")
print(webevents_df.head(10))

# Базовая статистика
print("\nСтатистика данных:")
print("Распределение событий по типам:")
print(webevents_df['event_type'].value_counts())
print(f"\nПериод данных: от {webevents_df['timestamp'].min()} до {webevents_df['timestamp'].max()}")
```
Результат выполнения

<img width="636" height="403" alt="image" src="https://github.com/user-attachments/assets/9d04e066-eb16-44d9-a291-0ee35a66da9a" />

### 4. Сохранение данных в CSV файлы для дальнейшего использования
```
webevents_df.to_csv('webevents.csv', index=False)

print("✅ Данные сохранены в CSV файлы:")
print("- webevents.csv")
```
Результат выполнения

<img width="381" height="70" alt="image" src="https://github.com/user-attachments/assets/42deab8e-8388-49b5-a4df-8eec8cd4bcd1" />

### 5. Анализ распределения данных
```
print(f"\n Анализ данных:")
print(f"- Всего событий: {len(webevents_df):,}")
print(f"- Уникальных пользователей: {webevents_df['user_id'].nunique():,}")
print(f"- Среднее количество событий на пользователя: {len(webevents_df) / webevents_df['user_id'].nunique():.1f}")

# Распределение событий по типам
event_distribution = webevents_df['event_type'].value_counts()
print(f"\n Распределение событий по типам:")
for event_type, count in event_distribution.items():
    percentage = (count / len(webevents_df)) * 100
    print(f"  {event_type}: {count} событий ({percentage:.1f}%)")

# Топ-10 самых активных пользователей
active_users = webevents_df['user_id'].value_counts().head(10)
print(f"\n Топ-10 самых активных пользователей:")
for user_id, count in active_users.items():
    print(f"  Пользователь {user_id}: {count} событий")

# Анализ по дням (если нужно показать активность по времени)
webevents_df['date'] = webevents_df['timestamp'].dt.date
daily_activity = webevents_df['date'].value_counts().sort_index()
print(f"\n Активность по дням (первые 5 дней):")
for date, count in list(daily_activity.items())[:5]:
    print(f"  {date}: {count} событий")
```
Результат выполнения

<img width="541" height="523" alt="image" src="https://github.com/user-attachments/assets/cde3cc0c-7002-45ff-8d11-fbfc365313e1" />

### 6. Подключение к MongoDB
```
try:
    # Попробуем подключиться к MongoDB через имя сервиса (для Docker)
    mongo_client = MongoClient('mongodb://mongouser:mongopass@mongodb:27017/')
    if check_mongo_connection(mongo_client):
        print("✅ Подключение через Docker сервис 'mongodb'")
    else:
        raise Exception("Не удалось подключиться через Docker сервис")
except:
    try:
        # Если не работает через Docker, попробуем localhost
        mongo_client = MongoClient('mongodb://mongouser:mongopass@localhost:27017/')
        if check_mongo_connection(mongo_client):
            print("✅ Подключение через localhost")
        else:
            raise Exception("Не удалось подключиться через localhost")
    except:
        print("❌ Не удалось подключиться к MongoDB")
        print("Проверьте, что MongoDB запущен: docker compose ps")
        mongo_client = None

if mongo_client:
    mongo_db = mongo_client['studmongo']
    
    # Очистка существующих коллекций
    mongo_db.webevents.drop()
    
    # Загрузка данных в MongoDB
    print("📥 Загрузка данных в MongoDB...")
    
    # Преобразуем столбец 'date' в datetime, если он существует
    webevents_df_for_mongo = webevents_df.copy()
    if 'date' in webevents_df_for_mongo.columns:
        webevents_df_for_mongo['date'] = pd.to_datetime(webevents_df_for_mongo['date'])
        print("✅ Преобразован столбец 'date' в datetime для совместимости с MongoDB")
    
    # Загрузка веб-событий
    webevents_collection = mongo_db['webevents']
    webevents_records = webevents_df_for_mongo.to_dict('records')
    webevents_collection.insert_many(webevents_records)
    print(f"✅ Загружено {len(webevents_records):,} веб-событий")
    
    # Создание индексов для оптимизации
    webevents_collection.create_index("user_id")
    webevents_collection.create_index("event_type")
    webevents_collection.create_index("timestamp")
    webevents_collection.create_index("date")
    webevents_collection.create_index([("user_id", 1), ("timestamp", -1)])
    print("✅ Созданы индексы для оптимизации запросов")
    
    # Базовая статистика по коллекции
    print(f"\n📊 Статистика коллекции webevents:")
    print(f"- Всего документов: {webevents_collection.count_documents({}):,}")
    print(f"- Уникальных пользователей: {len(webevents_collection.distinct('user_id')):,}")
    
else:
    print("❌ Пропуск операций с MongoDB из-за ошибки подключения")
```
Результат выполнения

<img width="660" height="201" alt="image" src="https://github.com/user-attachments/assets/f0c2f07a-f9c3-4cdb-b097-7d2db5fd44d3" />

### 7. Подключение к PostgreSQL
```
pg_conn_params = {
    "dbname": "studpg",
    "user": "postgres",
    "password": "changeme",
    "host": "postgresql",  # Имя сервиса в docker-compose
    "port": "5432"
}

pg_conn = check_postgres_connection(pg_conn_params)
if pg_conn:
    try:
        # Создание таблиц
        with pg_conn.cursor() as cur:
            # Удаление существующих таблиц (если есть другие таблицы, оставляем только webevents)
            cur.execute("DROP TABLE IF EXISTS webevents CASCADE")
            
            # Создание таблицы веб-событий
            cur.execute("""
                CREATE TABLE webevents (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    event_type VARCHAR(20) NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    date DATE
                )
            """)
            
            # Создание индексов для оптимизации
            cur.execute("CREATE INDEX idx_webevents_user_id ON webevents(user_id)")
            cur.execute("CREATE INDEX idx_webevents_event_type ON webevents(event_type)")
            cur.execute("CREATE INDEX idx_webevents_timestamp ON webevents(timestamp)")
            cur.execute("CREATE INDEX idx_webevents_date ON webevents(date)")
            cur.execute("CREATE INDEX idx_webevents_user_timestamp ON webevents(user_id, timestamp DESC)")
        
        print("✅ Созданы таблицы и индексы для веб-событий")
        
        # Загрузка данных
        print("📥 Загрузка данных в PostgreSQL...")
        
        # Загрузка веб-событий
        with pg_conn.cursor() as cur:
            for _, row in webevents_df.iterrows():
                cur.execute("""
                    INSERT INTO webevents (user_id, event_type, timestamp, date)
                    VALUES (%s, %s, %s, %s)
                """, (row['user_id'], row['event_type'], row['timestamp'], row['date']))
        
        pg_conn.commit()
        print(f"✅ Загружено {len(webevents_df):,} веб-событий")

        
        print(f"\n📊 Статистика PostgreSQL:")
        print(f"- Всего событий: {total_events:,}")
        print(f"- Уникальных пользователей: {unique_users:,}")
        
    except Exception as e:
        print(f"❌ Ошибка при работе с PostgreSQL: {e}")
        pg_conn.rollback()
    finally:
        pg_conn.close()
else:
    print("❌ Пропуск операций с PostgreSQL из-за ошибки подключения")
```
Результат выполнения

<img width="768" height="267" alt="image" src="https://github.com/user-attachments/assets/503ef96a-2870-4e3d-b772-1b02cb97b146" />


### 8. Агрегация в PostgreSQL
```
# Функция для выполнения агрегации событий по пользователям и типам в PostgreSQL
def aggregate_events_by_user_and_type():
    """Агрегация данных: количество событий каждого типа для каждого пользователя"""
    
    pg_conn = psycopg2.connect(**pg_conn_params)
    
    try:
        with pg_conn.cursor() as cur:
            # SQL запрос для агрегации данных
            query = """
            SELECT 
                user_id,
                event_type,
                COUNT(*) as event_count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY user_id), 1) as percentage
            FROM webevents
            GROUP BY user_id, event_type
            ORDER BY user_id, event_count DESC
            """
            
            cur.execute(query)
            results = cur.fetchall()
            return results
            
    except Exception as e:
        print(f"Ошибка в PostgreSQL запросе: {e}")
        return []
    finally:
        pg_conn.close()

# Функция для получения общей статистики по агрегированным данным
def get_aggregation_stats():
    """Получение статистики по агрегированным данным"""
    
    pg_conn = psycopg2.connect(**pg_conn_params)
    
    try:
        with pg_conn.cursor() as cur:
            # Исправленный запрос для подсчета уникальных комбинаций пользователь-тип события
            cur.execute("""
                SELECT COUNT(*) 
                FROM (
                    SELECT DISTINCT user_id, event_type 
                    FROM webevents
                ) as unique_combinations
            """)
            unique_combinations = cur.fetchone()[0]
            
            # Среднее количество типов событий на пользователя
            cur.execute("""
                SELECT AVG(event_types_count) 
                FROM (
                    SELECT user_id, COUNT(DISTINCT event_type) as event_types_count 
                    FROM webevents 
                    GROUP BY user_id
                ) as user_event_diversity
            """)
            avg_event_types = cur.fetchone()[0]
            
            return unique_combinations, avg_event_types
            
    except Exception as e:
        print(f"Ошибка в PostgreSQL запросе: {e}")
        return 0, 0
    finally:
        pg_conn.close()

# Выполнение агрегации данных с замером времени
print(f"📊 Выполнение агрегации данных в PostgreSQL...")

# Замер времени выполнения агрегации
aggregation_start = time.time()
aggregated_data = aggregate_events_by_user_and_type()
aggregation_time = time.time() - aggregation_start

# Получение статистики по агрегированным данным
stats_start = time.time()
unique_combinations, avg_event_types = get_aggregation_stats()
stats_time = time.time() - stats_start

print(f"Агрегация данных завершена")
print(f"Время выполнения агрегации: {aggregation_time:.4f} секунд")
print(f"Время получения статистики: {stats_time:.4f} секунд")
print(f"Общее время: {aggregation_time + stats_time:.4f} секунд")
# Базовая информация о результатах агрегации
if aggregated_data:
    print(f"\n📈 Результаты агрегации:")
    print(f"- Обработано записей: {len(aggregated_data):,}")
    print(f"- Уникальных комбинаций пользователь-тип события: {unique_combinations:,}")
    print(f"- Среднее количество типов событий на пользователя: {avg_event_types:.1f}")
    
    # Примеры агрегированных данных (первые 10 записей)
    print(f"\n Примеры агрегированных данных (первые 10 записей):")
    for i, (user_id, event_type, count, percentage) in enumerate(aggregated_data[:10], 1):
        print(f"  {i}. Пользователь {user_id}, {event_type}: {count} событий ({percentage}%)")
    
    # Анализ распределения данных
    print(f"\n📊 Анализ распределения агрегированных данных:")
    
    # Группируем данные по пользователям для анализа
    users_summary = {}
    for user_id, event_type, count, percentage in aggregated_data:
        if user_id not in users_summary:
            users_summary[user_id] = []
        users_summary[user_id].append((event_type, count, percentage))
    
    # Статистика по пользователям
    total_users = len(users_summary)
    users_with_multiple_events = sum(1 for events in users_summary.values() if len(events) > 1)
    
    print(f"- Всего пользователей в выборке: {total_users:,}")
    print(f"- Пользователей с несколькими типами событий: {users_with_multiple_events:,}")
    print(f"- Процент пользователей с разными типами событий: {users_with_multiple_events/total_users*100:.1f}%")
    
    # Анализ по типам событий
    event_totals = {}
    for user_id, events in users_summary.items():
        for event_type, count, _ in events:
            if event_type not in event_totals:
                event_totals[event_type] = 0
            event_totals[event_type] += count
    
    print(f"\n Общее распределение событий по типам:")
    total_events = sum(event_totals.values())
    for event_type, count in sorted(event_totals.items(), key=lambda x: x[1], reverse=True):
        percentage = (count / total_events) * 100
        print(f"  - {event_type}: {count:,} событий ({percentage:.1f}%)")
    
else:
    print("❌ Не удалось выполнить агрегацию данных")
```
Результат выполнения

<img width="567" height="538" alt="image" src="https://github.com/user-attachments/assets/f913d8ca-9a25-4444-8647-8846dc430429" />

### 9. Агрегация в MongoDB
```
# Функция для группировки событий по пользователям и типам в MongoDB
def aggregate_events_by_user_and_type_mongodb():
    """Агрегация данных: количество событий каждого типа для каждого пользователя в MongoDB"""
    
    try:
        # Проверяем, что подключение к MongoDB активно
        if not mongo_client:
            print("❌ Нет подключения к MongoDB")
            return []
            
        mongo_db = mongo_client['studmongo']
        webevents_collection = mongo_db['webevents']
        
        # Агрегация для группировки событий по пользователям и типам
        aggregation_pipeline = [
            # Группировка по user_id и event_type с подсчетом количества
            {"$group": {
                "_id": {
                    "user_id": "$user_id",
                    "event_type": "$event_type"
                },
                "event_count": {"$sum": 1}
            }},
            # Проекция для удобного формата вывода
            {"$project": {
                "user_id": "$_id.user_id",
                "event_type": "$_id.event_type", 
                "event_count": 1,
                "_id": 0
            }},
            # Сортировка по user_id и количеству событий (по убыванию)
            {"$sort": {
                "user_id": 1,
                "event_count": -1
            }}
        ]
        
        # Выполнение агрегации
        results = list(webevents_collection.aggregate(aggregation_pipeline))
        return results
        
    except Exception as e:
        print(f"❌ Ошибка в MongoDB агрегационном запросе: {e}")
        import traceback
        traceback.print_exc()
        return []

# Функция для получения дополнительной статистики по агрегированным данным
def get_mongodb_aggregation_stats():
    """Получение статистики по агрегированным данным в MongoDB"""
    
    try:
        if not mongo_client:
            return 0, 0
            
        mongo_db = mongo_client['studmongo']
        webevents_collection = mongo_db['webevents']
        
        # Получение количества уникальных комбинаций пользователь-тип события
        unique_combinations_pipeline = [
            {"$group": {
                "_id": {
                    "user_id": "$user_id",
                    "event_type": "$event_type"
                }
            }},
            {"$count": "unique_combinations"}
        ]
        
        result = list(webevents_collection.aggregate(unique_combinations_pipeline))
        unique_combinations = result[0]['unique_combinations'] if result else 0
        
        # Получение среднего количества типов событий на пользователя
        avg_event_types_pipeline = [
            {"$group": {
                "_id": "$user_id",
                "event_types_count": {"$addToSet": "$event_type"}
            }},
            {"$project": {
                "event_types_count": {"$size": "$event_types_count"}
            }},
            {"$group": {
                "_id": None,
                "avg_event_types": {"$avg": "$event_types_count"}
            }}
        ]
        
        result = list(webevents_collection.aggregate(avg_event_types_pipeline))
        avg_event_types = result[0]['avg_event_types'] if result else 0
        
        return unique_combinations, avg_event_types
        
    except Exception as e:
        print(f"❌ Ошибка при получении статистики MongoDB: {e}")
        return 0, 0

# Выполнение агрегации данных в MongoDB с замером времени
print(f"📊 Выполнение агрегации данных в MongoDB...")

# Замер времени выполнения агрегации
aggregation_start = time.time()
mongodb_aggregated_data = aggregate_events_by_user_and_type_mongodb()
aggregation_time = time.time() - aggregation_start

# Получение статистики по агрегированным данным
stats_start = time.time()
mongodb_unique_combinations, mongodb_avg_event_types = get_mongodb_aggregation_stats()
stats_time = time.time() - stats_start
print(f" Агрегация данных в MongoDB завершена")
print(f" Время выполнения агрегации: {aggregation_time:.4f} секунд")
print(f" Время получения статистики: {stats_time:.4f} секунд")
print(f" Общее время: {aggregation_time + stats_time:.4f} секунд")

# Базовая информация о результатах агрегации
if mongodb_aggregated_data:
    print(f"\n📈 Результаты агрегации в MongoDB:")
    print(f"- Обработано записей: {len(mongodb_aggregated_data):,}")
    print(f"- Уникальных комбинаций пользователь-тип события: {mongodb_unique_combinations:,}")
    print(f"- Среднее количество типов событий на пользователя: {mongodb_avg_event_types:.1f}")
    
    # Примеры агрегированных данных (первые 10 записей)
    print(f"\n Примеры агрегированных данных (первые 10 записей):")
    for i, record in enumerate(mongodb_aggregated_data[:10], 1):
        print(f"  {i}. Пользователь {record['user_id']}, {record['event_type']}: {record['event_count']} событий")
    
    # Анализ распределения данных
    print(f"\n Анализ распределения агрегированных данных:")
    
    # Группируем данные по пользователям для анализа
    users_summary = {}
    for record in mongodb_aggregated_data:
        user_id = record['user_id']
        if user_id not in users_summary:
            users_summary[user_id] = []
        users_summary[user_id].append((record['event_type'], record['event_count']))
    
    # Статистика по пользователям
    total_users = len(users_summary)
    users_with_multiple_events = sum(1 for events in users_summary.values() if len(events) > 1)
    
    print(f"- Всего пользователей в выборке: {total_users:,}")
    print(f"- Пользователей с несколькими типами событий: {users_with_multiple_events:,}")
    print(f"- Процент пользователей с разными типами событий: {users_with_multiple_events/total_users*100:.1f}%")
    
    # Анализ по типам событий
    event_totals = {}
    for user_id, events in users_summary.items():
        for event_type, count in events:
            if event_type not in event_totals:
                event_totals[event_type] = 0
            event_totals[event_type] += count
    
    print(f"\n Общее распределение событий по типам:")
    total_events = sum(event_totals.values())
    for event_type, count in sorted(event_totals.items(), key=lambda x: x[1], reverse=True):
        percentage = (count / total_events) * 100
        print(f"  - {event_type}: {count:,} событий ({percentage:.1f}%)")
    
    # Дополнительная статистика по MongoDB
    print(f"\n Дополнительная MongoDB статистика:")
    mongo_db = mongo_client['studmongo']
    total_documents = mongo_db.webevents.count_documents({})
    print(f"- Всего документов в коллекции: {total_documents:,}")
    print(f"- Размер коллекции: {mongo_db.command('collstats', 'webevents')['size'] / (1024*1024):.2f} MB")
    
else:
    print("❌ Не удалось выполнить агрегацию данных в MongoDB")
```
Результат выполнения

<img width="710" height="535" alt="image" src="https://github.com/user-attachments/assets/f7c66af5-f034-4e42-9ba2-1067f3af113b" />

### 10. 
```
import time
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import pandas as pd

# Настройка стиля графиков
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

def measure_performance():
    """Измерение производительности PostgreSQL и MongoDB"""
    
    print("ЗАПУСК ТЕСТИРОВАНИЯ ПРОИЗВОДИТЕЛЬНОСТИ")
    print("=" * 50)
    
    # Тестирование PostgreSQL
    print("\nТЕСТИРОВАНИЕ POSTGRESQL...")
    pg_start_time = time.time()
    
    postgres_data = aggregate_events_by_user_and_type()
    
    pg_end_time = time.time()
    pg_execution_time = pg_end_time - pg_start_time
    
    # Анализ результатов PostgreSQL
    pg_df = pd.DataFrame(postgres_data, columns=['user_id', 'event_type', 'count', 'percentage'])
    pg_total_events = pg_df['count'].sum()
    
    print(f"PostgreSQL завершен:")
    print(f"   - Время выполнения: {pg_execution_time:.4f} сек")
    print(f"   - Обработано событий: {pg_total_events:,}")
    print(f"   - Получено группировок: {len(postgres_data):,}")
    
    # Тестирование MongoDB
    print("\nТЕСТИРОВАНИЕ MONGODB...")
    mongo_start_time = time.time()
    
    mongodb_data = aggregate_events_by_user_and_type_mongodb()
    
    mongo_end_time = time.time()
    mongo_execution_time = mongo_end_time - mongo_start_time
    
    # Анализ результатов MongoDB
    mongo_df = pd.DataFrame(mongodb_data)
    mongo_total_events = mongo_df['event_count'].sum()
    
    print(f"MongoDB завершен:")
    print(f"   - Время выполнения: {mongo_execution_time:.4f} сек")
    print(f"   - Обработано событий: {mongo_total_events:,}")
    print(f"   - Получено группировок: {len(mongodb_data):,}")
    
    return {
        'postgres': {
            'time': pg_execution_time,
            'total_events': pg_total_events,
            'efficiency': pg_total_events / pg_execution_time if pg_execution_time > 0 else 0
        },
        'mongodb': {
            'time': mongo_execution_time,
            'total_events': mongo_total_events,
            'efficiency': mongo_total_events / mongo_execution_time if mongo_execution_time > 0 else 0
        }
    }

def create_performance_charts(performance_data):
    """Создание графиков производительности"""
    
    # Подготовка данных для графиков
    systems = ['PostgreSQL', 'MongoDB']
    times = [performance_data['postgres']['time'], performance_data['mongodb']['time']]
    efficiencies = [performance_data['postgres']['efficiency'], performance_data['mongodb']['efficiency']]
    
    # Создание графиков
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    
    # График 1: Время выполнения
    bars1 = ax1.bar(systems, times, color=['#3366cc', '#dc3912'], alpha=0.7)
    ax1.set_ylabel('Время выполнения (секунды)')
    ax1.set_title('Сравнение времени выполнения операций группировки', fontsize=14, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    
    # Добавляем значения на столбцы времени
    for bar in bars1:
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height + 0.001,
                f'{height:.4f} сек', ha='center', va='bottom', fontweight='bold')
    
    # График 2: Эффективность (событий в секунду)
    bars2 = ax2.bar(systems, efficiencies, color=['#3366cc', '#dc3912'], alpha=0.7)
    ax2.set_ylabel('Событий в секунду')
    ax2.set_title('Эффективность обработки данных', fontsize=14, fontweight='bold')
    ax2.grid(True, alpha=0.3)
    
    # Добавляем значения на столбцы эффективности
    for bar in bars2:
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height + 10,
                f'{height:.0f} событий/сек', ha='center', va='bottom', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('performance_comparison.png', dpi=300, bbox_inches='tight')
    plt.show()

def print_analytical_report_analysis(performance_data):
    """Анализ применимости для построения аналитических отчетов"""
    
    print("\n" + "=" * 70)
    print("АНАЛИЗ ПРИМЕНИМОСТИ ДЛЯ АНАЛИТИЧЕСКИХ ОТЧЕТОВ")
    print("=" * 70)
    
    pg_time = performance_data['postgres']['time']
    mongo_time = performance_data['mongodb']['time']
    pg_efficiency = performance_data['postgres']['efficiency']
    mongo_efficiency = performance_data['mongodb']['efficiency']
    
    # Определение лидеров
    time_winner = "PostgreSQL" if pg_time < mongo_time else "MongoDB"
    efficiency_winner = "PostgreSQL" if pg_efficiency > mongo_efficiency else "MongoDB"
    
    time_diff = abs(pg_time - mongo_time)
    
    print(f"\nРЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ:")
    print(f"   Время выполнения PostgreSQL: {pg_time:.4f} сек")
    print(f"   Время выполнения MongoDB: {mongo_time:.4f} сек")
    print(f"   Эффективность PostgreSQL: {pg_efficiency:.0f} событий/сек")
    print(f"   Эффективность MongoDB: {mongo_efficiency:.0f} событий/сек")
    
    print(f"\nСРАВНИТЕЛЬНЫЙ АНАЛИЗ:")
    print(f"   Быстрее по времени: {time_winner}")
    print(f"   Эффективнее по обработке: {efficiency_winner}")
    
    print(f"\nВЫВОДЫ ДЛЯ АНАЛИТИЧЕСКИХ ОТЧЕТОВ:")
    
    # Анализ применимости
    if time_winner == "PostgreSQL" and efficiency_winner == "PostgreSQL":
        print("   PostgreSQL демонстрирует превосходную производительность для аналитических запросов")
        print("   Рекомендуется для сложных отчетов с множественными группировками и фильтрами")
        print("   Идеален для интеграции с BI-инструментами (Tableau, Power BI)")
    elif time_winner == "MongoDB" and efficiency_winner == "MongoDB":
        print("   MongoDB показывает лучшие результаты для агрегационных операций")
        print("   Рекомендуется для отчетов на основе документной модели данных")
        print("   Эффективен для гибких схем данных и быстрого прототипирования")
    else:
        print("   Смешанные результаты - выбор зависит от конкретных требований")
        if time_winner == "PostgreSQL":
            print("   PostgreSQL быстрее для однократных запросов")
        else:
            print("   MongoDB быстрее для однократных запросов")
        
        if efficiency_winner == "PostgreSQL":
            print("   PostgreSQL эффективнее при больших объемах данных")
        else:
            print("   MongoDB эффективнее при больших объемах данных")
    
    print(f"\nРЕКОМЕНДАЦИИ ПО ПРИМЕНЕНИЮ:")
    
    # Рекомендации для PostgreSQL
    print(f"   PostgreSQL лучше подходит для:")
    print(f"      - Сложных SQL-запросов с JOIN и подзапросами")
    print(f"      - Транзакционных отчетов с гарантированной целостностью данных")
    print(f"      - Систем с жесткой схемой данных и реляционными связями")
    print(f"      - Интеграции с традиционными BI-инструментами")
    
    # Рекомендации для MongoDB
    print(f"   MongoDB лучше подходит для:")
    print(f"      - Гибких схем данных и быстрого изменения структуры отчетов")
    print(f"      - Обработки JSON-данных и вложенных структур")
    print(f"      - Горизонтального масштабирования и распределенных систем")
    print(f"      - Прототипирования и итеративной разработки отчетов")

# Основной код тестирования
print("ТЕСТИРОВАНИЕ ПРОИЗВОДИТЕЛЬНОСТИ ДЛЯ АНАЛИТИЧЕСКИХ ОТЧЕТОВ")
print("=" * 70)

# Запуск тестирования
performance_data = measure_performance()

# Создание графиков
create_performance_charts(performance_data)

# Вывод анализа для аналитических отчетов
print_analytical_report_analysis(performance_data)

print(f"\nТестирование завершено!")
```
Результат выполнения

<img width="848" height="293" alt="image" src="https://github.com/user-attachments/assets/a4484681-98b0-4287-bb0f-cf7e856253b9" />
<img width="874" height="367" alt="image" src="https://github.com/user-attachments/assets/94eea8fe-e491-4fc8-b615-2579c2c94219" />
<img width="778" height="515" alt="image" src="https://github.com/user-attachments/assets/15a59992-8f47-4bfe-b696-2e698e4306b4" />


