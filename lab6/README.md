# Анализ и визуализация больших данных. Машинное обучение на больших данных с использованием Apache Spark MLlib

## Цель и задачи работы:
- Познакомиться с понятием «большие данные» и способами их обработки;
- Познакомиться с инструментом Apache Spark и возможностями, которые он предоставляет для обработки больших данных.
- Получить навыки выполнения разведочного анализа данных использованием pyspark.

## Выполнение работы в Google Colab
### 1. Инициализация платформы Spark и загрузка данных в фрейм данных Spark
Импорт модулей, не связанных с PySpark и подключение к Гугл диску:

<img width="1010" height="704" alt="image" src="https://github.com/user-attachments/assets/c1e7a023-522f-4e54-b209-c708bfa597ff" />


Импорт модулей, связанных с PySpark:

<img width="869" height="600" alt="image" src="https://github.com/user-attachments/assets/6ecd7e4f-ef30-4309-9cc6-0d84499e7fd3" />

### 2. Обзор набора данных

Обзор данных и столбцов:

<img width="1060" height="685" alt="image" src="https://github.com/user-attachments/assets/50a7f176-8313-4521-b349-a245e9de61b9" />

Описание фрейма данных:

<img width="1580" height="382" alt="image" src="https://github.com/user-attachments/assets/d3ac991b-a3b0-40ec-b3a0-ef70433f5350" />

### 3. Обнаружение пропущенных значений и аномальных нулей.

Обзор столбцов:

<img width="1576" height="362" alt="image" src="https://github.com/user-attachments/assets/d668b7cd-641d-41ae-9c9d-9cd6c4a12c62" />

Проверка столбцов на наличие NaN значений:
<img width="1579" height="337" alt="image" src="https://github.com/user-attachments/assets/043fae83-7afa-4f1c-94ee-61e693531754" />

Получение общей сводки данных:

<img width="1242" height="504" alt="image" src="https://github.com/user-attachments/assets/45b63e7a-9e47-42b7-bd28-48dfbbf8f218" />

Вывод статистики по тренировкам, число записей которых превышает 50:

<img width="852" height="231" alt="image" src="https://github.com/user-attachments/assets/ed0379e6-e33c-45ee-880e-b35f1de7e8c5" />

### 4. Ленивая оценка Pyspark
Топ-5 тренировок по количеству пользователей:

<img width="1173" height="319" alt="image" src="https://github.com/user-attachments/assets/e5a3404c-d0f1-4581-b846-924a83791ddc" />

### 5. Исследовательский анализ данных
Создание таблицы и графиков для 5 видов спорта, которыми занимается наибольшее количество пользователей:

<img width="1192" height="723" alt="image" src="https://github.com/user-attachments/assets/c7ea3c76-1bf1-429c-9ef5-2867aac17c5f" />

<img width="746" height="499" alt="image" src="https://github.com/user-attachments/assets/4c27b7b2-35d4-409e-a381-6a6229b232e1" />

<img width="1056" height="387" alt="image" src="https://github.com/user-attachments/assets/a213e9bf-0841-4924-90c5-05a6a9688d1b" />

<img width="917" height="281" alt="image" src="https://github.com/user-attachments/assets/f340f4da-0bab-4383-97c0-384a90090a4c" />

### 6. UNSTACK PYSPARK DATAFRAME

Вычисления процентов участия в тренировках мужчин и женщин для всех видов спорта:

<img width="1571" height="372" alt="image" src="https://github.com/user-attachments/assets/cd288b3b-aae2-4749-a7b8-03cda697597a" />

<img width="916" height="724" alt="image" src="https://github.com/user-attachments/assets/b48b12ed-a7b6-45ed-a6e6-ed254f797fdf" />

<img width="1009" height="726" alt="image" src="https://github.com/user-attachments/assets/a5c7d769-acea-4894-ab4a-5e7cceb0dbea" />

<img width="929" height="586" alt="image" src="https://github.com/user-attachments/assets/b7a9cb55-ffa4-4e8c-bbe1-0573859a9fbd" />


