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

Найдем топ-5 видов спорта по числу тренировок:

<img width="1067" height="724" alt="image" src="https://github.com/user-attachments/assets/d74e47e7-a414-4b60-832c-bedc4f90655c" />

<img width="853" height="502" alt="image" src="https://github.com/user-attachments/assets/132f7051-c4fe-4f77-88d3-f61efabed506" />

<img width="1034" height="374" alt="image" src="https://github.com/user-attachments/assets/48c04b0a-f052-423b-bff9-6ada9259fed9" />

Узнаем, сколько людей занималось более чем одним видом спорта:

<img width="971" height="413" alt="image" src="https://github.com/user-attachments/assets/f79f9ccc-52d7-45b4-82e0-df41e162b6bb" />

Построим диаграмму:


<img width="1032" height="548" alt="image" src="https://github.com/user-attachments/assets/d2fb5b6a-e75a-4ba3-ad2f-5eaf2c1d6e66" />

Распределение количества рекордов за тренировку:

<img width="1332" height="315" alt="image" src="https://github.com/user-attachments/assets/184bf5e1-0837-4cd4-854e-75ebbe417ce4" />

Результат:

<img width="1566" height="400" alt="image" src="https://github.com/user-attachments/assets/8c034b55-450e-4a0e-b235-2d54f0a52447" />

<img width="1552" height="681" alt="image" src="https://github.com/user-attachments/assets/d31d5c29-8b2f-4656-b8b1-bc654ead9e4d" />

Посмотрим, сколько пользователей выполнило более 10 тренировок:

<img width="1063" height="489" alt="image" src="https://github.com/user-attachments/assets/516c7823-57f2-4d4c-8ac2-4b0c20a9e621" />


### 7. Pyspark UDF

<img width="1585" height="249" alt="image" src="https://github.com/user-attachments/assets/7d5eb667-5ff3-4491-8e9f-7044f3c76129" />

<img width="973" height="516" alt="image" src="https://github.com/user-attachments/assets/1204d50b-d3f2-44ad-9f32-aff2426846b0" />

<img width="1012" height="200" alt="image" src="https://github.com/user-attachments/assets/f9778416-b961-42a6-b1a9-a05ffe0bce51" />

Теперь посмотрим на продолжительность каждой тренировки (в минутах).

<img width="781" height="136" alt="image" src="https://github.com/user-attachments/assets/b8849672-1edc-4893-ac4f-2860dd3a2f64" />

Построим графики продолжительности тренировок:

<img width="1052" height="328" alt="image" src="https://github.com/user-attachments/assets/1465ff71-a279-42c8-878e-16400fa6f760" />

Результат:

<img width="1557" height="702" alt="image" src="https://github.com/user-attachments/assets/dd2c8f5a-c947-483f-8d34-9f7bc5a29dcc" />

<img width="783" height="58" alt="image" src="https://github.com/user-attachments/assets/968af26d-39d2-4fa1-9956-0c007f31cded" />

### 8.Преобразование объектов строк в устойчивый распределенный набор данных Spark (RDD)

Создадим вспомогательные функции для просмотра интервалов между записями:

<img width="752" height="551" alt="image" src="https://github.com/user-attachments/assets/41a3c0c1-d02e-4923-bee9-b964a36de978" />

Результат:

<img width="790" height="212" alt="image" src="https://github.com/user-attachments/assets/b7712ce8-6950-4638-bd7b-ba7d555ec59e" />

Теперь отобразим эти числа в виде столбцов и линейных диаграмм:

<img width="762" height="526" alt="image" src="https://github.com/user-attachments/assets/7e46d807-27d5-41ac-915f-f5c8ccd51f10" />

Результат:

<img width="1270" height="477" alt="image" src="https://github.com/user-attachments/assets/ee8d099c-2458-41a8-bd9f-d3fe5162452d" />

Время начала тренировки

<img width="809" height="417" alt="image" src="https://github.com/user-attachments/assets/bcd21bc4-e9fb-4a92-806d-f67b9d4b21d5" />

Результат:

<img width="1245" height="512" alt="image" src="https://github.com/user-attachments/assets/ba9e5c90-e3db-4d41-9e37-0702a887a896" />

Посмотрите глубже на информацию на уровне строки

<img width="723" height="614" alt="image" src="https://github.com/user-attachments/assets/00657290-000b-4c3f-a3ee-7d6c16cff08a" />

нормализуем время для всех тренировок, рассчитав продолжительность (в секундах) каждой записи временной метки из первой записи тренировки (первый элемент datetime списка в этой тренировке).
Затем отображаем частоту сердечных сокращений в зависимости от этого нормализованного времени, группируя по видам спорта.

<img width="690" height="434" alt="image" src="https://github.com/user-attachments/assets/55be8495-1f09-4d2e-992f-07ca7c3a3692" />

Результат:

<img width="892" height="509" alt="image" src="https://github.com/user-attachments/assets/d4aa5ba8-13e4-442a-905b-219f75ac358f" />

Перемещения во время тренировки

<img width="578" height="213" alt="image" src="https://github.com/user-attachments/assets/689388a6-ab4c-4bc8-9d61-2df3033c4c68" />

<img width="714" height="758" alt="image" src="https://github.com/user-attachments/assets/13c7acbc-ff9b-4c43-9661-00981ee1957f" />

<img width="572" height="219" alt="image" src="https://github.com/user-attachments/assets/653df92e-8c0b-4613-8f3c-725614a8348b" />

## Индивидуальные задания. Вариант 13

Задание 1: Как вы думаете, почему в данных присутствует пол 'unknown' (раздел 3)? Предложите 2-3 возможные причины.

Ответ:
1. Технические ограничения и ошибки данных
- Неполная интеграция между системами (например, когда данные импортируются из разных источников)
- Отсутствие обязательности заполнения поля "пол" при регистрации
- Технические сбои
2. Конфиденциальность и пользовательские предпочтения
- Пользователи сознательно выбрали не указывать свой пол в настройках профиля
3. Особенности сбора данных
- Данные могли собираться из публичных источников, где пол не был указан
- Наличие ботов или тестовых аккаунтов

Задание 2: Сравните гистограммы PerWorkoutRecordCount для 'running' и 'indoor cycling' (раздел 5). Какие выводы можно сделать о типичной плотности записи данных для этих активностей?

Ответ:
Топ-5 активностей по количеству тренировок:

- run: 865 тренировок
- bike: 794 тренировки
- mountain bike: 336 тренировок
- bike (transport): 252 тренировки
- walk: 209 тренировок
Ключевая статистика по плотности записей:

- 5,541 тренировок имеют менее 50 записей каждая

- 5,541 тренировок имеют менее 50 записей каждая
Средняя плотность для этих тренировок: 23 записи
Выводы:

- Бег (running) является самой популярной активностью (865 тренировок), что может указывать на его регулярное использование пользователями. Велоактивности в сумме (bike + mountain bike + bike transport) составляют 1,382 тренировки, что больше чем у бега, но распределены по подтипам

- Общая картина плотности:

- Значительная часть тренировок (5,541 из 253,020) имеет низкую плотность данных (<50 записей). Средняя плотность 23 записи на тренировку для этой группы довольно низкая

Задание 3: Представьте, что Endomondo хочет запустить таргетированную кампанию для пользователей, занимающихся несколькими видами спорта. Какие данные из анализа в разделе 6 помогут сегментировать этих пользователей?

Ответ:
Ключевые данные:

- Гендерное распределение по видам спорта;
- Количество видов активностей для каждого пользователя (среднее количество видов спорта ~3 на поль;зователя, максимум: до 16 видов спорта у одного пользователя)
- Разделение по видам активности пользователей (группровка для категории "bike" и пр).

```
from pyspark.sql import functions as F

sample_df = df.sample(False, fraction=0.0001, seed=42)
result_df = sample_df.limit(10)

```
Задание 5: Можно ли использовать Spark MLlib для построения рекомендательной системы (например, на основе коллаборативной фильтрации), которая предлагает пользователям виды спорта, популярные среди похожих на них пользователей? Какие данные (userId, sport, возможно, duration) для этого нужны?


Ответ:
- Spark MLlib хорошо подходит для построения такой рекомендательной системы с использованием ALS (Alternating Least Squares) для коллаборативной фильтрации.

- Необходимые данные: userId (числовой идентификатор пользователя), sport (вид спорта, преобразованный в числовой ID), рейтинг (оценка предпочтения), который можно рассчитать на основе количества тренировок по виду спорта, продолжительности тренировок (duration), частоты занятий

- Минимальный набор данных: таблица взаимодействий пользователь-спорт с числовыми рейтингами, где высокий рейтинг означает сильное предпочтение.
