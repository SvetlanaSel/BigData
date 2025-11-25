#!/usr/bin/env python3
"""
Анализ данных фильмов с использованием PySpark
Задача: найти топ-10 режиссеров по среднему рейтингу IMDB
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, round
from pyspark.sql.types import FloatType
import sys
import os

def create_spark_session():
    """Создать Spark сессию с локальной файловой системой"""
    spark = SparkSession.builder \
        .appName("Movie Analysis - Top Directors by Rating") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def load_data(spark, filepath):
    """Загрузить данные из локального файла"""
    print(f"Загрузка данных из: {filepath}")
    
    try:
        local_path = f"file://{filepath}"
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(local_path)
        
        print(f"Данные загружены успешно")
        print(f"Количество строк: {df.count()}")
        return df
    except Exception as e:
        print(f"Ошибка при загрузке данных: {e}")
        return None

def clean_and_prepare(df):
    """Очистка и подготовка данных"""
    if df is None:
        return None
        
    print("\n=== Очистка данных ===")
    initial_count = df.count()
    print(f"Исходное количество строк: {initial_count}")
    
    df = df.filter(col('director_name').isNotNull())
    df = df.filter(col('imdb_score').isNotNull())
    df = df.filter(col('director_name') != '')
    df = df.withColumn('imdb_score', col('imdb_score').cast(FloatType()))
    df = df.filter(col('imdb_score').isNotNull())
    
    final_count = df.count()
    print(f"Количество строк после очистки: {final_count}")
    print(f"Уникальных режиссеров: {df.select('director_name').distinct().count()}")
    
    return df

def analyze_rating_by_director(df):
    """Анализ среднего рейтинга по режиссерам"""
    print("\n=== Анализ среднего рейтинга по режиссерам ===")
    
    result = df.groupBy('director_name') \
        .agg(
            round(avg('imdb_score'), 2).alias('Mean_Rating'),
            count('*').alias('Movies_Count')
        ) \
        .orderBy(col('Mean_Rating').desc())
    
    return result

def main():
    local_path = "/opt/data/movie.csv"
    
    spark = create_spark_session()
    
    print("=== Анализ данных фильмов с использованием PySpark ===")
    print("Задача: Топ-10 режиссеров по среднему рейтингу IMDB")
    
    if not os.path.exists(local_path):
        print(f"Файл не найден: {local_path}")
        print("Завершение работы.")
        spark.stop()
        sys.exit(1)
    
    df = load_data(spark, local_path)
    
    if df is None:
        print("Не удалось загрузить данные. Завершение работы.")
        spark.stop()
        sys.exit(1)
    
    print("\n=== Схема данных ===")
    df.printSchema()
    
    print("\n=== Статистика по данным ===")
    print(f"Всего фильмов: {df.count()}")
    print(f"Всего режиссеров: {df.select('director_name').distinct().count()}")
    
    print("\nПервые 5 строк (режиссеры и рейтинги):")
    df.select('director_name', 'imdb_score', 'movie_title').show(5, truncate=30)
    
    df_clean = clean_and_prepare(df)
    
    if df_clean is None or df_clean.count() == 0:
        print("Ошибка: после очистки данных не осталось записей")
        spark.stop()
        sys.exit(1)
    
    result = analyze_rating_by_director(df_clean)
    
    print("\n=== Результаты ===")
    print("\nТоп-10 режиссеров по среднему рейтингу IMDB:")
    result.show(10, truncate=False)
    
    top_directors = result.collect()
    if top_directors:
        best_director = top_directors[0]
        print(f"\nЛучший режиссер по среднему рейтингу: '{best_director['director_name']}'")
        print(f"Средний рейтинг: {best_director['Mean_Rating']:.2f}")
        print(f"Количество фильмов: {best_director['Movies_Count']}")
    
    local_output = "/opt/results/top_directors_spark"
    os.makedirs("/opt/results", exist_ok=True)
    result.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"file://{local_output}")
    print(f"\nРезультаты сохранены: {local_output}")
    
    spark.stop()
    print("\n=== Анализ завершен успешно ===")

if __name__ == '__main__':
    main()