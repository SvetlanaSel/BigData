#!/usr/bin/env python3
"""
Анализ данных фильмов с использованием Pandas
Задача: найти топ-10 режиссеров по среднему рейтингу IMDB
"""
import pandas as pd
import sys
import os

def load_data(filepath):
    """Загрузить данные из CSV файла"""
    try:
        df = pd.read_csv(filepath, low_memory=False)
        print(f"Загружено строк: {len(df)}")
        return df
    except Exception as e:
        print(f"Ошибка при загрузке данных: {e}")
        sys.exit(1)

def clean_data(df):
    """Очистка и подготовка данных"""
    print("\n=== Очистка данных ===")
    print(f"Исходное количество строк: {len(df)}")
    
    # Удалить строки без режиссера или рейтинга
    df = df[df['director_name'].notna()]
    df = df[df['imdb_score'].notna()]
    
    # Удалить пустых режиссеров
    df = df[df['director_name'] != '']
    
    # Преобразовать рейтинг в числовой формат
    df['imdb_score'] = pd.to_numeric(df['imdb_score'], errors='coerce')
    
    # Удалить строки с некорректным рейтинга
    df = df[df['imdb_score'].notna()]
    
    print(f"Количество строк после очистки: {len(df)}")
    print(f"Уникальных режиссеров: {df['director_name'].nunique()}")
    
    return df

def analyze_directors_by_rating(df):
    """Анализ среднего рейтинга по режиссерам"""
    print("\n=== Анализ среднего рейтинга по режиссерам ===")
    
    # Группировка по режиссеру и вычисление статистики по рейтингу
    result = df.groupby('director_name')['imdb_score'].agg([
        'mean', 'count'
    ]).reset_index()
    
    result.columns = ['Director', 'Mean_Rating', 'Movies_Count']
    
    # Сортировка по среднему рейтингу (по убыванию)
    result = result.sort_values('Mean_Rating', ascending=False)
    
    return result

def find_top_directors(df):
    """Найти топ-10 режиссеров по среднему рейтингу"""
    result = analyze_directors_by_rating(df)
    
    print("\n=== Результаты ===")
    print("\nТоп-10 режиссеров по среднему рейтингу IMDB:")
    top_10 = result.head(10)
    print(top_10.to_string(index=False, float_format='%.2f'))
    
    if len(top_10) > 0:
        best_director = top_10.iloc[0]
        print(f"\nЛучший режиссер по среднему рейтингу: '{best_director['Director']}'")
        print(f"Средний рейтинг: {best_director['Mean_Rating']:.2f}")
        print(f"Количество фильмов: {int(best_director['Movies_Count'])}")
    
    return result

def main():
    # Путь к файлу данных
    data_file = '/opt/data/movie.csv'
    
    if not os.path.exists(data_file):
        data_file = 'movie.csv'
    
    if not os.path.exists(data_file):
        print(f"Файл не найден: {data_file}")
        sys.exit(1)
    
    print("=== Анализ данных фильмов ===")
    print(f"Файл: {data_file}")
    
    # Загрузка данных
    df = load_data(data_file)
    
    # Показать базовую информацию
    print("\n=== Информация о данных ===")
    print(f"Колонки: {list(df.columns)}")
    print(f"Общее количество строк: {len(df)}")
    
    # Проверим наличие нужных колонок
    required_columns = ['director_name', 'imdb_score']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"Отсутствуют необходимые колонки: {missing_columns}")
        sys.exit(1)
    
    print("\nПервые 5 строк:")
    print(df[['director_name', 'imdb_score', 'movie_title']].head())
    
    # Очистка данных
    df_clean = clean_data(df)
    
    # Анализ
    result = find_top_directors(df_clean)
    
    # Сохранить результаты
    output_file = 'results/top_directors_by_rating.csv'
    os.makedirs('results', exist_ok=True)
    result.to_csv(output_file, index=False)
    print(f"\nРезультаты сохранены в: {output_file}")
    
    return result

if __name__ == '__main__':
    main()