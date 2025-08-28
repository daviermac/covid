import pandas as pd
import mysql.connector
from mysql.connector import Error
from datetime import datetime

DB_CONFIG = {
    'host': 'localhost',
    'database': 'covid_db',  
    'user': 'root',
    'password': '1910'
}

def extract_data(file_path, limit=20):
    print("Extraindo dados do arquivo CSV...")
    try:
        df = pd.read_csv(file_path, delimiter=',', encoding='utf-8', nrows=limit)
        print(f"Dados extraídos com sucesso! Total de registros: {len(df)}")
        return df
    except Exception as e:
        print(f"Erro ao extrair dados: {e}")
        return None

def clean_data(df):
    print("Limpando dados...")
    try:
        df = df.dropna(subset=['city', 'last_available_confirmed', 'last_available_deaths'])
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['last_available_confirmed'] = pd.to_numeric(df['last_available_confirmed'], errors='coerce')
        df['last_available_deaths'] = pd.to_numeric(df['last_available_deaths'], errors='coerce')
        df['estimated_population'] = pd.to_numeric(df['estimated_population'], errors='coerce')
        df = df[(df['last_available_confirmed'] >= 0) & (df['last_available_deaths'] >= 0)]
        print(f"Dados limpos com sucesso! Total de registros após limpeza: {len(df)}")
        return df
    except Exception as e:
        print(f"Erro ao limpar dados: {e}")
        return df

def analyze_data(df):
    print("Analisando dados...")
    try:
        deaths_by_city = df.groupby('city')['last_available_deaths'].sum().sort_values(ascending=False)
        population_info = df[['city', 'estimated_population']].drop_duplicates().sort_values('estimated_population', ascending=False)
        city_with_most_cases = df.loc[df['last_available_confirmed'].idxmax()]['city']
        most_cases_count = df['last_available_confirmed'].max()
        city_with_least_cases = df.loc[df['last_available_confirmed'].idxmin()]['city']
        least_cases_count = df['last_available_confirmed'].min()
        
        print("Análise concluída!")
        return {
            'deaths_by_city': deaths_by_city,
            'population_info': population_info,
            'city_with_most_cases': (city_with_most_cases, most_cases_count),
            'city_with_least_cases': (city_with_least_cases, least_cases_count)
        }
    except Exception as e:
        print(f"Erro ao analisar dados: {e}")
        return None

def insert_data_to_mysql(df):
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            cursor = connection.cursor()
            insert_query = """
            INSERT INTO covid_data (date, city, confirmed, deaths, estimated_population)
            VALUES (%s, %s, %s, %s, %s)
            """
            for index, row in df.iterrows():
                record = (
                    row['date'] if pd.notnull(row['date']) else None,
                    row['city'] if pd.notnull(row['city']) else None,
                    int(row['last_available_confirmed']) if pd.notnull(row['last_available_confirmed']) else None,
                    int(row['last_available_deaths']) if pd.notnull(row['last_available_deaths']) else None,
                    int(row['estimated_population']) if pd.notnull(row['estimated_population']) else None
                )
                cursor.execute(insert_query, record)
            connection.commit()
            print(f"{cursor.rowcount} registros inseridos no banco de dados!")
            return True
    except Error as e:
        print(f"Erro ao inserir dados no MySQL: {e}")
        return False
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def generate_report(analysis_results):
    print("Gerando relatório...")
    try:
        report_content = f"""
RELATÓRIO DE ANÁLISE DE DADOS DA COVID-19
=========================================

Data de geração: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}

1. TOTAL DE MORTES POR CIDADE:
{'='*40}
"""
        
        for city, deaths in analysis_results['deaths_by_city'].items():
            report_content += f"{city}: {int(deaths)} mortes\n"
        
        report_content += f"""
2. POPULAÇÃO ESTIMADA:
{'='*40}
"""
        
        for index, row in analysis_results['population_info'].head(10).iterrows():
            report_content += f"{row['city']}: {int(row['estimated_population'])} habitantes\n"
        
        report_content += f"""
3. CIDADE COM MAIOR NÚMERO DE CASOS:
{'='*40}
{analysis_results['city_with_most_cases'][0]}: {int(analysis_results['city_with_most_cases'][1])} casos

4. CIDADE COM MENOR NÚMERO DE CASOS:
{'='*40}
{analysis_results['city_with_least_cases'][0]}: {int(analysis_results['city_with_least_cases'][1])} casos

=========================================
Relatório gerado automaticamente pelo sistema de análise de dados da COVID-19.
"""
        
        with open('relatorio_covid.txt', 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        print("Relatório gerado com sucesso!")
        return report_content
        
    except Exception as e:
        print(f"Erro ao gerar relatório: {e}")
        return None

def main():
    print("Iniciando análise de dados da COVID-19...")
    file_path = "C:/Users/davir/OneDrive/Área de Trabalho/caso_full.csv/caso_full.csv"
    df = extract_data(file_path, limit=20)
    
    if df is None:
        print("Falha ao extrair dados. Encerrando...")
        return
    
    df_clean = clean_data(df)
    
    if df_clean is None or len(df_clean) == 0:
        print("Nenhum dado válido após a limpeza. Encerrando...")
        return
    
    analysis_results = analyze_data(df_clean)
    
    if analysis_results is None:
        print("Falha ao analisar dados. Encerrando...")
        return
    
    insert_data_to_mysql(df_clean)
    
    report = generate_report(analysis_results)
    
    if report:
        print("\nRELATÓRIO GERADO:")
        print("="*50)
        print(report)
        print("="*50)
    
    print("\nProcesso concluído com sucesso!")

if __name__ == "__main__":
    main()
