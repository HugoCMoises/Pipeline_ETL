
"""
Script de ETL e Análise de Dados 
Autor: Hugo Leonardo Cardoso Moises
Descrição: Processamento, limpeza, validação e análise de consumo de energia.
Estrutura de pastas esperada:
  - /data: contém os arquivos brutos (tabela1.parquet, tabela2.csv, etc.)
  - /output: onde o relatório excel será salvo (criada automaticamente)
"""

import pandas as pd
import numpy as np
import re
import sys
import os  # Importado para gerenciar caminhos e pastas
import matplotlib.pyplot as plt
from validate_docbr import CPF, CNPJ
import pyxlsb
   

# ==============================================================================
# CONSTANTES E MAPAS GLOBAIS
# ==============================================================================

DICT_ESTADOS = {
    'distrito federal': 'DF', 'goias': 'GO', 'gois': 'GO', 'go': 'GO',
    'mato grosso': 'MT', 'mato grosso do sul': 'MS',
    'alagoas': 'AL', 'bahia': 'BA', 'ceara': 'CE', 'maranhao': 'MA', 'paraiba': 'PB',
    'pernambuco': 'PE', 'piaui': 'PI', 'rio grande do norte': 'RN', 'sergipe': 'SE',
    'acre': 'AC', 'amapa': 'AP', 'amazonas': 'AM', 'para': 'PA', 'rondonia': 'RO',
    'roraima': 'RR', 'tocantins': 'TO',
    'espirito santo': 'ES', 'minas gerais': 'MG', 'mg': 'MG',
    'rio de janeiro': 'RJ',
    'sao paulo': 'SP', 'so paulo': 'SP', 'sp': 'SP',
    'parana': 'PR', 'paran': 'PR', 'pr': 'PR',
    'rio grande do sul': 'RS', 'santa catarina': 'SC'
}

MAPA_MESES = {
    'JAN': '01', 'FEV': '02', 'MAR': '03', 'ABR': '04', 'MAI': '05', 'JUN': '06',
    'JUL': '07', 'AGO': '08', 'SET': '09', 'OUT': '10', 'NOV': '11', 'DEZ': '12'
}

COLUNAS_FINAIS_PADRAO = ['CÓDIGO', 'UF', 'MÉDIA DE POTÊNCIA (kW)', 'DATA DO DIA', 'CLASSE']

# ==============================================================================
# FUNÇÕES UTILITÁRIAS DE LIMPEZA E VALIDAÇÃO
# ==============================================================================

def normalizar_robusta(coluna_pandas):
    """Remove acentos, espaços e converte para minúsculas."""
    texto_limpo = coluna_pandas.astype(str).str.lower().str.strip()
    if hasattr(texto_limpo.str, 'normalize'):
        texto_limpo = texto_limpo.str.normalize('NFD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    return texto_limpo.str.strip()

def padronizar_cpf_cnpj(documento):
    """Limpa, formata e VALIDA matematicamente CPF ou CNPJ."""
    doc_str = str(documento)
    doc_limpo = re.sub(r'\D', '', doc_str)
    
    if not doc_limpo:
        return pd.NA

    if len(doc_limpo) <= 11:
        doc_padrao = doc_limpo.zfill(11)
        if not CPF().validate(doc_padrao): return pd.NA
        return f"{doc_padrao[0:3]}.{doc_padrao[3:6]}.{doc_padrao[6:9]}-{doc_padrao[9:11]}"
    else:
        doc_padrao = doc_limpo.zfill(14)
        if not CNPJ().validate(doc_padrao): return pd.NA
        return f"{doc_padrao[0:2]}.{doc_padrao[2:5]}.{doc_padrao[5:8]}/{doc_padrao[8:12]}-{doc_padrao[12:14]}"

def checar_cpf_cnpj_invalido(documento):
    """Auxiliar para análise exploratória: verifica se o dado parece inválido."""
    if pd.isna(documento): return True
    doc_limpo = re.sub(r'\D', '', str(documento))
    if not doc_limpo: return True
    try:
        if int(doc_limpo) == 0: return True
    except ValueError: return True
    return False

# ==============================================================================
# ETAPA 1: ANÁLISE EXPLORATÓRIA (DADOS BRUTOS)
# ==============================================================================

def executar_analise_exploratoria():
    print("\n" + "="*50)
    print("ETAPA 1: ANÁLISE EXPLORATÓRIA INICIAL (DADOS BRUTOS)")
    print("="*50)

    # Caminhos atualizados para a pasta data/
    tabelas_info = [
        ('Tabela 1 (Parquet)', "data/tabela1.parquet", {'engine': 'pyarrow'}, "CPF/CNPJ", "UF", "DATA"),
        ('Tabela 2 (CSV UTF-16)', "data/tabela2.csv", {'encoding': 'utf-16', 'sep': ';', 'header': 1, 'low_memory': False}, ["CPF", "CNPJ"], "ESTADO", "DIA"),
        ('Tabela 3 (CSV |)', "data/tabela3.csv", {'encoding': 'utf-8', 'sep': '|', 'header': None, 'names': ["CPF / CNPJ", "UF", "POTENCIA MÉDIA(kW)", "DATA", "CLASSE"], 'low_memory': False}, "CPF / CNPJ", "UF", "DATA"),
        ('Tabela 4 (JSONL)', "data/tabela4.jsonl", {'lines': True, 'encoding': 'utf-8'}, "cpf_cnpj", "estado", "data"),
        ('Tabela 5 (TXT)', "data/tabela5.txt", {'encoding': 'utf-8', 'sep': '\t'}, "CPF/CNPJ", "UF", "DATA"),
        ('Tabela 6 (XLSB)', "data/tabela6.xlsb", {'engine': 'pyxlsb', 'header': None, 'names': ['CPF/CNPJ', 'Classe', 'Potência Média (kW)', 'Dia', 'Estado']}, 'CPF/CNPJ', 'Estado', 'Dia')
    ]

    for nome, caminho, kwargs, cols_cpf_cnpj, col_uf, col_data in tabelas_info:
        print(f"\n--- Analisando: {nome} ---")
        try:
            df_temp = None
            if caminho.endswith('.parquet'): df_temp = pd.read_parquet(caminho, **kwargs)
            elif caminho.endswith('.csv') or caminho.endswith('.txt'): df_temp = pd.read_csv(caminho, **kwargs)
            elif caminho.endswith('.jsonl'): df_temp = pd.read_json(caminho, **kwargs)
            elif caminho.endswith('.xlsb'): df_temp = pd.read_excel(caminho, **kwargs)
            
            if df_temp is not None:
                total = len(df_temp)
                nulos = df_temp.isnull().any(axis=1).sum()
                print(f"Total linhas: {total} | Linhas com nulos: {nulos}")
                
                # Verifica comprimento de strings de CPF/CNPJ
                if isinstance(cols_cpf_cnpj, list):
                    for col in cols_cpf_cnpj:
                        if col in df_temp.columns:
                            print(f"Distr. Tamanho '{col}':\n{df_temp[col].astype(str).str.len().value_counts(dropna=False).sort_index().to_dict()}")
                elif cols_cpf_cnpj in df_temp.columns:
                    print(f"Distr. Tamanho '{cols_cpf_cnpj}':\n{df_temp[cols_cpf_cnpj].astype(str).str.len().value_counts(dropna=False).sort_index().to_dict()}")
                
                print(f"Amostra:\n{df_temp.head(2)}")
        except Exception as e:
            print(f"Erro na análise de {nome}: {e}")

# ==============================================================================
# ETAPA 2: PROCESSAMENTO E TRATAMENTO DAS TABELAS
# ==============================================================================

def processar_tabela1():
    print("\nProcessando Tabela 1 (Parquet)...")
    df = pd.read_parquet("data/tabela1.parquet", engine='pyarrow') # Caminho data/
    df['CLASSE'] = df['CLASSE'].astype(str).str.upper()
    df['CÓDIGO'] = df["CPF/CNPJ"].apply(padronizar_cpf_cnpj)
    df['UF'] = normalizar_robusta(df['UF']).map(DICT_ESTADOS)
    
    df['DATA'] = pd.to_datetime(df['DATA'], errors='coerce', dayfirst=True).dt.normalize()
    
    if df['POTENCIA_MEDIA'].dtype == 'object':
        df['POTENCIA_MEDIA'] = df['POTENCIA_MEDIA'].astype(str).str.replace(',', '.', regex=False)
    df['POTENCIA_MEDIA'] = pd.to_numeric(df['POTENCIA_MEDIA'], errors='coerce')
    
    df = df.dropna(subset=['UF', 'CÓDIGO', 'DATA', 'POTENCIA_MEDIA'])
    return df.rename(columns={"DATA": "DATA DO DIA", "POTENCIA_MEDIA": "MÉDIA DE POTÊNCIA (kW)"})[COLUNAS_FINAIS_PADRAO]

def processar_tabela2():
    print("Processando Tabela 2 (CSV UTF-16)...")
    df = pd.read_csv("data/tabela2.csv", encoding='utf-16', sep=';', low_memory=False, header=1) # Caminho data/
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df['CPF/CNPJ'] = df['CPF'].fillna(df['CNPJ'])
    df = df.dropna(how='all')
    
    df['CLASSE'] = df['CLASSE'].astype(str).str.upper()
    df['CÓDIGO'] = df["CPF/CNPJ"].apply(padronizar_cpf_cnpj)
    df['UF'] = normalizar_robusta(df['ESTADO']).map(DICT_ESTADOS)
    
    df['DIA'] = pd.to_datetime(df['DIA'], errors='coerce', dayfirst=True).dt.normalize()
    
    col_pot = 'POTENCIA MÉDIA (kW)'
    if df[col_pot].dtype == 'object':
        df[col_pot] = df[col_pot].astype(str).str.replace(',', '.', regex=False)
    df[col_pot] = pd.to_numeric(df[col_pot], errors='coerce')
    
    df = df.dropna(subset=['UF', 'CÓDIGO', 'DIA', col_pot])
    return df.rename(columns={"DIA": "DATA DO DIA", col_pot: "MÉDIA DE POTÊNCIA (kW)"})[COLUNAS_FINAIS_PADRAO]

def processar_tabela3():
    print("Processando Tabela 3 (CSV |)...")
    cols = ["CPF / CNPJ", "UF", "POTENCIA MÉDIA(kW)", "DATA", "CLASSE"]
    df = pd.read_csv("data/tabela3.csv", encoding='utf-8', sep='|', encoding_errors='ignore', header=None, names=cols) # Caminho data/
    df = df.dropna(how='all')
    
    df['CLASSE'] = df['CLASSE'].astype(str).str.upper()
    df['CÓDIGO'] = df["CPF / CNPJ"].apply(padronizar_cpf_cnpj)
    df['UF'] = normalizar_robusta(df['UF']).map(DICT_ESTADOS)
    
    df['DATA'] = pd.to_datetime(df['DATA'], errors='coerce', dayfirst=True).dt.normalize()
    
    col_pot = 'POTENCIA MÉDIA(kW)'
    if df[col_pot].dtype == 'object':
        df[col_pot] = df[col_pot].astype(str).str.replace(',', '.', regex=False)
    df[col_pot] = pd.to_numeric(df[col_pot], errors='coerce')
    
    df = df.dropna(subset=['UF', 'CÓDIGO', 'DATA', col_pot])
    return df.rename(columns={"DATA": "DATA DO DIA", col_pot: "MÉDIA DE POTÊNCIA (kW)"})[COLUNAS_FINAIS_PADRAO]

def processar_tabela4():
    print("Processando Tabela 4 (JSONL)...")
    df = pd.read_json("data/tabela4.jsonl", encoding='utf-8', lines=True) # Caminho data/
    df = df.dropna(how='all')
    
    df['CLASSE'] = df['tipo_classe'].astype(str).str.upper()
    df['CÓDIGO'] = df['cpf_cnpj'].apply(padronizar_cpf_cnpj)
    df['UF'] = normalizar_robusta(df['estado']).map(DICT_ESTADOS)
    
    df['data'] = pd.to_datetime(df['data'], errors='coerce', dayfirst=True).dt.normalize()
    
    if df['pot'].dtype == 'object':
        df['pot'] = df['pot'].astype(str).str.replace(',', '.', regex=False)
    df['pot'] = pd.to_numeric(df['pot'], errors='coerce')
    
    df = df.dropna(subset=['UF', 'CÓDIGO', 'data', 'pot'])
    return df.rename(columns={"data": "DATA DO DIA", "pot": "MÉDIA DE POTÊNCIA (kW)"})[COLUNAS_FINAIS_PADRAO]

def processar_tabela5():
    print("Processando Tabela 5 (TXT)...")
    df = pd.read_csv("data/tabela5.txt", encoding='utf-8', sep='\t') # Caminho data/
    df = df.dropna(how='all')
    
    df['CLASSE'] = df['CLASSE'].astype(str).str.upper()
    df['CÓDIGO'] = df["CPF/CNPJ"].apply(padronizar_cpf_cnpj)
    df['UF'] = normalizar_robusta(df['UF']).map(DICT_ESTADOS)
    
    data_str = df['DATA'].astype(str).str.upper().str.replace('/', '-', regex=False)
    for k, v in MAPA_MESES.items():
        data_str = data_str.str.replace(k, v, regex=False)
    df['DATA'] = pd.to_datetime(data_str, errors='coerce', dayfirst=True).dt.normalize()
    
    if df['POTENCIA_MEDIA'].dtype == 'object':
        df['POTENCIA_MEDIA'] = df['POTENCIA_MEDIA'].astype(str).str.replace(',', '.', regex=False)
    df['POTENCIA_MEDIA'] = pd.to_numeric(df['POTENCIA_MEDIA'], errors='coerce')
    
    df = df.dropna(subset=['UF', 'CÓDIGO', 'DATA', 'POTENCIA_MEDIA'])
    return df.rename(columns={"DATA": "DATA DO DIA", "POTENCIA_MEDIA": "MÉDIA DE POTÊNCIA (kW)"})[COLUNAS_FINAIS_PADRAO]

def processar_tabela6():
    print("Processando Tabela 6 (XLSB)...")
    cols = ['CPF/CNPJ', 'Classe', 'Potência Média (kW)', 'Dia', 'Estado']
    df = pd.read_excel("data/tabela6.xlsb", engine='pyxlsb', header=None, names=cols) # Caminho data/
    df = df.dropna(how='all')
    
    df['CLASSE'] = df['Classe'].astype(str).str.upper()
    df['CÓDIGO'] = df['CPF/CNPJ'].apply(padronizar_cpf_cnpj)
    df['UF'] = normalizar_robusta(df['Estado']).map(DICT_ESTADOS)
    
    df['Dia'] = pd.to_datetime(df['Dia'], errors='coerce', dayfirst=True).dt.normalize()
    
    col_pot = 'Potência Média (kW)'
    if df[col_pot].dtype == 'object':
        df[col_pot] = df[col_pot].astype(str).str.replace(',', '.', regex=False)
    df[col_pot] = pd.to_numeric(df[col_pot], errors='coerce')
    
    df = df.dropna(subset=['UF', 'CÓDIGO', 'Dia', col_pot])
    return df.rename(columns={"Dia": "DATA DO DIA", col_pot: "MÉDIA DE POTÊNCIA (kW)"})[COLUNAS_FINAIS_PADRAO]

# ==============================================================================
# ETAPA 3: UNIFICAÇÃO E CÁLCULOS
# ==============================================================================

def unificar_e_calcular_custos(lista_dfs):
    print("\n" + "="*50)
    print("ETAPA 3: UNIFICAÇÃO E CÁLCULO DE CUSTO")
    print("="*50)
    
    # Filtra DataFrames válidos
    validos = [d for d in lista_dfs if isinstance(d, pd.DataFrame) and not d.empty]
    
    if not validos:
        print("ERRO: Nenhum dado válido para processar.")
        return pd.DataFrame()
    
    # Unifica
    print(f"Unificando {len(validos)} tabelas...")
    df_final = pd.concat(validos, ignore_index=True)
    print(f"Linhas totais: {len(df_final)}")
    
    # Remove duplicatas
    df_final = df_final.drop_duplicates(subset=['CÓDIGO', 'DATA DO DIA'], keep='first').copy()
    print(f"Linhas após deduplicação: {len(df_final)}")
    
    # Calcula Custo
    print("Calculando coluna CUSTO...")
    energia_dia = df_final['MÉDIA DE POTÊNCIA (kW)'] * 24
    
    condicoes = [
        (df_final['CLASSE'] == 'A1'),
        (df_final['CLASSE'] == 'A2'),
        (df_final['CLASSE'] == 'B1'),
        (df_final['CLASSE'] == 'B2'),
        (df_final['CLASSE'].isin(['C1', 'C2'])) & (df_final['MÉDIA DE POTÊNCIA (kW)'] > 50),
        (df_final['CLASSE'].isin(['C1', 'C2'])),
        (df_final['CLASSE'] == 'D1'),
        (df_final['CLASSE'] == 'D2')
    ]
    
    tarifas = [0.7, 0.7, 0.6, 0.3, 1.5, 0.5, 1.0, 1.0]
    
    tarifa_aplicada = np.select(condicoes, tarifas, default=np.nan)
    df_final.loc[:, 'CUSTO'] = energia_dia * tarifa_aplicada
    
    print("Cálculo concluído. Amostra da Tabela Final com Custo:")
    print(df_final.head()) 
    
    return df_final

# ==============================================================================
# ETAPA 4: RELATÓRIOS E SAÍDAS
# ==============================================================================

def gerar_grafico(df_final):
    print("\nGerando Gráfico de Série Temporal...")
    if df_final.empty or 'CUSTO' not in df_final.columns: return

    df_plot = df_final.groupby([pd.Grouper(key='DATA DO DIA', freq='MS'), 'UF'])['CUSTO'].sum().unstack(level='UF')
    
    fig, ax = plt.subplots(figsize=(16, 9))
    df_plot.plot(ax=ax)
    
    ax.set_title('Soma Mensal de Custo por UF (Série Temporal)', fontsize=16)
    ax.set_xlabel('Mês/Ano', fontsize=12)
    ax.set_ylabel('Custo Total Mensal (R$)', fontsize=12)
    ax.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, p: f'R$ {x:,.0f}'.replace(',', '.')))
    ax.legend(title='UF', bbox_to_anchor=(1.02, 1), loc='upper left')
    ax.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout(rect=[0, 0, 0.85, 1])
    
    print("Gráfico gerado. Uma janela será aberta.")
    plt.show()

def gerar_excel(df_final):
    print("\nGerando Relatório Excel...")
    
    # Garante que a pasta output existe
    pasta_saida = "output"
    if not os.path.exists(pasta_saida):
        os.makedirs(pasta_saida)
        
    nome_arq = os.path.join(pasta_saida, "relatorio_custos_anuais.xlsx")
    
    if df_final.empty: return

    try:
        df_final['ANO'] = df_final['DATA DO DIA'].dt.year
        anos = sorted(df_final['ANO'].unique())
        
        with pd.ExcelWriter(nome_arq, engine='xlsxwriter') as writer:
            for ano in anos:
                df_ano = df_final[df_final['ANO'] == ano]
                df_resumo = df_ano.groupby('CÓDIGO')['CUSTO'].sum().reset_index()
                df_resumo.rename(columns={'CÓDIGO': 'CPF/CNPJ', 'CUSTO': f'Custo Total {ano}'}, inplace=True)
                df_resumo.sort_values(by=f'Custo Total {ano}', ascending=False, inplace=True)
                
                nome_aba = str(ano)
                df_resumo.to_excel(writer, sheet_name=nome_aba, index=False)
                
                # Formatação
                wb = writer.book
                ws = writer.sheets[nome_aba]
                fmt_moeda = wb.add_format({'num_format': 'R$ #,##0.00'})
                fmt_header = wb.add_format({'bold': True, 'bg_color': '#D9E1F2', 'border': 1, 'align': 'center'})
                
                for i, col in enumerate(df_resumo.columns):
                    ws.write(0, i, col, fmt_header)
                ws.set_column('A:A', 30)
                ws.set_column('B:B', 20, fmt_moeda)
                ws.freeze_panes(1, 0)
                
        print(f"Arquivo '{nome_arq}' salvo com sucesso.")
        df_final.drop('ANO', axis=1, inplace=True)
    except Exception as e:
        print(f"Erro ao gerar Excel: {e}")

def responder_perguntas(df_final):
    print("\n" + "="*50)
    print("PERGUNTAS E RESPOSTAS (Custo Médio por Grupo)")
    print("="*50)
    
    mapa_grupos = {
        'A1': 'Grupo A', 'A2': 'Grupo A',
        'B1': 'Grupo B', 'B2': 'Grupo B',
        'C1': 'Grupo C', 'C2': 'Grupo C',
        'D1': 'Grupo D', 'D2': 'Grupo D'
    }
    
    df_final['GRUPO_CLASSE'] = df_final['CLASSE'].map(mapa_grupos)
    df_analise = df_final.dropna(subset=['GRUPO_CLASSE'])
    
    if df_analise.empty:
        print("Sem dados para análise de grupos.")
        return

    media_custo = df_analise.groupby(['GRUPO_CLASSE', 'UF'])['CUSTO'].mean()
    
    def fmt(v): return f'R$ {v:,.2f}'.replace(",", "X").replace(".", ",").replace("X", ".")

    for grupo in ['Grupo A', 'Grupo B', 'Grupo C', 'Grupo D']:
        try:
            dados_g = media_custo.loc[grupo]
            uf_max = dados_g.idxmax()
            val_max = dados_g.max()
            print(f"Maior média {grupo}: {uf_max} ({fmt(val_max)})")
        except KeyError:
            print(f"{grupo}: Sem dados.")
            
    df_final.drop('GRUPO_CLASSE', axis=1, inplace=True, errors='ignore')

# ==============================================================================
# EXECUÇÃO PRINCIPAL
# ==============================================================================

def main():
    # 1. Exploração
    executar_analise_exploratoria()
    
    # 2. Processamento Individual
    try:
        t1 = processar_tabela1()
        t2 = processar_tabela2()
        t3 = processar_tabela3()
        t4 = processar_tabela4()
        t5 = processar_tabela5()
        t6 = processar_tabela6()
    except Exception as e:
        print(f"\nERRO CRÍTICO no processamento das tabelas: {e}")
        print("Verifique se a pasta 'data/' existe e contém todos os arquivos.")
        return

    # 3. Unificação
    df_completo = unificar_e_calcular_custos([t1, t2, t3, t4, t5, t6])
    
    # 4. Saídas
    if not df_completo.empty:
        gerar_grafico(df_completo)
        gerar_excel(df_completo)
        responder_perguntas(df_completo)
    else:
        print("Processo encerrado sem dados finais.")

if __name__ == "__main__":
    main()