# ⚡ ETL Pipeline – Consumo de Energia  
### *ScriptETL.py — Desenvolvido por Hugo Moisés*

![Python](https://img.shields.io/badge/Python-3.10+-blue?style=for-the-badge&logo=python)
![Pandas](https://img.shields.io/badge/Pandas-Processing-blueviolet?style=for-the-badge&logo=pandas)
![Matplotlib](https://img.shields.io/badge/Matplotlib-Visualization-red?style=for-the-badge&logo=matplotlib)
![Status](https://img.shields.io/badge/Status-Concluído-brightgreen?style=for-the-badge)
![License](https://img.shields.io/badge/License-MIT-yellow?style=for-the-badge)

---

## 📌 Sobre o Projeto  
Este repositório contém o **pipeline ETL oficial para tratamento e consolidação de dados de consumo de energia**.

O script principal, **ScriptETL.py**, realiza:

- Extração de múltiplos arquivos (CSV, XLSB, Parquet, TXT, JSONL)  
- Padronização e validação de CPF e CNPJ  
- Normalização de datas e UF  
- Tratamento e unificação de dados heterogêneos  
- Cálculo de custos por consumidor  
- Geração de dashboards e relatórios analíticos  
- Exportação profissional para Excel (planilhas anuais)

O objetivo é entregar um dataset confiável, limpo e padronizado para análise energética.

---

## 🎯 Objetivo  
- Processar e padronizar diferentes bases já incluídas no repositório  
- Unificar informações para geração de análises e relatórios  
- Automatizar a validação de documentos (CPF/CNPJ)  
- Realizar cálculos tarifários e criação de novas métricas  
- Gerar arquivos finais em formato Excel + gráficos informativos  

---

## 🧠 Principais Funcionalidades  

### ✔ Processamento unitário de cada tabela  
Cada arquivo é tratado com regras específicas conforme seu formato:
- CSV, TXT → leitura padrão  
- XLSB → leitura via `pyxlsb`  
- Parquet → leitura nativa via Pandas  
- JSONL → leitura linha a linha  
- Conversão robusta de datas  
- Limpeza de colunas e normalização textual

### ✔ Funções utilitárias customizadas  
- Padronização de UF (com dicionário robusto)  
- Padronização e validação de CPF/CNPJ  
- Função universal de normalização textual  
- Conversor de datas com múltiplos formatos  
- Tratamento de dados faltantes e inconsistentes  

### ✔ Unificação final  
- Concatenação e deduplicação das tabelas  
- Cálculo de custo diário e mensal  
- Criação de novas colunas derivadas  
- Agrupamento de consumo por UF e ano  

### ✔ Relatórios Automatizados  
- 📊 Excel anual com custos por CPF/CNPJ  
- 📈 Gráfico temporal por estado  
- 📚 Resumo estatístico por grupos tarifários (A, B, C, D)

---

## 📂 Arquitetura do Repositório  

├── ScriptETL.py
├── requirements.txt
├── data/
│ ├── tabela1.parquet
│ ├── tabela2.csv
│ ├── tabela3.csv
│ ├── tabela4.jsonl
│ ├── tabela5.txt
│ └── tabela6.xlsb
└── output/
└── relatorio_custos_anuais.xlsx (gerado automaticamente)

---

## ▶️ Como Executar o Script  

### Instale as dependências (arquivo já incluso):
pip install -r requirements.txt

Execute o script:
python ScriptETL.py

Veja o relatório gerado:
/output/relatorio_custos_anuais.xlsx

📊 Exemplos de Resultados

Dataset final limpo e padronizado

Análise temporal de custo por UF

Relatórios anuais organizados em abas

Agrupamentos tarifários com insights

Dataset pronto para BI ou análises estatísticas





🤝 Autor

Hugo Leonardo Cardoso Moisés
Desenvolvedor de Software • QA • Analista de Dados
📧 hugoleonardomoises@gmail.com

🔗 linkedin.com/in/hugo-moises
