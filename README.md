# Databricks-Professional

Este material de estudo deriva-se do fornecido pelo Dear-alhussein com algumas adaptações e anotações. 
Link do repositório original: https://github.com/derar-alhussein


# Databricks Certified Data Engineer Professional

<img align="left" role="left" src="https://img-c.udemycdn.com/course/240x135/5125510_214e_3.jpg" width="180" alt="Databricks Certified Data Engineer Professional -Preparation" />
This repository contains the resources of the preparation course for Databricks Data Engineer Professional certification exam on Udemy:
<br/>
<a href="https://www.udemy.com/course/databricks-certified-data-engineer-professional/?referralCode=936CBDC941031CE4D795" target="_blank">https://www.udemy.com/course/databricks-certified-data-engineer-professional/?referralCode=936CBDC941031CE4D795</a>.
<br/>
<br/>


## Practice Exams

<img align="left" role="left" src="https://img-c.udemycdn.com/course/240x135/5317104_98d4.jpg" width="180" alt="Practice Exams: Databricks Data Engineer Professional" />
Practice exams for this certification are available in the following Udemy course:
<br/>
<a href="https://www.udemy.com/course/practice-exams-databricks-data-engineer-professional-k/?referralCode=36812341C732CD1E6D5A" target="_blank">https://www.udemy.com/course/practice-exams-databricks-data-engineer-professional-k/?referralCode=36812341C732CD1E6D5A</a>.<br/>

## Conteúdo

|Tópico|Conceito Chave|Sintaxe/Comando (SQL/Python)|
| :- | :- | :- |
|APIs de Auto CDC|Simplificação de Processamento CDC|CREATE FLOW ou APPLY CHANGES INTO|
|Lakeflow Pipelines (LDP)|Framework Declarativo de ETL|CREATE OR REFRESH STREAMING TABLE|
|Expectations (LDP)|Controle de Qualidade de Dados|CONSTRAINT  EXPECT (<condição>) ON VIOLATION <ação>|
|Liquid Clustering|Otimização Flexível de Layout|CLUSTER BY (col1, col2)|
|Auto Optimize|Compactação Automática|delta.autoOptimize.autoCompact = true|
|Particionamento Delta|Subdivisão Física de Dados|PARTITIONED BY (col\_name)|
|Delta Sharing|Compartilhamento Seguro|CREATE SHARE <nome\_do\_share>|
|Informações do Exame|Detalhes Administrativos|-|



|Descrição dos Detalhes|Observações de Certificação|Fonte|
| :- | :- | :- |
|Captura mudanças de dados (CDC) de forma declarativa, suportando SCD Tipo 1 e Tipo 2. Substitui a necessidade de instruções MERGE INTO manuais.|O comando SEQUENCE BY é essencial para tratar registros fora de ordem. A sintaxe CREATE FLOW é a evolução da APPLY CHANGES INTO.|1|
|Evolução do Delta Live Tables (DLT). Gerencia automaticamente checkpoints, retentativas e a orquestração de dependências do pipeline.|LDP permite criar tabelas de streaming via SQL declarativo, facilitando a automação em comparação ao Spark Structured Streaming puro.|2|
|Define regras de integridade. As ações incluem reter (padrão/Warn), descartar (DROP ROW) ou interromper a carga (FAIL UPDATE).|Por padrão, se nenhuma ação de violação for definida, o registro é mantido e a métrica é registrada no log de eventos.|3|
|Substitui o antigo Z-ORDER. Permite a reorganização física dos dados e a alteração das chaves de clusterização sem a necessidade de reescrever a tabela completa.|Incompatível com particionamento tradicional (Hive-style) ou ZORDER na mesma tabela. Oferece melhor desempenho em filtros multidimensionais.|4|
|Combina Optimized Writes (gravação de arquivos de 128 MB) e Auto Compaction para reduzir o problema de "small files" durante a escrita.|Focado em reduzir a latência de futuras operações de MERGE. Não realiza o agrupamento multidimensional que o Z-Ordering provê.|5|
|Organiza os dados em subdiretórios baseados em colunas de baixa cardinalidade para otimizar o skipping de arquivos (data skipping).|Recomendado apenas para partições com volume superior a 1 GB. O excesso de partições (over-partitioning) pode prejudicar a performance.|6|
|Protocolo aberto para compartilhamento de dados em tempo real sem a necessidade de cópia física entre diferentes plataformas ou workspaces.|Suporta modelos Databricks-to-Databricks (D2D) e acesso aberto via destinatário externo com tokens de segurança (D2O).|7|
|Exame composto por 60 questões com duração de 120 minutos. Taxa de aprovação mínima de 70% (42 questões corretas). Custo de $200.|Domínios principais: Processamento de Dados (30%) e Modelagem de Dados (20%). Realizado via plataforma Webassessor.|8|
