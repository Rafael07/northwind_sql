# ETL pipelines para extração de dados de diferentes fontes e ingestão em banco Postgres em container

Esse projeto utiliza do famoso banco Northwind para extrair suas tabelas de fontes diversas (um bd incompleto Postgres dockerizado e uma planilha como complemento), armazenar no file system obedencendo uma regra de organização conforme a data da ingestão, para, por fim, fazer a carga de todas as tabelas salvas em um banco target contido num container.

O projeto se encontra em andamento, com diversas melhorias por vir:

- criação de testes unitários;
- orquestração com airflow;
- documentação com mkdocs;
- criação de tabelas e views com a lógica de negócio, para dispor na camada gold;
- dashboards para atender as demandas dos times de negócios;