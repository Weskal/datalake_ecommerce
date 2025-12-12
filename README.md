# Projeto Ecommerce - Eng. de Dados

<p>Esse projeto tem o propósito de criar uma simulação de sistema de Ecommerce, com dados fakes gerados, ingestões, transformações e visualizações em BI</p>

Principais tecnologias:
- Python
- Apache Airflow
- Minio (para simular o S3 localmente)
- Postgres
- BigQuery - Em desenvolvimento
- Power BI - Em desenvolvimento
- Machine Learning - Em desenvolvimento

### Setup

3. Esquema da tabela para atualizar o status de cada pipeline

´´´
CREATE TABLE PIPELINE_STATUS (
	pipeline_id int primary key not null,
	pipeline_name varchar(100) not null,
	status varchar(30) null,
	notes varchar(1000) null,
	frequence varchar(50) not null,
	last_run_duration float null,
	updated_at timestamp null
);
´´´

