// Databricks notebook source
// MAGIC %md
// MAGIC Verificando arquivos na tabela inbound

// COMMAND ----------

display(dbutils.fs.ls("/mnt/dados/inbound"))

// COMMAND ----------

// MAGIC %md
// MAGIC Lendo dados da Camada Inbound

// COMMAND ----------

val path = "dbfs:/mnt/dados/inbound/dados_brutos_imoveis.json"
val dados = spark.read.json(path)

// COMMAND ----------

display(dados)

// COMMAND ----------

// MAGIC %md Removendo colunas

// COMMAND ----------

val dados_anuncio = dados.drop("imagens","usuario")
display (dados_anuncio)

// COMMAND ----------

// MAGIC %md Criando uma coluna de identificação

// COMMAND ----------

import org.apache.spark.sql.functions.col

// COMMAND ----------

val df_bronze = dados_anuncio.withColumn("id", col("anuncio.id"))
display(df_bronze)

// COMMAND ----------

// MAGIC %md Salvando na camada bronze

// COMMAND ----------

val path = "/mnt/dados/bronze/dataset_imoveis"
// colocando o comando SaveMode.Overwrite o arquivo será sempre subscrito
df_bronze.write.format("delta").mode(SaveMode.Overwrite).save(path)

// COMMAND ----------


