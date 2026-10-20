# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_bronze_ingest_olist
#
# Lê os 9 CSVs do [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
# de `lh_bronze/Files/landing/olist/` e grava uma tabela Delta por arquivo em `lh_bronze.olist.<entidade>`.
#
# Regras da Bronze:
# - **Fiel à origem**: todas as colunas como `string`, nomes da origem, nenhuma linha descartada.
# - **Snapshot por lote**: cada execução acrescenta o lote inteiro, particionado por `dp_ingestion_date`.
#   Reexecutar o mesmo `batch_id` substitui só aquele lote (`replaceWhere`), então a carga é idempotente.
# - **Validação só estrutural**: arquivo existe, tem linhas e tem as colunas esperadas.

# PARAMETERS CELL ********************

batch_id = ""                      # o pipeline envia @pipeline().RunId
landing_folder = "landing/olist"   # pasta em lh_bronze/Files
download_from_kaggle = False       # True baixa o ZIP pela API do Kaggle antes de ingerir
key_vault_url = ""                 # ex.: https://kv-portfolio.vault.azure.net/ (segredos kaggle-username e kaggle-key)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_fw_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

NOTEBOOK_NAME = "nb_bronze_ingest_olist"
batch_id = new_batch_id(batch_id)
started_ts = datetime.now(timezone.utc)

# arquivo de origem -> (tabela Bronze, colunas esperadas)
SOURCE_FILES = {
    "olist_orders_dataset.csv": ("orders", [
        "order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"]),
    "olist_order_items_dataset.csv": ("order_items", [
        "order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value"]),
    "olist_order_payments_dataset.csv": ("order_payments", [
        "order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"]),
    "olist_order_reviews_dataset.csv": ("order_reviews", [
        "review_id", "order_id", "review_score", "review_comment_title", "review_comment_message",
        "review_creation_date", "review_answer_timestamp"]),
    "olist_customers_dataset.csv": ("customers", [
        "customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"]),
    "olist_sellers_dataset.csv": ("sellers", [
        "seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"]),
    "olist_products_dataset.csv": ("products", [
        "product_id", "product_category_name", "product_name_lenght", "product_description_lenght",
        "product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]),
    "olist_geolocation_dataset.csv": ("geolocation", [
        "geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng", "geolocation_city", "geolocation_state"]),
    "product_category_name_translation.csv": ("product_category_name_translation", [
        "product_category_name", "product_category_name_english"]),
}
print(f"batch_id = {batch_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Opcional: baixar do Kaggle
#
# O dataset tem licença CC BY-NC-SA 4.0 e **não é versionado neste repositório**. O caminho padrão é subir os CSVs
# para `Files/landing/olist/` pela interface do lakehouse ou pelo OneLake file explorer. Com `download_from_kaggle = True`,
# o notebook busca as credenciais no Azure Key Vault (nunca em texto no código) e baixa o ZIP pela API do Kaggle.

# CELL ********************

if download_from_kaggle:
    import io
    import zipfile
    import requests

    if not key_vault_url:
        raise ValueError("Informe key_vault_url para baixar do Kaggle.")
    kaggle_user = notebookutils.credentials.getSecret(key_vault_url, "kaggle-username")
    kaggle_key = notebookutils.credentials.getSecret(key_vault_url, "kaggle-key")
    response = requests.get(
        "https://www.kaggle.com/api/v1/datasets/download/olistbr/brazilian-ecommerce",
        auth=(kaggle_user, kaggle_key), timeout=300,
    )
    response.raise_for_status()

    mount_point = "/mnt/olist_landing"
    notebookutils.fs.mount(files_path(LH_BRONZE, landing_folder), mount_point)
    local_folder = notebookutils.fs.getMountPath(mount_point)
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(local_folder)
    notebookutils.fs.unmount(mount_point)
    print(f"ZIP extraído em Files/{landing_folder}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Ingestão

# CELL ********************

landing_files = {f.name for f in notebookutils.fs.ls(files_path(LH_BRONZE, landing_folder))}
missing = sorted(set(SOURCE_FILES) - landing_files)
if missing:
    raise FileNotFoundError(f"Arquivos ausentes em Files/{landing_folder}: {missing}")

summary = []
for file_name, (table_name, expected_columns) in SOURCE_FILES.items():
    raw = (
        spark.read
        .option("header", "true")
        .option("multiLine", "true")   # comentários das avaliações têm quebra de linha
        .option("quote", '"')
        .option("escape", '"')
        .option("encoding", "UTF-8")
        .option("inferSchema", "false")  # Bronze guarda texto; a tipagem é decidida na Silver
        .csv(files_path(LH_BRONZE, f"{landing_folder}/{file_name}"))
    )

    # alguns CSVs do Kaggle começam com BOM, que vira parte do nome da primeira coluna
    header = [c.lstrip("﻿").strip() for c in raw.columns]
    absent = [c for c in expected_columns if c not in header]
    if absent:
        raise ValueError(f"{file_name}: colunas esperadas ausentes {absent}")

    bronze = (
        raw.select(
            *[F.col(f"`{original}`").alias(clean) for original, clean in zip(raw.columns, header)
              if clean in expected_columns],
            F.col("_metadata.file_path").alias("dp_source_file"),
        )
        .withColumn("dp_inserted_ts", F.current_timestamp())
        .withColumn("dp_ingestion_date", F.current_date())
        .withColumn("dp_source_system", F.lit("olist"))
        .withColumn("dp_batch_id", F.lit(batch_id))
    )

    (
        bronze.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"dp_batch_id = '{batch_id}'")
        .partitionBy("dp_ingestion_date")
        .save(table_path(LH_BRONZE, "olist", table_name))
    )

    row_count = read_table(LH_BRONZE, "olist", table_name).where(F.col("dp_batch_id") == batch_id).count()
    if row_count == 0:
        raise ValueError(f"{file_name}: arquivo sem linhas")
    log_run(batch_id, NOTEBOOK_NAME, "bronze", f"olist.{table_name}", "succeeded", row_count, started_ts)
    summary.append((table_name, row_count))

display(spark.createDataFrame(summary, "table_name string, row_count long"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.notebook.exit(batch_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
