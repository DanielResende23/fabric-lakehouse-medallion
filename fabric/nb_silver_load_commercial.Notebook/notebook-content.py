# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_silver_load_commercial
# # Bronze (`lh_bronze.olist.*`, texto bruto) → Silver (`lh_silver.commercial.*`, limpo e tipado).
# # | Silver | Origem Bronze | Chave natural | Tratamentos principais |
# |---|---|---|---|
# | `customer` | `customers` | `customer_id` | CEP com 5 dígitos, cidade normalizada, UF maiúscula |
# | `seller` | `sellers` | `seller_id` | idem |
# | `product` | `products` + `product_category_name_translation` | `product_id` | nomes de coluna corrigidos (`lenght`), categoria em inglês com fallback |
# | `sales_order` | `orders` | `order_id` | timestamps tipados, domínio de status |
# | `sales_order_item` | `order_items` | `order_id`, `order_item_number` | valores `DECIMAL(18,4)`, não negativos |
# | `order_payment` | `order_payments` | `order_id`, `payment_sequential` | domínio de forma de pagamento |
# | `order_review` | `order_reviews` | `order_id` | **uma avaliação por pedido** (a respondida por último) |
# | `zip_code_location` | `geolocation` | `zip_code_prefix` | 1 linha por CEP (média de lat/long), coordenadas fora do Brasil em quarentena |
# # Tipos: valores `DECIMAL(18,4)` (18 e não 19 dígitos: limite do tipo Decimal do modelo Direct Lake), instantes `TIMESTAMP` (`_ts`), datas `DATE` (`_date`).

# PARAMETERS CELL ********************

batch_id = ""   # vazio = último lote da Bronze

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

NOTEBOOK_NAME = "nb_silver_load_commercial"
started_ts = datetime.now(timezone.utc)

if not batch_id:
    batch_id = (
        read_table(LH_BRONZE, "olist", "orders")
        .orderBy(F.col("dp_inserted_ts").desc())
        .select("dp_batch_id").first()[0]
    )
print(f"batch_id = {batch_id}")

# Um batch_id que a Bronze não gravou (ex.: rerun do pipeline com RunId novo) para aqui, antes de qualquer MERGE.
BRONZE_TABLES = ["orders", "order_items", "order_payments", "order_reviews", "customers", "sellers",
                 "products", "geolocation", "product_category_name_translation"]
empty = [t for t in BRONZE_TABLES
         if read_table(LH_BRONZE, "olist", t).where(F.col("dp_batch_id") == batch_id).isEmpty()]
if empty:
    raise ValueError(f"Lote {batch_id} sem linhas na Bronze para {empty}. Rode o Bronze ou informe batch_id vazio.")


def bronze(table: str) -> DataFrame:
    """Lote atual da tabela Bronze, sem as colunas técnicas da Bronze."""
    df = read_table(LH_BRONZE, "olist", table).where(F.col("dp_batch_id") == batch_id)
    return df.drop(*[c for c in df.columns if c.startswith("dp_")])


def to_ts(column: str):
    return F.to_timestamp(clean_string(column), "yyyy-MM-dd HH:mm:ss")


def to_amount(column: str):
    return clean_string(column).cast("decimal(18,4)")


def to_int(column: str):
    # alguns inteiros chegam como "1.0"
    return clean_string(column).cast("double").cast("int")


def zip_prefix(column: str):
    return F.lpad(clean_string(column), 5, "0")


def publish(df: DataFrame, table: str, keys: list, rules: list) -> int:
    """Colunas técnicas -> regras de DQ -> deduplicação conferida -> MERGE na Silver -> run log."""
    audited = add_row_hash(add_audit_columns(df, batch_id))
    valid = apply_dq(audited, table, rules, batch_id)
    assert_unique(valid, keys, f"silver.commercial.{table}")
    row_count = upsert_table(valid, LH_SILVER, "commercial", table, keys)
    log_run(batch_id, NOTEBOOK_NAME, "silver", f"commercial.{table}", "succeeded", row_count, started_ts)
    print(f"commercial.{table}: {row_count} linhas")
    return row_count

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Pedidos, itens, pagamentos e avaliações

# CELL ********************

ORDER_STATUS = ["created", "approved", "invoiced", "processing", "shipped", "delivered", "canceled", "unavailable"]
PAYMENT_TYPES = ["credit_card", "boleto", "voucher", "debit_card", "not_defined"]

sales_order = bronze("orders").select(
    clean_string("order_id").alias("order_id"),
    clean_string("customer_id").alias("customer_id"),
    F.lower(clean_string("order_status")).alias("order_status"),
    to_ts("order_purchase_timestamp").alias("purchase_ts"),
    to_ts("order_approved_at").alias("approved_ts"),
    to_ts("order_delivered_carrier_date").alias("delivered_carrier_ts"),
    to_ts("order_delivered_customer_date").alias("delivered_customer_ts"),
    F.to_date(to_ts("order_estimated_delivery_date")).alias("estimated_delivery_date"),
)
publish(sales_order, "sales_order", ["order_id"], [
    DqRule("order_id_not_null", "order_id IS NOT NULL"),
    DqRule("customer_id_not_null", "customer_id IS NOT NULL"),
    DqRule("purchase_ts_not_null", "purchase_ts IS NOT NULL"),
    DqRule("order_status_domain", f"order_status IN ({', '.join(repr(s) for s in ORDER_STATUS)})"),
    DqRule("delivered_has_delivery_ts", "order_status <> 'delivered' OR delivered_customer_ts IS NOT NULL", "warn"),
    DqRule("approved_after_purchase", "approved_ts >= purchase_ts", "warn"),
    DqRule("delivered_after_purchase", "delivered_customer_ts >= purchase_ts", "warn"),
])

sales_order_item = bronze("order_items").select(
    clean_string("order_id").alias("order_id"),
    to_int("order_item_id").alias("order_item_number"),
    clean_string("product_id").alias("product_id"),
    clean_string("seller_id").alias("seller_id"),
    to_ts("shipping_limit_date").alias("shipping_limit_ts"),
    to_amount("price").alias("item_price_amount"),
    to_amount("freight_value").alias("freight_amount"),
)
publish(sales_order_item, "sales_order_item", ["order_id", "order_item_number"], [
    DqRule("keys_not_null", "order_id IS NOT NULL AND order_item_number IS NOT NULL"),
    DqRule("product_seller_not_null", "product_id IS NOT NULL AND seller_id IS NOT NULL"),
    DqRule("price_not_negative", "item_price_amount >= 0"),
    DqRule("freight_not_negative", "freight_amount >= 0"),
    DqRule("price_positive", "item_price_amount > 0", "warn"),
])

order_payment = bronze("order_payments").select(
    clean_string("order_id").alias("order_id"),
    to_int("payment_sequential").alias("payment_sequential"),
    F.lower(clean_string("payment_type")).alias("payment_type"),
    to_int("payment_installments").alias("installment_count"),
    to_amount("payment_value").alias("payment_amount"),
)
publish(order_payment, "order_payment", ["order_id", "payment_sequential"], [
    DqRule("keys_not_null", "order_id IS NOT NULL AND payment_sequential IS NOT NULL"),
    DqRule("payment_not_negative", "payment_amount >= 0"),
    DqRule("payment_type_domain", f"payment_type IN ({', '.join(repr(p) for p in PAYMENT_TYPES)})", "warn"),
    DqRule("payment_type_defined", "payment_type <> 'not_defined'", "warn"),
])

# Pedidos com mais de uma avaliação: fica a respondida por último (desempate pela criação e pelo review_id).
latest_review = Window.partitionBy("order_id").orderBy(
    F.col("review_answered_ts").desc_nulls_last(),
    F.col("review_created_ts").desc_nulls_last(),
    F.col("review_id").desc(),
)
order_review = (
    bronze("order_reviews").select(
        clean_string("review_id").alias("review_id"),
        clean_string("order_id").alias("order_id"),
        to_int("review_score").alias("review_score"),
        clean_string("review_comment_title").alias("review_comment_title"),
        clean_string("review_comment_message").alias("review_comment_message"),
        to_ts("review_creation_date").alias("review_created_ts"),
        to_ts("review_answer_timestamp").alias("review_answered_ts"),
    )
    .where(F.col("order_id").isNotNull())
    .withColumn("_rank", F.row_number().over(latest_review))
    .where("_rank = 1")
    .drop("_rank")
)
publish(order_review, "order_review", ["order_id"], [
    DqRule("review_id_not_null", "review_id IS NOT NULL"),
    DqRule("review_score_range", "review_score BETWEEN 1 AND 5"),
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Clientes, vendedores, produtos e CEPs

# CELL ********************

customer = bronze("customers").select(
    clean_string("customer_id").alias("customer_id"),
    clean_string("customer_unique_id").alias("customer_unique_id"),
    zip_prefix("customer_zip_code_prefix").alias("zip_code_prefix"),
    normalize_city("customer_city").alias("city_name"),
    F.upper(clean_string("customer_state")).alias("state_code"),
)
publish(customer, "customer", ["customer_id"], [
    DqRule("customer_id_not_null", "customer_id IS NOT NULL"),
    DqRule("customer_unique_id_not_null", "customer_unique_id IS NOT NULL", "warn"),
    DqRule("state_code_format", "state_code RLIKE '^[A-Z]{2}$'", "warn"),
])

seller = bronze("sellers").select(
    clean_string("seller_id").alias("seller_id"),
    zip_prefix("seller_zip_code_prefix").alias("zip_code_prefix"),
    normalize_city("seller_city").alias("city_name"),
    F.upper(clean_string("seller_state")).alias("state_code"),
)
publish(seller, "seller", ["seller_id"], [
    DqRule("seller_id_not_null", "seller_id IS NOT NULL"),
    DqRule("state_code_format", "state_code RLIKE '^[A-Z]{2}$'", "warn"),
])

category_translation = bronze("product_category_name_translation").select(
    F.lower(clean_string("product_category_name")).alias("category_code"),
    F.lower(clean_string("product_category_name_english")).alias("category_name_en"),
)
product = (
    bronze("products").select(
        clean_string("product_id").alias("product_id"),
        F.lower(clean_string("product_category_name")).alias("category_code"),
        to_int("product_name_lenght").alias("name_length"),          # "lenght" é grafia da origem
        to_int("product_description_lenght").alias("description_length"),
        to_int("product_photos_qty").alias("photo_count"),
        to_int("product_weight_g").alias("weight_grams"),
        to_int("product_length_cm").alias("length_cm"),
        to_int("product_height_cm").alias("height_cm"),
        to_int("product_width_cm").alias("width_cm"),
    )
    .join(category_translation, "category_code", "left")
    # 2 categorias não têm tradução na origem: usa o próprio código como nome em inglês
    .withColumn("category_name_en", F.coalesce("category_name_en", F.regexp_replace("category_code", "_", " ")))
)
publish(product, "product", ["product_id"], [
    DqRule("product_id_not_null", "product_id IS NOT NULL"),
    DqRule("category_not_null", "category_code IS NOT NULL", "warn"),
    DqRule("weight_positive", "weight_grams > 0", "warn"),
])

# Geolocalização: ~1 milhão de linhas com várias coordenadas por CEP. Primeiro filtra coordenadas
# fora do Brasil (quarentena), depois consolida 1 linha por prefixo de CEP.
geolocation = bronze("geolocation").select(
    zip_prefix("geolocation_zip_code_prefix").alias("zip_code_prefix"),
    clean_string("geolocation_lat").cast("double").alias("latitude"),
    clean_string("geolocation_lng").cast("double").alias("longitude"),
    normalize_city("geolocation_city").alias("city_name"),
    F.upper(clean_string("geolocation_state")).alias("state_code"),
)
geolocation_valid = apply_dq(add_audit_columns(geolocation, batch_id), "geolocation_point", [
    DqRule("zip_code_not_null", "zip_code_prefix IS NOT NULL"),
    DqRule("inside_brazil", "latitude BETWEEN -34.0 AND 5.5 AND longitude BETWEEN -74.0 AND -34.0"),
], batch_id)

most_common_city = Window.partitionBy("zip_code_prefix").orderBy(F.col("points").desc(), F.col("city_name"))
zip_city = (
    geolocation_valid.groupBy("zip_code_prefix", "city_name", "state_code").agg(F.count(F.lit(1)).alias("points"))
    .withColumn("_rank", F.row_number().over(most_common_city))
    .where("_rank = 1")
    .select("zip_code_prefix", "city_name", "state_code")
)
zip_code_location = (
    geolocation_valid.groupBy("zip_code_prefix").agg(
        F.avg("latitude").cast("decimal(9,6)").alias("latitude"),
        F.avg("longitude").cast("decimal(9,6)").alias("longitude"),
        F.count(F.lit(1)).alias("point_count"),
    )
    .join(zip_city, "zip_code_prefix")
)
publish(zip_code_location, "zip_code_location", ["zip_code_prefix"], [
    DqRule("zip_code_format", "zip_code_prefix RLIKE '^[0-9]{5}$'"),
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Integridade entre entidades (aviso)
# # Órfãos não bloqueiam a Silver: a Gold aponta para o membro desconhecido (`-1`). Aqui só medimos.

# CELL ********************

def orphan_count(child: str, parent: str, key: str) -> int:
    return (
        read_table(LH_SILVER, "commercial", child).select(key).distinct()
        .join(read_table(LH_SILVER, "commercial", parent).select(key), key, "left_anti")
        .count()
    )


referential_checks = [
    ("sales_order_item", "sales_order", "order_id"),
    ("sales_order_item", "product", "product_id"),
    ("sales_order_item", "seller", "seller_id"),
    ("sales_order", "customer", "customer_id"),
    ("order_payment", "sales_order", "order_id"),
    ("order_review", "sales_order", "order_id"),
]
now = datetime.now(timezone.utc)
rows = []
for child, parent, key in referential_checks:
    orphans = orphan_count(child, parent, key)
    rows.append((batch_id, child, f"fk_{key}_in_{parent}", f"{child}.{key} IN {parent}.{key}", "warn", None, orphans, now))
    print(f"{child}.{key} -> {parent}: {orphans} órfãos")

(
    spark.createDataFrame(rows, DQ_RESULT_SCHEMA)
    .write.format("delta").mode("append")
    .save(table_path(LH_SILVER, "framework", "dq_result"))
)

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
