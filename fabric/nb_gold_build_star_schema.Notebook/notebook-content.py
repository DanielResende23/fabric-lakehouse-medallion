# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_gold_build_star_schema
# # Silver (`lh_silver.commercial.*`) → Gold (`lh_gold`), star schema para o modelo semântico em Direct Lake.
# # | Tabela | Grão | Observação |
# |---|---|---|
# | `shared.dim_date` | 1 dia | Calendário contínuo, nomes em pt-BR, `date_key` = `yyyyMMdd` |
# | `commercial.dim_customer` | 1 `customer_id` | Inclui `customer_unique_id` (a pessoa), UF, região e coordenadas |
# | `commercial.dim_seller` | 1 vendedor | UF, região e coordenadas |
# | `commercial.dim_product` | 1 produto | Categoria em pt-BR e inglês, dimensões físicas |
# | `commercial.dim_order_status` | 1 status | Agrupa status em **Concluído / Em andamento / Perdido** |
# | `commercial.dim_payment_type` | 1 forma de pagamento | Nome em pt-BR |
# | `commercial.fact_sales` | 1 item de pedido | Receita de produto e frete; vendedor e produto |
# | `commercial.fact_order` | 1 pedido | Snapshot acumulado: datas do ciclo, prazo, atraso, avaliação |
# | `commercial.fact_payment` | 1 pagamento do pedido | Forma de pagamento e parcelas |
# # - Chaves substitutas `_key` BIGINT por `xxhash64` da chave natural: estáveis entre cargas, sem tabela de mapeamento.
#   Colisão é conferida a cada execução (`assert_unique`).
# - Membro desconhecido `-1` em toda dimensão; FK sem correspondência aponta para ele, nunca fica nula.
#   Exceção: chaves de data opcionais (entrega, aprovação) ficam nulas para manter o calendário contínuo (ver ADR 0002).
# - Lógica que depende de filtro fica em DAX; aqui só derivações por linha (prazo, atraso, flags).

# PARAMETERS CELL ********************

batch_id = ""

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

NOTEBOOK_NAME = "nb_gold_build_star_schema"
batch_id = new_batch_id(batch_id)
started_ts = datetime.now(timezone.utc)

# V-Order nas escritas desta sessão: arquivos Parquet otimizados para leitura do VertiPaq (Direct Lake).
spark.conf.set("spark.sql.parquet.vorder.default", "true")

UNKNOWN_KEY = -1


def silver(table: str) -> DataFrame:
    df = read_table(LH_SILVER, "commercial", table)
    return df.drop(*[c for c in df.columns if c.startswith("dp_")])


def surrogate_key(*natural_key_columns: str):
    return F.xxhash64(*[F.col(c) for c in natural_key_columns])


def date_key(column):
    return F.date_format(column, "yyyyMMdd").cast("int")


def with_unknown_member(dim: DataFrame, key_column: str, defaults: dict) -> DataFrame:
    """Acrescenta a linha -1; colunas não informadas ficam nulas."""
    values = {c: defaults.get(c) for c in dim.columns}
    values[key_column] = UNKNOWN_KEY
    unknown = spark.createDataFrame([tuple(values[c] for c in dim.columns)], dim.schema)
    return dim.unionByName(unknown)


def publish(df: DataFrame, schema: str, table: str, keys: list = None) -> int:
    if keys:
        assert_unique(df, keys, f"gold.{schema}.{table}")
    audited = df.withColumn("dp_inserted_ts", F.current_timestamp()).withColumn("dp_batch_id", F.lit(batch_id))
    row_count = overwrite_table(audited, LH_GOLD, schema, table)
    enable_vorder(LH_GOLD, schema, table)
    log_run(batch_id, NOTEBOOK_NAME, "gold", f"{schema}.{table}", "succeeded", row_count, started_ts)
    print(f"{schema}.{table}: {row_count} linhas")
    return row_count

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Referências fixas (UF, região, status, forma de pagamento)

# CELL ********************

STATES = {
    "AC": ("Acre", "Norte"), "AP": ("Amapá", "Norte"), "AM": ("Amazonas", "Norte"), "PA": ("Pará", "Norte"),
    "RO": ("Rondônia", "Norte"), "RR": ("Roraima", "Norte"), "TO": ("Tocantins", "Norte"),
    "AL": ("Alagoas", "Nordeste"), "BA": ("Bahia", "Nordeste"), "CE": ("Ceará", "Nordeste"),
    "MA": ("Maranhão", "Nordeste"), "PB": ("Paraíba", "Nordeste"), "PE": ("Pernambuco", "Nordeste"),
    "PI": ("Piauí", "Nordeste"), "RN": ("Rio Grande do Norte", "Nordeste"), "SE": ("Sergipe", "Nordeste"),
    "DF": ("Distrito Federal", "Centro-Oeste"), "GO": ("Goiás", "Centro-Oeste"),
    "MT": ("Mato Grosso", "Centro-Oeste"), "MS": ("Mato Grosso do Sul", "Centro-Oeste"),
    "ES": ("Espírito Santo", "Sudeste"), "MG": ("Minas Gerais", "Sudeste"),
    "RJ": ("Rio de Janeiro", "Sudeste"), "SP": ("São Paulo", "Sudeste"),
    "PR": ("Paraná", "Sul"), "RS": ("Rio Grande do Sul", "Sul"), "SC": ("Santa Catarina", "Sul"),
}
states = spark.createDataFrame(
    [(code, name, region) for code, (name, region) in STATES.items()],
    "state_code string, state_name string, region_name string",
)

# (código, nome pt-BR, grupo, ordem)
ORDER_STATUS = [
    ("created", "Criado", "Em andamento", 1),
    ("approved", "Aprovado", "Em andamento", 2),
    ("invoiced", "Faturado", "Em andamento", 3),
    ("processing", "Em separação", "Em andamento", 4),
    ("shipped", "Enviado", "Em andamento", 5),
    ("delivered", "Entregue", "Concluído", 6),
    ("canceled", "Cancelado", "Perdido", 7),
    ("unavailable", "Indisponível", "Perdido", 8),
]
PAYMENT_TYPES = [
    ("credit_card", "Cartão de crédito", 1),
    ("boleto", "Boleto", 2),
    ("voucher", "Voucher", 3),
    ("debit_card", "Cartão de débito", 4),
    ("not_defined", "Não definido", 5),
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Dimensões

# CELL ********************

# double no Gold: Direct Lake só mapeia DECIMAL com até 4 casas para o tipo Decimal do modelo
zip_location = silver("zip_code_location").select(
    "zip_code_prefix",
    F.col("latitude").cast("double").alias("latitude"),
    F.col("longitude").cast("double").alias("longitude"),
)

dim_customer = (
    silver("customer")
    .join(states, "state_code", "left")
    .join(zip_location, "zip_code_prefix", "left")
    .select(
        surrogate_key("customer_id").alias("customer_key"),
        "customer_id", "customer_unique_id", "zip_code_prefix",
        F.initcap("city_name").alias("city_name"),
        "state_code", "state_name", "region_name", "latitude", "longitude",
    )
)
dim_customer = with_unknown_member(dim_customer, "customer_key", {
    "customer_id": "unknown", "customer_unique_id": "unknown", "city_name": "Desconhecido",
    "state_code": "NA", "state_name": "Desconhecido", "region_name": "Desconhecido",
})

dim_seller = (
    silver("seller")
    .join(states, "state_code", "left")
    .join(zip_location, "zip_code_prefix", "left")
    .select(
        surrogate_key("seller_id").alias("seller_key"),
        "seller_id", "zip_code_prefix",
        F.initcap("city_name").alias("city_name"),
        "state_code", "state_name", "region_name", "latitude", "longitude",
    )
)
dim_seller = with_unknown_member(dim_seller, "seller_key", {
    "seller_id": "unknown", "city_name": "Desconhecido",
    "state_code": "NA", "state_name": "Desconhecido", "region_name": "Desconhecido",
})

dim_product = silver("product").select(
    surrogate_key("product_id").alias("product_key"),
    "product_id",
    F.coalesce(F.initcap(F.regexp_replace("category_code", "_", " ")), F.lit("Sem categoria")).alias("category_name"),
    F.coalesce(F.initcap("category_name_en"), F.lit("Uncategorized")).alias("category_name_en"),
    "photo_count", "weight_grams", "length_cm", "height_cm", "width_cm",
)
dim_product = with_unknown_member(dim_product, "product_key", {
    "product_id": "unknown", "category_name": "Desconhecido", "category_name_en": "Unknown",
})

dim_order_status = spark.createDataFrame(
    [(i, code, name, group, order) for i, (code, name, group, order) in enumerate(ORDER_STATUS, start=1)]
    + [(UNKNOWN_KEY, "unknown", "Desconhecido", "Desconhecido", 99)],
    "order_status_key long, order_status_code string, order_status_name string, status_group_name string, sort_order int",
)

dim_payment_type = spark.createDataFrame(
    [(i, code, name, order) for i, (code, name, order) in enumerate(PAYMENT_TYPES, start=1)]
    + [(UNKNOWN_KEY, "unknown", "Desconhecido", 99)],
    "payment_type_key long, payment_type_code string, payment_type_name string, sort_order int",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

orders = silver("sales_order")
# Todas as datas que viram date_key nas fatos precisam caber no calendário.
last_review_date = silver("order_review").agg(F.max(F.to_date("review_created_ts"))).first()[0]
bounds = orders.select(
    F.min(F.to_date("purchase_ts")).alias("first_date"),
    F.greatest(
        F.max("estimated_delivery_date"),
        F.max(F.to_date("delivered_customer_ts")),
        F.max(F.to_date("approved_ts")),
        F.lit(last_review_date).cast("date"),
    ).alias("last_date"),
).first()
first_date = bounds["first_date"].replace(month=1, day=1)
last_date = bounds["last_date"].replace(month=12, day=31)

MONTHS = ["janeiro", "fevereiro", "março", "abril", "maio", "junho",
          "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"]
WEEKDAYS = ["domingo", "segunda-feira", "terça-feira", "quarta-feira", "quinta-feira", "sexta-feira", "sábado"]
month_names = F.array(*[F.lit(m) for m in MONTHS])
weekday_names = F.array(*[F.lit(d) for d in WEEKDAYS])

dim_date = (
    spark.sql(f"SELECT explode(sequence(DATE'{first_date}', DATE'{last_date}', INTERVAL 1 DAY)) AS date")
    .select(
        date_key(F.col("date")).alias("date_key"),
        "date",
        F.year("date").alias("year_number"),
        F.quarter("date").alias("quarter_number"),
        F.concat(F.lit("T"), F.quarter("date")).alias("quarter_name"),
        F.month("date").alias("month_number"),
        F.initcap(F.element_at(month_names, F.month("date"))).alias("month_name"),
        F.initcap(F.substring(F.element_at(month_names, F.month("date")), 1, 3)).alias("month_short_name"),
        (F.year("date") * 100 + F.month("date")).alias("year_month_number"),
        F.concat(F.substring(F.element_at(month_names, F.month("date")), 1, 3), F.lit("/"), F.year("date")).alias("year_month_name"),
        F.dayofweek("date").alias("weekday_number"),
        F.initcap(F.element_at(weekday_names, F.dayofweek("date"))).alias("weekday_name"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Fatos

# CELL ********************

items = silver("sales_order_item")
payments = silver("order_payment")
reviews = silver("order_review")

customer_keys = dim_customer.select("customer_id", "customer_key")
status_keys = dim_order_status.select(F.col("order_status_code").alias("order_status"), "order_status_key")
payment_keys = dim_payment_type.select(F.col("payment_type_code").alias("payment_type"), "payment_type_key")

order_items = items.groupBy("order_id").agg(
    F.count(F.lit(1)).cast("int").alias("item_count"),
    F.sum("item_price_amount").cast("decimal(18,4)").alias("items_amount"),
    F.sum("freight_amount").cast("decimal(18,4)").alias("freight_amount"),
)
order_payments = payments.groupBy("order_id").agg(
    F.sum("payment_amount").cast("decimal(18,4)").alias("payment_amount"),
)

purchase_date = F.to_date("purchase_ts")
delivered_date = F.to_date("delivered_customer_ts")
is_delivered = (F.col("order_status") == "delivered") & F.col("delivered_customer_ts").isNotNull()

order_base = (
    orders
    .withColumn("is_delivered", is_delivered)
    .withColumn("delivery_days", F.when(is_delivered, F.datediff(delivered_date, purchase_date)))
    .withColumn("delay_days", F.when(is_delivered, F.datediff(delivered_date, F.col("estimated_delivery_date"))))
    .withColumn("is_late", F.coalesce(F.col("delay_days") > 0, F.lit(False)))
    .withColumn("is_lost", F.col("order_status").isin("canceled", "unavailable"))
    .join(customer_keys, "customer_id", "left")
    .join(status_keys, "order_status", "left")
    .withColumn("customer_key", F.coalesce("customer_key", F.lit(UNKNOWN_KEY)))
    .withColumn("order_status_key", F.coalesce("order_status_key", F.lit(UNKNOWN_KEY)))
    .withColumn("purchase_date_key", date_key(purchase_date))
)

fact_order = (
    order_base
    .join(order_items, "order_id", "left")
    .join(order_payments, "order_id", "left")
    .join(reviews.select("order_id", "review_score", "review_comment_message", "review_created_ts"), "order_id", "left")
    .select(
        "order_id", "customer_key", "order_status_key", "purchase_date_key",
        date_key(F.to_date("approved_ts")).alias("approved_date_key"),
        date_key(delivered_date).alias("delivered_date_key"),
        date_key(F.col("estimated_delivery_date")).alias("estimated_delivery_date_key"),
        date_key(F.to_date("review_created_ts")).alias("review_date_key"),
        F.coalesce("item_count", F.lit(0)).alias("item_count"),
        F.coalesce("items_amount", F.lit(0).cast("decimal(18,4)")).alias("items_amount"),
        F.coalesce("freight_amount", F.lit(0).cast("decimal(18,4)")).alias("freight_amount"),
        (F.coalesce("items_amount", F.lit(0)) + F.coalesce("freight_amount", F.lit(0))).cast("decimal(18,4)").alias("order_amount"),
        F.coalesce("payment_amount", F.lit(0).cast("decimal(18,4)")).alias("payment_amount"),
        "delivery_days", "delay_days", "is_delivered", "is_late", "is_lost",
        "review_score",
        F.col("review_score").isNotNull().alias("has_review"),
        F.col("review_comment_message").isNotNull().alias("has_review_comment"),
        (F.col("review_score") <= 2).alias("is_negative_review"),
    )
)

fact_sales = (
    items
    .join(order_base.select("order_id", "customer_key", "order_status_key", "purchase_date_key",
                            "is_delivered", "is_late", "is_lost"), "order_id", "left")
    .join(dim_product.select("product_id", "product_key"), "product_id", "left")
    .join(dim_seller.select("seller_id", "seller_key"), "seller_id", "left")
    .select(
        "order_id", "order_item_number",
        *[F.coalesce(c, F.lit(UNKNOWN_KEY)).alias(c)
          for c in ["customer_key", "seller_key", "product_key", "order_status_key"]],
        "purchase_date_key",
        "item_price_amount", "freight_amount",
        (F.col("item_price_amount") + F.col("freight_amount")).cast("decimal(18,4)").alias("gross_amount"),
        F.coalesce("is_delivered", F.lit(False)).alias("is_delivered"),
        F.coalesce("is_late", F.lit(False)).alias("is_late"),
        F.coalesce("is_lost", F.lit(False)).alias("is_lost"),
    )
)

fact_payment = (
    payments
    .join(order_base.select("order_id", "customer_key", "order_status_key", "purchase_date_key"), "order_id", "left")
    .join(payment_keys, "payment_type", "left")
    .select(
        "order_id", "payment_sequential",
        *[F.coalesce(c, F.lit(UNKNOWN_KEY)).alias(c) for c in ["customer_key", "order_status_key", "payment_type_key"]],
        "purchase_date_key", "installment_count", "payment_amount",
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Testes antes de publicar
# # Nada é gravado se a Gold não fechar com a Silver: integridade referencial (zero órfãos) e reconciliação de totais.

# CELL ********************

dims = {
    "customer_key": (dim_customer, "dim_customer"),
    "seller_key": (dim_seller, "dim_seller"),
    "product_key": (dim_product, "dim_product"),
    "order_status_key": (dim_order_status, "dim_order_status"),
    "payment_type_key": (dim_payment_type, "dim_payment_type"),
}
for fact, fact_name in [(fact_sales, "fact_sales"), (fact_order, "fact_order"), (fact_payment, "fact_payment")]:
    for key, (dim, dim_name) in dims.items():
        if key in fact.columns:
            assert_no_orphans(fact, dim, key, fact_name, dim_name)
    for key in [c for c in fact.columns if c.endswith("date_key")]:
        assert_no_orphans(fact, dim_date.select(F.col("date_key").alias(key)), key, fact_name, "dim_date")

reconciliation = [
    ("fact_order rows = sales_order rows", fact_order.count(), orders.count()),
    ("fact_sales rows = sales_order_item rows", fact_sales.count(), items.count()),
    ("fact_payment rows = order_payment rows", fact_payment.count(), payments.count()),
    ("fact_sales price = silver price",
     fact_sales.agg(F.sum("item_price_amount")).first()[0], items.agg(F.sum("item_price_amount")).first()[0]),
    ("fact_payment amount = silver amount",
     fact_payment.agg(F.sum("payment_amount")).first()[0], payments.agg(F.sum("payment_amount")).first()[0]),
]
for check, gold_value, silver_value in reconciliation:
    if gold_value != silver_value:
        raise AssertionError(f"Reconciliação falhou: {check} ({gold_value} <> {silver_value})")
    print(f"OK  {check}: {gold_value}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Publicação

# CELL ********************

publish(dim_date, "shared", "dim_date", ["date_key"])
publish(dim_customer, "commercial", "dim_customer", ["customer_key"])
publish(dim_seller, "commercial", "dim_seller", ["seller_key"])
publish(dim_product, "commercial", "dim_product", ["product_key"])
publish(dim_order_status, "commercial", "dim_order_status", ["order_status_key"])
publish(dim_payment_type, "commercial", "dim_payment_type", ["payment_type_key"])
publish(fact_order, "commercial", "fact_order", ["order_id"])
publish(fact_sales, "commercial", "fact_sales", ["order_id", "order_item_number"])
publish(fact_payment, "commercial", "fact_payment", ["order_id", "payment_sequential"])

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
