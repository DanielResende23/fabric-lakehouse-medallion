# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_fw_maintain_tables
#
# Manutenção semanal, fora do horário de carga. Ordem obrigatória:
#
# 1. `OPTIMIZE ... VORDER`: compacta arquivos pequenos e reescreve em V-Order (guardrails e leitura do Direct Lake).
# 2. **Reenquadrar o modelo** para que ele passe a ler a versão compactada.
# 3. `VACUUM`: só então remove os arquivos antigos. Fazer o VACUUM antes do framing pode apagar arquivos que o
#    modelo ainda está usando.

# PARAMETERS CELL ********************

retention_hours = 168   # 7 dias: mínimo padrão do Delta para não quebrar time travel e leituras em andamento

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

# Lista fechada (nunca texto livre vindo de parâmetro).
TABLES = [
    (LH_GOLD, "shared", "dim_date"),
    (LH_GOLD, "commercial", "dim_customer"),
    (LH_GOLD, "commercial", "dim_seller"),
    (LH_GOLD, "commercial", "dim_product"),
    (LH_GOLD, "commercial", "dim_order_status"),
    (LH_GOLD, "commercial", "dim_payment_type"),
    (LH_GOLD, "commercial", "fact_sales"),
    (LH_GOLD, "commercial", "fact_order"),
    (LH_GOLD, "commercial", "fact_payment"),
    (LH_SILVER, "commercial", "sales_order"),
    (LH_SILVER, "commercial", "sales_order_item"),
    (LH_SILVER, "commercial", "order_payment"),
    (LH_SILVER, "commercial", "order_review"),
    (LH_SILVER, "commercial", "customer"),
    (LH_SILVER, "commercial", "seller"),
    (LH_SILVER, "commercial", "product"),
    (LH_SILVER, "commercial", "zip_code_location"),
]

for lakehouse, schema, table in TABLES:
    if table_exists(lakehouse, schema, table):
        spark.sql(f"OPTIMIZE delta.`{table_path(lakehouse, schema, table)}` VORDER")
        print(f"OPTIMIZE {lakehouse}.{schema}.{table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.notebook.run("nb_gold_refresh_semantic_model", 1800)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for lakehouse, schema, table in TABLES:
    if table_exists(lakehouse, schema, table):
        spark.sql(f"VACUUM delta.`{table_path(lakehouse, schema, table)}` RETAIN {int(retention_hours)} HOURS")
        print(f"VACUUM {lakehouse}.{schema}.{table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
