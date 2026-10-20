# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_fw_utils
#
# Funções compartilhadas pelos notebooks Bronze, Silver e Gold. Carregado com `%run nb_fw_utils`.
#
# - **Caminhos OneLake resolvidos em tempo de execução** a partir do workspace atual: os notebooks não têm lakehouse padrão
#   nem GUID fixo, então o mesmo código roda em DEV, TST e PRD sem regra de deployment.
# - **Escrita Delta** idempotente (`overwrite_table`, `upsert_table`).
# - **Colunas técnicas `dp_`** (auditoria e linhagem até o lote).
# - **Run log** (`lh_silver.framework.etl_run_log`) e **qualidade de dados** (`lh_silver.framework.dq_result` + quarentena).

# CELL ********************

import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

import notebookutils
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

IDENT = re.compile(r"^[a-z0-9_]+$")

LH_BRONZE = "lh_bronze"
LH_SILVER = "lh_silver"
LH_GOLD = "lh_gold"


def _check_ident(*names: str) -> None:
    """Só aceita identificadores snake_case: evita injeção em caminhos e SQL montados por string."""
    for name in names:
        if not IDENT.match(name):
            raise ValueError(f"Identificador inválido: {name!r}")


def new_batch_id(batch_id: str = "") -> str:
    """Usa o RunId do pipeline quando vier por parâmetro; em execução manual gera um id local."""
    return batch_id.strip() or f"manual-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Caminhos OneLake

# CELL ********************

_LAKEHOUSE_ROOT: dict = {}


def lakehouse_root(lakehouse: str) -> str:
    """abfss://<workspaceId>@onelake.dfs.fabric.microsoft.com/<lakehouseId> do lakehouse no workspace atual."""
    _check_ident(lakehouse)
    if lakehouse not in _LAKEHOUSE_ROOT:
        workspace_id = notebookutils.runtime.context["currentWorkspaceId"]
        item = notebookutils.lakehouse.get(lakehouse)
        lakehouse_id = item["id"] if isinstance(item, dict) else item.id
        _LAKEHOUSE_ROOT[lakehouse] = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}"
    return _LAKEHOUSE_ROOT[lakehouse]


def table_path(lakehouse: str, schema: str, table: str) -> str:
    _check_ident(schema, table)
    return f"{lakehouse_root(lakehouse)}/Tables/{schema}/{table}"


def files_path(lakehouse: str, relative: str) -> str:
    return f"{lakehouse_root(lakehouse)}/Files/{relative.strip('/')}"


def table_exists(lakehouse: str, schema: str, table: str) -> bool:
    return DeltaTable.isDeltaTable(spark, table_path(lakehouse, schema, table))


def read_table(lakehouse: str, schema: str, table: str) -> DataFrame:
    return spark.read.format("delta").load(table_path(lakehouse, schema, table))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Colunas técnicas e escrita Delta

# CELL ********************

def clean_string(column: str):
    """TRIM e vazio vira NULL."""
    trimmed = F.trim(F.col(column))
    return F.when(trimmed == "", None).otherwise(trimmed)


def normalize_city(column: str):
    """Minúsculas, sem acento e sem espaços duplos: 'São Paulo ' e 'sao paulo' viram 'sao paulo'."""
    accents, plain = "áàâãäéèêëíìîïóòôõöúùûüçñ", "aaaaaeeeeiiiiooooouuuucn"
    return F.regexp_replace(F.translate(F.lower(clean_string(column)), accents, plain), r"\s+", " ")


def add_audit_columns(df: DataFrame, batch_id: str, source_system: str = "olist") -> DataFrame:
    now = F.current_timestamp()
    return (
        df.withColumn("dp_inserted_ts", now)
        .withColumn("dp_updated_ts", now)
        .withColumn("dp_source_system", F.lit(source_system))
        .withColumn("dp_batch_id", F.lit(batch_id))
    )


def add_row_hash(df: DataFrame) -> DataFrame:
    """Hash das colunas de negócio (sem as dp_): o merge só reescreve a linha quando o conteúdo mudou."""
    business_columns = sorted(c for c in df.columns if not c.startswith("dp_"))
    values = [F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in business_columns]
    return df.withColumn("dp_row_hash", F.sha2(F.concat_ws("||", *values), 256))


def overwrite_table(df: DataFrame, lakehouse: str, schema: str, table: str, partition_by: list = None) -> int:
    path = table_path(lakehouse, schema, table)
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)
    return spark.read.format("delta").load(path).count()


def upsert_table(df: DataFrame, lakehouse: str, schema: str, table: str, keys: list) -> int:
    """MERGE por chave natural.

    - Atualiza só quando `dp_row_hash` muda e preserva `dp_inserted_ts` original.
    - A origem chega como snapshot completo, então chave que sumiu da origem é removida
      (decisão registrada em docs/decisions/0003-cargas-e-idempotencia.md).
    """
    _check_ident(*keys)
    # Origem vazia com whenNotMatchedBySourceDelete apagaria a tabela inteira: nunca seguir.
    if df.isEmpty():
        raise ValueError(f"{lakehouse}.{schema}.{table}: origem vazia, MERGE cancelado")
    if not table_exists(lakehouse, schema, table):
        return overwrite_table(df, lakehouse, schema, table)

    target = DeltaTable.forPath(spark, table_path(lakehouse, schema, table))
    condition = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    update_set = {c: f"s.{c}" for c in df.columns if c != "dp_inserted_ts"}
    (
        target.alias("t")
        .merge(df.alias("s"), condition)
        .whenMatchedUpdate(condition="t.dp_row_hash <> s.dp_row_hash", set=update_set)
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )
    return target.toDF().count()


def enable_vorder(lakehouse: str, schema: str, table: str) -> None:
    """V-Order melhora leitura no Direct Lake; liga como propriedade da tabela (vale para escritas futuras e OPTIMIZE)."""
    path = table_path(lakehouse, schema, table)
    spark.sql(f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES ('delta.parquet.vorder.enabled' = 'true')")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Run log

# CELL ********************

RUN_LOG_SCHEMA = T.StructType([
    T.StructField("batch_id", T.StringType()),
    T.StructField("notebook_name", T.StringType()),
    T.StructField("layer_name", T.StringType()),
    T.StructField("target_table", T.StringType()),
    T.StructField("status_code", T.StringType()),
    T.StructField("row_count", T.LongType()),
    T.StructField("started_ts", T.TimestampType()),
    T.StructField("finished_ts", T.TimestampType()),
    T.StructField("message_text", T.StringType()),
])


def log_run(batch_id: str, notebook_name: str, layer_name: str, target_table: str, status_code: str,
            row_count: int = None, started_ts: datetime = None, message_text: str = None) -> None:
    row = [(batch_id, notebook_name, layer_name, target_table, status_code,
            None if row_count is None else int(row_count), started_ts, datetime.now(timezone.utc), message_text)]
    (
        spark.createDataFrame(row, RUN_LOG_SCHEMA)
        .write.format("delta").mode("append")
        .save(table_path(LH_SILVER, "framework", "etl_run_log"))
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Qualidade de dados
#
# Regras de linha são expressões SQL que valem `true` para a linha **válida**. `NULL` no resultado conta como válido,
# porque nulos são tratados por regras `is not null` próprias.
#
# | Severidade | Efeito |
# |---|---|
# | `error` | Linha vai para `lh_silver.quarantine.<tabela>` e não segue para a Silver |
# | `warn` | Linha segue; a contagem fica registrada em `framework.dq_result` |

# CELL ********************

@dataclass(frozen=True)
class DqRule:
    name: str
    expression: str
    severity: str = "error"  # error | warn


DQ_RESULT_SCHEMA = T.StructType([
    T.StructField("batch_id", T.StringType()),
    T.StructField("target_table", T.StringType()),
    T.StructField("rule_name", T.StringType()),
    T.StructField("rule_expression", T.StringType()),
    T.StructField("severity_code", T.StringType()),
    T.StructField("checked_count", T.LongType()),
    T.StructField("failed_count", T.LongType()),
    T.StructField("checked_ts", T.TimestampType()),
])


def apply_dq(df: DataFrame, target_table: str, rules: list, batch_id: str) -> DataFrame:
    """Avalia as regras, grava o resultado, manda reprovados em `error` para quarentena e devolve as linhas válidas."""
    _check_ident(target_table)
    flagged = df
    for rule in rules:
        flagged = flagged.withColumn(f"_dq_{rule.name}", F.coalesce(F.expr(rule.expression), F.lit(True)))
    flagged = flagged.cache()

    aggregations = [F.count(F.lit(1)).alias("_checked")] + [
        F.sum(F.when(~F.col(f"_dq_{r.name}"), 1).otherwise(0)).alias(r.name) for r in rules
    ]
    totals = flagged.agg(*aggregations).first().asDict()
    now = datetime.now(timezone.utc)
    results = [
        (batch_id, target_table, r.name, r.expression, r.severity, int(totals["_checked"]), int(totals[r.name] or 0), now)
        for r in rules
    ]
    (
        spark.createDataFrame(results, DQ_RESULT_SCHEMA)
        .write.format("delta").mode("append")
        .save(table_path(LH_SILVER, "framework", "dq_result"))
    )

    error_rules = [r for r in rules if r.severity == "error"]
    is_valid = F.lit(True)
    for rule in error_rules:
        is_valid = is_valid & F.col(f"_dq_{rule.name}")
    failed_rules = F.array_compact(F.array(*[
        F.when(~F.col(f"_dq_{r.name}"), F.lit(r.name)) for r in error_rules
    ])) if error_rules else F.array().cast("array<string>")

    dq_columns = [f"_dq_{r.name}" for r in rules]
    quarantine = flagged.filter(~is_valid).withColumn("dq_failed_rules", failed_rules).drop(*dq_columns)
    quarantine_path = table_path(LH_SILVER, "quarantine", target_table)
    (
        quarantine.write.format("delta").mode("overwrite")
        .option("replaceWhere", f"dp_batch_id = '{batch_id}'")
        .option("mergeSchema", "true")
        .save(quarantine_path)
    )

    for r, (_, _, _, _, _, checked, failed, _) in zip(rules, results):
        if failed:
            print(f"[DQ] {target_table}.{r.name} ({r.severity}): {failed} de {checked} linhas reprovadas")
    return flagged.filter(is_valid).drop(*dq_columns)


def assert_unique(df: DataFrame, keys: list, table_name: str) -> None:
    duplicated = df.groupBy(*keys).count().filter("count > 1").count()
    if duplicated:
        raise AssertionError(f"{table_name}: {duplicated} chaves duplicadas em {keys}")


def assert_no_orphans(fact: DataFrame, dim: DataFrame, key: str, fact_name: str, dim_name: str) -> None:
    orphans = fact.select(key).where(F.col(key).isNotNull()).join(dim.select(key), key, "left_anti").count()
    if orphans:
        raise AssertionError(f"{fact_name}.{key}: {orphans} linhas sem correspondência em {dim_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
