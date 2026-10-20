# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_gold_refresh_semantic_model
#
# Último passo do pipeline. Duas tarefas:
#
# 1. **Rebind**: garante que a expressão `OneLake_Gold` do modelo aponte para o `lh_gold` **deste** workspace.
#    O TMDL versionado no Git guarda IDs de exemplo; depois de um sync do Git ou de um deploy para TST/PRD,
#    este passo corrige o endereço sem edição manual.
# 2. **Framing**: refresh do modelo Direct Lake. É uma operação de metadados (não copia dados) que aponta o modelo
#    para a versão mais recente das tabelas Delta. Atualizações automáticas ficam desligadas no modelo para que o
#    usuário nunca veja uma Gold carregada pela metade.
#
# Usa Semantic Link (`sempy`), pré-instalado no runtime do Fabric.

# PARAMETERS CELL ********************

semantic_model_name = "Olist Vendas e Perdas"
gold_lakehouse = "lh_gold"
expression_name = "OneLake_Gold"
batch_id = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import time
from datetime import datetime, timezone

import notebookutils
import sempy.fabric as fabric

workspace_id = notebookutils.runtime.context["currentWorkspaceId"]
lakehouse = notebookutils.lakehouse.get(gold_lakehouse)
lakehouse_id = lakehouse["id"] if isinstance(lakehouse, dict) else lakehouse.id
onelake_url = f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{lakehouse_id}"

expected_expression = (
    "let\n"
    f'    Fonte = AzureStorage.DataLake("{onelake_url}", [HierarchicalNavigation = true])\n'
    "in\n"
    "    Fonte"
)

server = fabric.create_tom_server(readonly=False, workspace=workspace_id)
database = server.Databases.GetByName(semantic_model_name)
expression = database.Model.Expressions.Find(expression_name)
if expression is None:
    raise ValueError(f"Expressão {expression_name!r} não encontrada em {semantic_model_name!r}")

if onelake_url not in expression.Expression:
    expression.Expression = expected_expression
    database.Model.SaveChanges()
    print(f"Modelo reapontado para {onelake_url}")
else:
    print("Modelo já aponta para o lh_gold deste workspace")
server.Disconnect()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

request_id = fabric.refresh_dataset(semantic_model_name, workspace=workspace_id, refresh_type="full")

for _ in range(165):   # até ~27 min, abaixo do timeout de 30 min da atividade no pipeline
    status = fabric.get_refresh_execution_details(semantic_model_name, request_id, workspace=workspace_id).status
    if status in ("Completed", "Failed", "Cancelled", "Disabled", "TimedOut"):
        break
    time.sleep(10)

print(f"Framing {request_id}: {status}")
if status != "Completed":
    raise RuntimeError(f"Refresh do modelo terminou com status {status}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
