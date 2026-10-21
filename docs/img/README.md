# Screenshots

Capturas da primeira execução completa, em uma capacidade Fabric trial (2026-10-20).

| Arquivo | O que mostra |
|---|---|
| `01-workspace-lineage.jpg` | Linhagem do workspace DEV: `lh_gold` → modelo semântico → relatório; rodapé com branch `main` e commit sincronizado |
| `02-pipeline-run.jpg` | Saída do `pl_daily_load_olist` com as 4 atividades concluídas (e o retry da Gold após HTTP 430) |
| `03-lakehouse-schemas.jpg` | Explorer do `lh_gold` com os schemas `commercial` e `shared` e a prévia da `fact_order` |
| `04-semantic-model.jpg` | Exibição de modelo do "Olist Vendas e Perdas" no Fabric |
| `05-report.jpg` | Página "Perdas no E-commerce" do relatório |

Ainda não capturadas:

| Arquivo | Motivo |
|---|---|
| `06-copilot.png` | O trial do Fabric não inclui Copilot; exige capacidade paga |
| `07-dq-result.png` | Consulta em `framework.dq_result` no SQL analytics endpoint do `lh_silver` |
| `08-deployment-pipeline.png` | Deployment pipeline DEV/TST/PRD ainda não criado |
