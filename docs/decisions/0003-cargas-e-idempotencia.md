# ADR 0003 - Padrões de carga e idempotência

- **Status:** aceito
- **Data:** 2026-10-20

## Contexto

O dataset Olist é um snapshot estático (2016-2018), mas o projeto deve se comportar como um pipeline diário de produção: reexecutável, auditável e rastreável até a execução.

## Decisão

| Camada | Padrão | Por quê |
|---|---|---|
| Bronze | Append do snapshot completo, partição `dp_ingestion_date`, `replaceWhere` por `dp_batch_id` | Guarda histórico para reprocessar; reexecutar o mesmo lote não duplica |
| Silver | `MERGE` por chave natural; atualiza só quando `dp_row_hash` muda; chave ausente na origem é removida | Mostra o padrão de upsert; preserva `dp_inserted_ts`. A remoção é segura porque a origem é snapshot completo |
| Gold | Overwrite completo de dimensões e fatos | Volume pequeno; reconstrução total a partir da Silver é o mais simples e sempre consistente |
| Modelo | Framing ao fim do pipeline, atualização automática desligada | O usuário nunca vê Gold carregada pela metade |

- O `RunId` do pipeline é o `batch_id` de todas as camadas e aparece em `dp_batch_id`, `framework.etl_run_log` e `framework.dq_result`.
- A Gold só é gravada depois de passar nos testes de integridade (zero órfãos) e de reconciliação com a Silver.

## Quando mudar

Com origem incremental (CDC, data de alteração), a Gold passaria a `MERGE` nas dimensões (com SCD2 onde o histórico importar, ex.: endereço do cliente) e carga incremental por partição de data nas fatos.
