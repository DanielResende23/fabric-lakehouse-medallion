# Qualidade de dados

As regras ficam no código, ao lado da transformação (`nb_silver_load_commercial`), e os resultados ficam em tabela, consultáveis pelo SQL analytics endpoint do `lh_silver`.

## Onde cada verificação acontece

| Camada | Verificação | Efeito na falha |
|---|---|---|
| Bronze | Arquivo presente, colunas esperadas, pelo menos uma linha | Pipeline para |
| Silver | Regras de linha `error` (nulo em chave, domínio, faixa) | Linha vai para `lh_silver.quarantine.<tabela>` |
| Silver | Regras de linha `warn` | Linha segue; contagem registrada |
| Silver | Unicidade da chave natural depois da deduplicação | Pipeline para |
| Silver | Órfãos entre entidades (FK) | Só registro; Gold usa membro `-1` |
| Gold | Zero órfãos em todas as FKs, inclusive datas | Pipeline para, nada é publicado |
| Gold | Reconciliação de linhas e valores com a Silver | Pipeline para, nada é publicado |

## Regras da Silver

| Tabela | Regra | Severidade |
|---|---|---|
| `sales_order` | `order_id`, `customer_id`, `purchase_ts` não nulos; `order_status` no domínio | error |
| `sales_order` | Pedido `delivered` tem data de entrega; aprovação e entrega depois da compra | warn |
| `sales_order_item` | Chaves, produto e vendedor não nulos; preço e frete `>= 0` | error |
| `sales_order_item` | Preço `> 0` | warn |
| `order_payment` | Chaves não nulas; valor `>= 0` | error |
| `order_payment` | Forma de pagamento no domínio e diferente de `not_defined` | warn |
| `order_review` | `review_id` não nulo; nota entre 1 e 5 | error |
| `customer`, `seller` | Chave não nula | error |
| `customer`, `seller` | UF com 2 letras | warn |
| `product` | Chave não nula | error |
| `product` | Categoria preenchida; peso `> 0` | warn |
| `geolocation_point` | CEP não nulo; coordenada dentro do Brasil | error |

## Problemas conhecidos do dataset tratados

| Problema | Tratamento |
|---|---|
| Comentários de avaliação com quebra de linha dentro do CSV | Leitura com `multiLine` e `escape '"'` |
| Pedidos com mais de uma avaliação | Fica a avaliação respondida por último (Silver `order_review`, chave `order_id`) |
| Geolocalização com várias coordenadas por CEP e pontos fora do Brasil | Pontos fora do Brasil em quarentena; média de latitude/longitude por prefixo de CEP e cidade mais frequente |
| Nomes de cidade com e sem acento (`sao paulo`, `são paulo`) | `normalize_city`: minúsculas, sem acento, espaços simples |
| CEP com zero à esquerda perdido | `lpad` para 5 dígitos |
| Colunas `product_name_lenght` / `product_description_lenght` (grafia da origem) | Renomeadas na Silver para `name_length` / `description_length` |
| Categorias sem tradução para inglês | Fallback para o próprio código da categoria |
| Pedidos `delivered` sem data de entrega | Regra `warn`; ficam fora de prazo e atraso (`is_delivered = false`) |
| Pagamentos `not_defined` | Regra `warn`; mapeados para "Não definido" |

## Consultas úteis (SQL analytics endpoint do `lh_silver`)

```sql
-- Regras reprovadas no último lote
SELECT target_table, rule_name, severity_code, failed_count, checked_count
FROM framework.dq_result
WHERE batch_id = (SELECT TOP 1 batch_id FROM framework.etl_run_log ORDER BY finished_ts DESC)
  AND failed_count > 0
ORDER BY severity_code, failed_count DESC;

-- Duração e volume por etapa do último lote
SELECT layer_name, target_table, status_code, row_count, started_ts, finished_ts
FROM framework.etl_run_log
WHERE batch_id = (SELECT TOP 1 batch_id FROM framework.etl_run_log ORDER BY finished_ts DESC)
ORDER BY finished_ts;
```
