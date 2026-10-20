# ADR 0004 - Direct Lake on OneLake e medidas nas tabelas fato

- **Status:** aceito
- **Data:** 2026-10-20

## Contexto

O modelo semântico lê a Gold. As opções são Import, DirectQuery, Direct Lake on SQL endpoint e Direct Lake on OneLake.

## Decisão

**Direct Lake on OneLake**, com uma expressão compartilhada `OneLake_Gold` e partições `entity` apontando para `schema.tabela` do `lh_gold`.

- Sem cópia de dados para o modelo e sem refresh de importação: o "refresh" é o framing, operação de metadados.
- É a variação recomendada pela Microsoft para modelos novos; não depende do SQL endpoint (nem do seu atraso de sincronização de metadados).
- Não existe fallback para DirectQuery: se uma tabela estourar os guardrails da SKU, a consulta falha em vez de ficar lenta sem ninguém perceber. Por isso a Gold é escrita com V-Order e o `nb_fw_maintain_tables` compacta as tabelas semanalmente.

**Medidas nas próprias tabelas fato** (`Vendas`, `Pedidos`, `Pagamentos`), organizadas em display folders, em vez de uma tabela `_Medidas`. Uma tabela só de medidas exigiria uma tabela calculada ou importada dentro de um modelo Direct Lake; com a medida na fato do seu grão, a origem de cada número fica explícita para quem lê e para o Copilot.

## Rebind entre ambientes

O TMDL versionado tem IDs de exemplo no endereço OneLake. O notebook `nb_gold_refresh_semantic_model` (último passo do pipeline) lê o ID do workspace e do `lh_gold` em execução, atualiza a expressão via TOM (Semantic Link) se ela apontar para outro lugar e dispara o framing. O mesmo artefato funciona em DEV, TST e PRD sem edição manual.

## Alternativas

- **Direct Lake on SQL endpoint:** necessário só se a segurança estivesse no SQL endpoint ou se o fallback para DirectQuery fosse desejado.
- **Import:** cópia dos dados e refresh agendado; sem ganho para um modelo de ~100 mil pedidos já em Delta otimizado.
- **Regras de deployment pipeline para a fonte:** dependem do suporte do tipo de fonte e ficam fora do Git; o rebind por notebook fica versionado e testável.
