# ADR 0002 - Modelagem da Gold: três fatos e chaves por hash

- **Status:** aceito
- **Data:** 2026-10-20

## Contexto

O Olist tem três grãos naturais: item de pedido (produto, vendedor, preço, frete), pedido (ciclo de entrega e avaliação) e pagamento (forma de pagamento, parcelas). As perguntas de negócio cruzam os três: receita perdida por categoria, atraso por UF, nota da avaliação por atraso, forma de pagamento por status.

## Decisão

1. **Três fatos, cada uma no seu grão:**
   - `fact_sales` (item): receita de produto e frete, filtrável por vendedor e produto.
   - `fact_order` (pedido, snapshot acumulado): datas do ciclo, prazo, atraso, perda, avaliação.
   - `fact_payment` (pagamento): forma de pagamento e parcelas.
   As três compartilham as dimensões `Cliente`, `Status do Pedido` e `Calendário`, então qualquer medida pode ser combinada por essas dimensões no mesmo visual.
2. **Flags de entrega replicadas em `fact_sales`** (`is_late`, `is_delivered`, `is_lost`): permitem "% de itens atrasados por vendedor/categoria" sem relacionamento entre fatos.
3. **Chaves substitutas `BIGINT` por `xxhash64` da chave natural:** estáveis entre cargas completas, sem tabela de mapeamento nem sequência. Colisão conferida a cada carga (`assert_unique`). Com ~100 mil linhas por dimensão a probabilidade é desprezível; em volume de bilhões, a escolha seria uma sequência ou um mapeamento persistido.
4. **Membro desconhecido `-1`** em toda dimensão; FK sem correspondência aponta para ele.
5. **Chaves de data opcionais ficam nulas** (entrega, aprovação, avaliação), em vez de apontar para uma data "desconhecida". O calendário precisa ser contínuo e sem linha especial para ser marcado como tabela de datas e usar inteligência de tempo. Um `date_key` nulo simplesmente não filtra.
6. **Status em dimensão própria** (`dim_order_status`) com o agrupamento `Concluído / Em andamento / Perdido`. A regra "o que é perda" fica em um lugar só (Gold), e o modelo semântico só a consome.

## Alternativas

- **Uma fato única por item com atributos de pedido repetidos:** contaria pedidos e avaliações em duplicidade quando o pedido tem mais de um item.
- **Relacionamento fato-fato (`fact_sales` → `fact_order`):** cria caminho ambíguo de filtro e piora as respostas do Copilot (regra AIR-E05).
- **Role-playing com várias tabelas de calendário:** mais tabelas visíveis para o mesmo conceito. Preferido um calendário com um relacionamento inativo para a data de entrega, usado por medida explícita.

## Consequências

- Medidas de pedido não respondem a filtro de Vendedor/Produto. A descrição da tabela `Pedidos` avisa e aponta as medidas por item equivalentes.
