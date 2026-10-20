# Copilot: preparação do modelo (Prep data for AI)

O modelo `Olist Vendas e Perdas` já sai do Git com o que dá para versionar em TMDL: nomes de negócio em pt-BR, descrição em toda tabela, coluna visível e medida, chaves ocultas, `summarizeBy: none` em códigos, categorias de dados geográficos, calendário marcado, `discourageImplicitMeasures` e tradução en-US. O que segue é configurado no serviço em **Semantic model → Prep data for AI** e fica documentado aqui para ser reaplicado em cada ambiente.

## 1. AI data schema

Incluir: todas as tabelas, exceto colunas ocultas (já ficam fora). Excluir explicitamente: `Latitude/Longitude do Cliente`, `Latitude/Longitude do Vendedor` (só servem para mapa) e `Fotos do Anúncio`.

## 2. AI instructions

Copiar o bloco abaixo no campo *AI instructions*:

```text
Contexto: marketplace de e-commerce brasileiro Olist, pedidos de set/2016 a out/2018. Valores em reais (BRL).

Glossário:
- "Vendas", "faturamento", "GMV" = medida [Receita Bruta] (produto + frete, todos os status).
- "Receita líquida de perdas", "receita efetiva" = [Receita Realizada].
- "Perda", "receita perdida", "venda perdida" = [Receita Perdida]: pedidos com Grupo do Status = "Perdido" (Cancelado ou Indisponível).
- "Atraso", "entrega atrasada", "fora do prazo" = entrega ao cliente depois da data prevista informada na compra. KPI: [% Pedidos Atrasados] (base: pedidos entregues).
- "Satisfação", "NPS", "nota" = [Nota Média de Avaliação] (escala 1 a 5). Avaliação negativa = nota 1 ou 2.
- "Cliente" = pessoa, contada por [Qtd Clientes] (Código Único do Cliente). Não conte linhas da tabela Cliente.
- "Lojista", "seller", "parceiro" = tabela Vendedor.
- "Departamento", "linha" = Produto[Categoria].
- "Estado" pode ser UF (sigla) ou Estado (nome); use UF do Cliente quando a pergunta for sobre clientes e UF do Vendedor quando for sobre vendedores.

Regras:
- Por padrão, períodos referem-se à data da compra (Calendário). Para volume de entregas por período use [Pedidos Entregues por Data de Entrega].
- Perguntas por vendedor ou produto usam medidas da tabela Vendas ([Receita Bruta], [Receita Perdida], [% Itens Atrasados]). Medidas da tabela Pedidos não são filtradas por vendedor nem produto.
- 2016 tem poucos pedidos (início da operação) e set-out/2018 está incompleto; ao comparar anos, avise sobre isso.
- Responda valores monetários em R$ com duas casas e percentuais com uma casa.
```

## 3. Verified answers

Criar a partir de visuais do relatório (*Set up verified answer*), com as frases gatilho abaixo.

| Visual | Frases gatilho |
|---|---|
| Cartão **Receita Perdida** | "quanto perdemos em vendas", "qual a receita perdida", "valor de pedidos cancelados" |
| Cartão **% Pedidos Atrasados** | "qual o percentual de atraso", "quantos pedidos chegaram atrasados", "nível de serviço de entrega" |
| Barras **% de pedidos atrasados por UF do cliente** | "onde mais atrasa", "atraso por estado", "quais estados têm mais atraso" |
| Barras **Receita perdida por categoria** | "quais categorias mais perdem venda", "perda por categoria" |
| Colunas **Nota média: entrega no prazo x atrasada** | "atraso afeta a nota", "impacto do atraso na satisfação" |

## 4. Aprovação

Depois de testar as perguntas de `test-questions.md`: **Approved for Copilot** no modelo (PRD) e endosso *Promoted*.

## Limitação conhecida

Sinônimos (linguistic schema) e a configuração do *Prep data for AI* ficam no serviço e não são versionados neste repositório; o glossário acima cobre os sinônimos principais pelas AI instructions.
