# Banco de perguntas de teste (Copilot / Data Agent)

Rodar antes de cada promoção para PRD. A coluna "Resposta esperada" diz qual medida e quais campos devem aparecer; o valor se confere no relatório.

| # | Pergunta | Resposta esperada |
|---|---|---|
| 1 | Qual foi a receita bruta em 2017? | `Receita Bruta` filtrada por `Calendário[Ano] = 2017` |
| 2 | Quanto perdemos com pedidos cancelados ou indisponíveis? | `Receita Perdida` (não `Receita Bruta` filtrada só por Cancelado) |
| 3 | Qual o percentual da receita que foi perdida por mês em 2018? | `% Receita Perdida` por `Calendário[Ano-Mês]` |
| 4 | Quais 5 categorias têm a maior receita perdida? | `Receita Perdida` por `Produto[Categoria]`, top 5 |
| 5 | Qual o percentual de pedidos entregues com atraso? | `% Pedidos Atrasados` |
| 6 | Em quais estados os clientes recebem com mais atraso? | `% Pedidos Atrasados` por `Cliente[UF do Cliente]` ou `Estado do Cliente` |
| 7 | Quais vendedores têm mais itens atrasados? | `% Itens Atrasados` por `Vendedor[Código do Vendedor]` (não `% Pedidos Atrasados`) |
| 8 | A nota da avaliação cai quando a entrega atrasa? | `Nota Média - Entrega no Prazo` x `Nota Média - Entrega Atrasada` |
| 9 | Quantos clientes compraram em 2017? | `Qtd Clientes` (cliente único), não contagem de `Cliente` |
| 10 | Qual o ticket médio por região? | `Ticket Médio` por `Cliente[Região do Cliente]` |
| 11 | Qual o prazo médio de entrega no Nordeste? | `Prazo Médio de Entrega (dias)` filtrado por `Região do Cliente = Nordeste` |
| 12 | Quanto do pagamento é feito no cartão de crédito? | `% Pago com Cartão de Crédito` |
| 13 | Como a receita de 2018 se compara com 2017? | `Receita Bruta Δ% AA`, com aviso de 2018 incompleto |
| 14 | Quantos pedidos foram entregues em agosto de 2018? | `Pedidos Entregues por Data de Entrega` (data de entrega, não de compra) |
| 15 | Qual o frete médio sobre o valor dos produtos em RR? | `% Frete sobre Produtos` por `UF do Cliente = RR` |

Registrar falhas (pergunta, resposta do Copilot, causa) e corrigir no modelo (descrição, nome, AI instructions), nunca só no prompt do usuário.
