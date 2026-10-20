# ADR 0001 - Um lakehouse por camada, com schemas

- **Status:** aceito
- **Data:** 2026-10-20

## Contexto

A arquitetura Medallion pode ser montada com um único lakehouse (schemas `bronze`, `silver`, `gold`) ou com um lakehouse por camada. O projeto precisa mostrar separação de responsabilidade e de acesso, e ainda rodar numa capacidade trial.

## Decisão

Três lakehouses com schemas habilitados:

| Lakehouse | Schemas | Quem acessa |
|---|---|---|
| `lh_bronze` | `olist` (sistema de origem) | Engenharia |
| `lh_silver` | `commercial` (domínio), `framework` (run log e DQ), `quarantine` | Engenharia, ciência de dados |
| `lh_gold` | `commercial`, `shared` (dimensões conformadas) | Modelo semântico, analistas |

Permissão por item: usuários de negócio só recebem acesso ao modelo semântico; ninguém fora da engenharia lê `lh_bronze`.

## Alternativas

- **Um lakehouse com schemas por camada:** menos itens, mas permissão só por schema (OneLake security) e mistura de dados brutos e curados no mesmo item.
- **Warehouse na Gold:** faria sentido com equipe T-SQL ou transações multi-tabela. Aqui toda a transformação é Spark, e o Lakehouse já entrega SQL analytics endpoint para consulta.
- **Controle do ELT em Warehouse ou SQL database (`wh_control`):** é o padrão para ambientes com muitas tabelas e configuração por metadado. Neste projeto (9 tabelas de origem) o run log e o DQ ficam como Delta no `lh_silver.framework`, o que basta e não exige mais um item.

## Consequências

- Os notebooks não têm lakehouse padrão: resolvem os caminhos OneLake pelo nome do lakehouse no workspace em execução (`nb_fw_utils.lakehouse_root`). Isso dispensa regras de deployment para notebooks.
- Três SQL analytics endpoints em vez de um.
