DROP TABLE IF EXISTS tabela_final;
CREATE TABLE    tabela_final (
    nome_banco varchar,
    quantidade_reclamacoes  varchar,
    quantidade_clientes_bancos  varchar,
    -- indice_reclamacoes  float,
    indice_satisfacao_funcionarios_bancos   varchar,
    indice_satisfacao_salarios_funcionarios_bancos varchar,
    classificacao_do_banco  varchar,
    cnpj    varchar,
primary key (cnpj)
);
