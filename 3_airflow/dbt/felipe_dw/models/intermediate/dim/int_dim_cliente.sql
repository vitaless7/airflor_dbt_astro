with clientes as (

    select * from {{ ref('stg_cadastros') }}

),

pedidos as (

    select * from {{ ref('stg_pedidos') }}

),

metricas_pedidos as (

    select
        id_cliente,
        min(data_pedido) as primeira_data_pedido,
        max(data_pedido) as ultima_data_pedido,
        count(distinct id_pedido) as total_pedidos,
        sum(valor_total) as valor_total_gasto

    from pedidos
    group by 1

),

final as (

    select
        c.id_cliente,
        c.nome,
        c.cpf,
        c.estado,
        c.data_nascimento,
        c.data_cadastro,
        coalesce(m.total_pedidos, 0) as total_pedidos,
        coalesce(m.valor_total_gasto, 0) as valor_total_gasto,
        m.primeira_data_pedido,
        m.ultima_data_pedido

    from clientes c
    left join metricas_pedidos m
        on c.id_cliente = m.id_cliente

)

select * from final