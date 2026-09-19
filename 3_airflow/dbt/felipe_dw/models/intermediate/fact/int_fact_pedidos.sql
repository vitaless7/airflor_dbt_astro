with pedidos as (

    select * from {{ ref('stg_pedidos') }}

),

clientes as (

    select * from {{ ref('stg_cadastros') }}

),

final as (

    select
        p.id_pedido,
        p.id_cliente,
        p.data_pedido,
        p.produto,
        p.quantidade,
        p.preco_unitario,
        p.valor_total,
        p.status,
        c.estado as estado_cliente,
        c.data_cadastro as data_cadastro_cliente

    from pedidos p
    left join clientes c
        on p.id_cliente = c.id_cliente

)

select * from final