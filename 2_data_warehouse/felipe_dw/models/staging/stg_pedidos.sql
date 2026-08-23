with source as (

    select *
    FROM {{ ref('pedidos') }}

),

staged as (

    select
        id_pedido,
        id_cliente,
        produto,
        quantidade,
        preco_unitario,
        valor_total,
        cast(data_pedido as date) as data_pedido,
        status

    from source

)

select * from staged