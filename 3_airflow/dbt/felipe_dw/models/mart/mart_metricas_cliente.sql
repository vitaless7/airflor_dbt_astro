with int_pedidos as (

    select * from {{ ref('int_fact_pedidos') }}

),

final as (

    select
        id_pedido,
        id_cliente,
        data_pedido,
        produto,
        quantidade,
        preco_unitario,
        valor_total,
        status,
        estado_cliente,
        
        -- Métricas calculadas no nível de linha/pedido
        round((valor_total / nullif(quantidade, 0))::numeric, 2) as preco_medio_item,
        extract(year from data_pedido) as ano_venda,
        extract(month from data_pedido) as mes_venda,
        to_char(data_pedido, 'YYYY-MM') as ano_mes_venda

    from int_pedidos

)

select * from final