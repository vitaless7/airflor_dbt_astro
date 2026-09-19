with pedidos as (

    select * from {{ ref('int_fact_pedidos') }}

),

metricas_por_periodo as (

    select
        data_pedido,
        to_char(data_pedido, 'YYYY-MM') as ano_mes,
        extract(year from data_pedido) as ano,
        extract(month from data_pedido) as mes,
        coalesce(estado_cliente, 'Não Informado') as estado_cliente,
        
        -- Contagens de Volume
        count(distinct id_pedido) as total_pedidos,
        count(distinct id_cliente) as total_clientes_unicos,
        sum(quantidade) as total_produtos_vendidos,
        
        -- Métricas Financeiras
        sum(valor_total) as faturamento_total,
        round(avg(valor_total)::numeric, 2) as ticket_medio,
        
        -- Métricas por Status
        count(distinct case when status = 'concluido' then id_pedido end) as pedidos_concluidos,
        count(distinct case when status = 'cancelado' then id_pedido end) as pedidos_cancelados

    from pedidos
    group by 1, 2, 3, 4, 5

)

select * from metricas_por_periodo
order by data_pedido desc, estado_cliente asc