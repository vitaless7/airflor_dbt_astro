with intervalo_datas as (

    select
        min(data_pedido) as data_inicio,
        max(data_pedido) as data_fim
    from {{ ref('stg_pedidos') }}

),

gerador_datas as (

    select
        generate_series(
            (select data_inicio from intervalo_datas),
            (select data_fim from intervalo_datas),
            interval '1 day'
        )::date as data_dia

),

final as (

    select
        data_dia,
        extract(year from data_dia) as ano,
        extract(month from data_dia) as mes,
        extract(day from data_dia) as dia,
        extract(quarter from data_dia) as trimestre,
        extract(dow from data_dia) as dia_da_semana,
        to_char(data_dia, 'YYYY-MM') as ano_mes

    from gerador_datas

)

select * from final