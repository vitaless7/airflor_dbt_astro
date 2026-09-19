with source as (

    select * 
    FROM {{ ref('cadastros') }}

),

renamed as (

    select
        id_cliente,
        nome,
        data_nascimento,
        cpf,
        estado,
        data_cadastro

    from source

)

select * from renamed