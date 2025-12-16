












with base_dates as (

    
    with date_spine as
(

    





with rawdata as (

    

    

    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select

    
    p0.generated_number * power(2, 0)
    
    
    + 1
    as generated_number

    from

    
    p as p0
    
    

    )

    select *
    from unioned
    where generated_number <= 1
    order by generated_number



),

all_periods as (

    select (
        

        datetime_add(
            cast( cast('2022-01-01T00:00:00' as datetime ) as datetime),
        interval (row_number() over (order by 1) - 1) day
        )


    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= cast('2022-01-02 01:00:00' as datetime )

)

select * from filtered



)
select
    cast(d.date_day as timestamp) as date_day
from
    date_spine d


    

),
model_data as (

    select
    

        cast(timestamp_trunc(
        cast(data as timestamp),
        day
    ) as datetime) as date_day,

    

        count(*) as row_cnt
    from
        
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`dashboard_subsidio_sppo`.`sumario_servico_dia` where data between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00') and data < date('2024-08-16')) f
    
    where 1=1
    
    group by
        date_day

),

final as (

    select
        cast(d.date_day as datetime) as date_day,
        case when f.date_day is null then true else false end as is_missing,
        coalesce(f.row_cnt, 0) as row_cnt
    from
        base_dates d
        left join
        model_data f on cast(d.date_day as datetime) = f.date_day
)
select
    *
from final
where row_cnt = 0
