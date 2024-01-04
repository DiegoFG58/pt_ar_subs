select 
    'PT' as country,
    year(FECHA_ALTA_LOCAL) as year,
    month(FECHA_ALTA_LOCAL) as month,
    count(distinct SUBS_ID) as subs
from {{ref('SUBS_DVP4M_PT_TMP')}}
group by 2,3

union all

select 
    'AR' as country,
    year(SUBS_DATE) as year,
    month(SUBS_DATE) as month,
    count(distinct SUBS_ID) as subs
from {{ref('passwap_ar', 'dim_passwap_ar_subscriptions')}}
group by 2,3