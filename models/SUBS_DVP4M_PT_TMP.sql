SELECT 
    S.ID AS SUBS_ID
    ,S.SUBSCRIPTION_DATE                AS FECHA_ALTA_LOCAL
    ,S.EXPIRATION_DATE                  AS FECHA_BAJA_LOCAL
    ,S.CANCELLATION_DATE                AS FECHA_BAJA_DIFERIDA
    ,S.LAST_INVOICE_DATE               AS FECHA_ULTIMO_PAGO
    ,OP.BILLING_CHANNEL_ID
    ,OP.BILLING_KINEMATIC_ID
    ,OP.BILLING_PROVIDER_JOB_ID
    ,OP.NETWORK_CHANNEL_ID
    ,OP.ORDER_CHANNEL_ID
    ,OP.OFFER_ID
    ,OP.SKU_ID
    ,OP.PACK_ID
    ,OP.COUNTRY_ID
    ,OP.END_USER_PROVIDER_JOB_ID
    ,COALESCE(parse_json(U.data):ad_id::string,'UNKNOWN') as ads_id
    ,COALESCE(parse_json(U.data):mccmnc::string,'UNKNOWN') as mccmnc
    ,COALESCE(parse_json(U.data):msisdn::string,'UNKNOWN') as msisdn
    ,COALESCE(parse_json(U.data):ad_network::string,'UNKNOWN') as provider
    ,COALESCE(parse_json(U.data):device_type::string,'UNKNOWN') as device_type      
    ,COALESCE(AD.CAMPAIGN_NAME,'UNKNOWN')                     as CAMPAIGN
    ,'UNKNOWN' as INVENTORY
    ,'UNKNOWN' as CANAL
    ,'UNKNOWN' as KEYWORD
    ,'UNKNOWN' as AFILIADO
    ,COALESCE(AD.AD_URL,'UNKNOWN')                                 as AD_URL  
    ,convert_timezone('Europe/Paris',current_timestamp) AS timestamp

FROM {{ source('dvp4m','SUBSCRIPTIONS')}}  S
INNER JOIN {{ source('dvp4m','OPERATIONS')}}  OP 
ON S.ID=OP.ID
INNER JOIN {{ source('dvp4m','USERS')}} U
ON S.ID=U.RELATED_ID
LEFT JOIN {{ source('tracking','REF_AD_ALL')}}    AD     ON AD.TRACKING_id = parse_json(U.DATA):ad_id::string
                                                    and lower(parse_json(U.data):ad_network::string) = lower(source)

WHERE (S.EXPIRATION_DATE is null or S.EXPIRATION_DATE >= dateadd(month, -1, convert_timezone('Europe/Paris',current_date)))
AND OP.TYPE=2            
AND OP.STATUS =2     
AND OP.COUNTRY_ID= 'PT'