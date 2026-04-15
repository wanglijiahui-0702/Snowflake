---Buyer NPS --------------------------------------------------------------------------------------
select "O"."BUYER_ID",
	"O"."ID",
	"OI"."ID",
	"B"."EMAIL_ADDRESS",
	"O"."ORDERED_AT_ET",
	"OI"."TOTAL_USD",
	"OI"."PRODUCT_LINE",
	"OI"."PRODUCT_TYPE",
	"OI"."QUANTITY" 
from "ANALYTICS"."CORE"."ORDERS" as "O" 
	inner join "ANALYTICS"."CORE"."ORDER_ITEMS" as "OI" on "O"."ID" = "OI"."ORDER_ID" 
	inner join "ANALYTICS"."CORE"."BUYERS" as "B" on "B"."ID" = "O"."BUYER_ID" 
where "O"."ORDERED_AT_ET" >= dateadd(month, -24, current_date()) 
	and "O"."STATUS" = 'Complete' 
	and "O"."CHANNEL" = 'Marketplace'

select "O"."BUYER_ID",
	"O"."ID",
	"DO"."ID",
	"O"."SUBTOTAL_USD",
	"DO"."PRODUCT_VALUE_USD" 
from "ANALYTICS"."CORE"."DIRECT_ORDERS" as "DO" 
	inner join "ANALYTICS"."CORE"."ORDERS" as "O" on "DO"."ORDER_ID" = "O"."ID" 
where "O"."ORDERED_AT_ET" >= dateadd(month, -24, current_date())

select * 
from "ANALYTICS"."CORE"."ORDERS" as "O" 
where "O"."STATUS" = 'Complete' 
	and "O"."CHANNEL" = 'Marketplace' 
	and "O"."ORDERED_AT_ET" >= dateadd(month, -24, current_date())

select * 
from "ANALYTICS"."CORE"."ORDERS" as "O" 
where "O"."STATUS" = 'Complete' 
	and "O"."CHANNEL" = 'Marketplace' 
	and "O"."ORDERED_AT_ET" >= dateadd(month, -6, current_date())

select "ANALYTICS"."CORE"."USERS"."ID",
	"ANALYTICS"."CORE"."USERS"."EMAIL_ADDRESS",
	"ANALYTICS"."CORE"."USERS"."CREATED_AT_ET",
 "ANALYTICS"."CORE"."USERS"."EXTERNAL_USER_ID",
from "ANALYTICS"."CORE"."USERS"

select * 
from "ANALYTICS"."CORE"."ORDERS" as "O" 
where "O"."STATUS" = 'Complete' 
	and "O"."CHANNEL" = 'Marketplace' 
	and "O"."ORDERED_AT_ET" >= dateadd(month, -12, current_date())


--- Seller NPS -------------------------------------------------------------------------------------
--- GMV Cohorts ---
SELECT
    sellers.entity_id
    , max(sellers.key) as seller_key
    , max(sellers.entity_name) as entity_name
    , sum(seller_gmv_by_month.gmv) / 3 as average_30_day_sales_past_90
    , max(sellers.contact_person_name) as Name
    , max(sellers.contact_phone_number) as Phone_Number
    , max(sellers.contact_email) as Email
--    , max(sellers.admin_url) as Admin_URL
    , CASE 
        WHEN average_30_day_sales_past_90 >= 30000 THEN 'Enterprise' 
        WHEN average_30_day_sales_past_90 >= 7500 AND average_30_day_sales_past_90 < 30000 THEN 'Professional'
        WHEN average_30_day_sales_past_90 >= 1000 AND average_30_day_sales_past_90 < 7500 THEN 'Pre-Professional'
        WHEN average_30_day_sales_past_90 < 1000 THEN 'Hobbyist'
        END AS gmv_cohort
FROM (
    SELECT
        seller_orders.seller_id
        --, min(seller_orders.ordered_at_et) as oldestsale
        --, max(seller_orders.ordered_at_et) as newestsale
        , sum(seller_orders.order_amount_usd) as gmv
    FROM analytics.core.seller_orders
    INNER JOIN analytics.core.orders on orders.id = seller_orders.order_id
    WHERE seller_orders.ordered_at_et > dateadd(day, -91, getdate())
        AND seller_orders.ordered_at_et <= dateadd(day, -1, getdate())
        AND orders.channel = 'Marketplace'
        AND seller_orders.seller_order_status in ('Complete','Partial Refund','Full Refund') --""Good Orders""
    GROUP BY seller_orders.seller_id
    ) seller_gmv_by_month
     
INNER JOIN analytics.core.sellers on sellers.id = seller_gmv_by_month.seller_id
GROUP BY sellers.entity_id
ORDER BY 
    CASE 
        WHEN gmv_cohort = 'Enterprise' THEN 1
        WHEN gmv_cohort = 'Professional' THEN 2
        WHEN gmv_cohort = 'Pre-Professional' THEN 3
        WHEN gmv_cohort = 'Hobbyist' THEN 4
        END asc

---Contact SGS ---
select "S"."ENTITY_ID" 
from "FIVETRAN"."HUBSPOT"."ENGAGEMENT_MEETING" as "CA" 
	inner join "FIVETRAN"."HUBSPOT"."ENGAGEMENT_COMPANY" as "CO" on "CA"."ENGAGEMENT_ID" = "CO"."ENGAGEMENT_ID" 
	inner join "ANALYTICS"."CORE"."SELLERS" as "S" on "CO"."COMPANY_ID" = "S"."ENTITY_ID" 
where "CA"."PROPERTY_HS_MEETING_OUTCOME" = 'COMPLETED' 
	and "CA"."PROPERTY_HS_CREATEDATE" >= DATEADD(month, -3, CURRENT_DATE())


select "S"."ENTITY_ID" 
from "FIVETRAN"."HUBSPOT"."ENGAGEMENT_CALL" as "CA" 
	inner join "FIVETRAN"."HUBSPOT"."ENGAGEMENT_COMPANY" as "CO" on "CA"."ENGAGEMENT_ID" = "CO"."ENGAGEMENT_ID" 
	inner join "ANALYTICS"."CORE"."SELLERS" as "S" on "CO"."COMPANY_ID" = "S"."ENTITY_ID" 
where "CA"."PROPERTY_HS_CALL_STATUS" = 'COMPLETED' 
	and "CA"."PROPERTY_HS_CREATEDATE" >= DATEADD(month, -3, CURRENT_DATE())

---BPOS---
select "C"."EMAIL" 
from "FIVETRAN"."BINDERPOS_GCP_MYSQL_BINLOG_BINDERPOS"."BINDERCUSTOMER" as "C"

---CX---
select distinct "S"."ENTITY_ID",
	"S"."CONTACT_EMAIL",
	"U"."EMAIL",
	"T"."ACCOUNT_EMAIL" 
from "SEGMENT"."ZENDESK"."TICKETS" as "T" 
	inner join "SEGMENT"."ZENDESK"."TICKET_FORMS" as "TF" on "T"."TICKET_FORM_ID" = "TF"."ID" 
	left join "ANALYTICS"."CORE"."SELLER_ORDERS" as "SO" on "SO"."ORDER_NUMBER" = "T"."ORDER_NUMBER_S" 
	left join "ANALYTICS"."CORE"."SELLERS" as "S" on "S"."ID" = "SO"."SELLER_ID" 
	left join "SEGMENT"."ZENDESK"."USERS" as "U" on "U"."ID" = "T"."REQUESTER_ID" 
where "TF"."NAME" in ('CX - Order Related - Seller', 'CX - Account Related - Seller') 
	and "T"."RECEIVED_AT" >=  DATEADD(month, -3, CURRENT_DATE())

---GAP---
select distinct "FIVETRAN"."HUBSPOT"."DEAL_COMPANY"."COMPANY_ID" as "GAP_SELLERS",
	'Yes' as "IS_GAP_SELLER" 
from "FIVETRAN"."HUBSPOT"."DEAL" 
	inner join "FIVETRAN"."HUBSPOT"."DEAL_COMPANY" on "FIVETRAN"."HUBSPOT"."DEAL"."DEAL_ID" = "FIVETRAN"."HUBSPOT"."DEAL_COMPANY"."DEAL_ID" 
where "FIVETRAN"."HUBSPOT"."DEAL"."DEAL_PIPELINE_ID" in ('45558649', '47451233', '47008565', '64117055') 
	and "FIVETRAN"."HUBSPOT"."DEAL"."DEAL_PIPELINE_STAGE_ID" is not NULL 
	and "FIVETRAN"."HUBSPOT"."DEAL"."DEAL_PIPELINE_STAGE_ID" not in ('94669473', '97804057', '98030985', '98257674', '98252363', '99067308', '125777922', '125815337')

---