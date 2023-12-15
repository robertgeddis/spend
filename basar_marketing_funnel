with spend as (
  -- SEM 
    select distinct
       dd.year, dd.month, dd.date,
       upper(sm.country) as country,  
       case when (sm.source_type = 'GDN' or sm.source = 'YT') then 'Other Online' 
            when ( (sm.source_type <> 'GDN' or sm.source <> 'YT') and (lower(sm.vertical) = 'brand' or lower(sm.vertical) is null) ) then 'SEM BR'
            when ( (sm.source_type <> 'GDN' or sm.source <> 'YT') and lower(sm.vertical) = 'childcare' )  then 'SEM CC'
            when ( (sm.source_type <> 'GDN' or sm.source <> 'YT') and lower(sm.vertical) = 'homecare' )  then 'SEM HK'
            when ( (sm.source_type <> 'GDN' or sm.source <> 'YT') and lower(sm.vertical) = 'petcare' ) then 'SEM PC'
            when ( (sm.source_type <> 'GDN' or sm.source <> 'YT') and lower(sm.vertical) = 'aupair' ) then 'SEM AP' 
            when ( (sm.source_type <> 'GDN' or sm.source <> 'YT') and lower(sm.vertical) = 'seniorcare' ) then 'SEM SC'         
            when ( (sm.source_type <> 'GDN' or sm.source <> 'YT') and lower(sm.vertical) in ('specialneeds', 'tutoring') ) then 'SEM Other'  
         else concat(sm.vertical, source_type) end as category,   
       case when sm.source_type = 'GDN' then 'GDN'
            when sm.source = 'YT' then 'YouTube'
            when (sm.source_type <> 'GDN' or sm.source <> 'YT') then 'SEM'
         end as channel,
       sm.currency,   
       sum(sm.cost) as spend 
    from intl.dw_f_campaign_spend_intl sm
      join reporting.DW_D_DATE dd             on date(sm.date) = dd.date   
    where dd.year >= year(now())-1
      and dd.date < date(current_date)  
      and lower(sm.campaign_type) = 'seeker' 
      and sm.country is not null
    group by 1,2,3,4,5,6,7
  
  union

  -- FACEBOOK
    select distinct
      dd.year, dd.month, dd.date,
      case when upper(fb.country) = 'EN' then 'CA' 
           when upper(fb.country) = '_'  then 'AU' 
           when upper(fb.country) = 'VE'  then 'DE' 
        else upper(fb.country) end as country,
      'Social' as category,
      'Facebook' as channel,
      fb.currency,
      sum(fb.spend) as spend
    from intl.DW_F_FACEBOOK_SPEND_INTL fb
      join reporting.DW_D_DATE dd             on date(fb.date_start) = dd.date 
    where dd.year >= year(now())-1
      and dd.date < date(current_date)  
      and lower(fb.campaign_Type) = 'seeker'
    group by 1,2,3,4,5,6,7
  
  union

  -- QUALITY CLICK
        select distinct
          dd.year, dd.month, dd.date,     
          case when program in ('DE', 'AT', 'CH') then program
                when product like '%FR%' then 'FR'
                when product like '%BE_nl%' then 'BE'  
                when product like '%BE_fr%' then 'FB'  
                when product like '%DK%' then 'DK'  
                when product like '%FI%' then 'FI'  
                when product like '%NL%' then 'NL'  
                when product like '%SE%' then 'SE'  
                when product like '%IE%' then 'IE'  
                when product like '%ES%' then 'ES'  
                when product like '%AU%' then 'AU'  
                when product like '%NO%' then 'NO'
                when product like '%NZ%' then 'NZ'
                when product like '%UK%' then 'UK'
                when product like '%CA%' then 'CA'
           end as country, 
          'Other Online' as category,
          'Quality Click' as channel,
          'EUR' as currency,
           sum(qc.commission) as spend
        from intl.quality_click_spend qc
          join reporting.DW_D_DATE dd     on date(qc.day) = dd.date     
        where partnerid <> 435
          and (lower(product) not like '%alltagshelfer%' or lower(product) not like '%provider%')
          and dd.year >= year(now())-1
          and dd.date < date(current_date)  
        group by 1,2,3,4,5,6,7
  
  union

  -- PUTZCHECKER (DE Only)
      select distinct
        dd.year, dd.month, dd.date,  
        'DE' as country, 'Other Online' as category, 'Putzchecker' as channel, 'EUR' as currency,
        ifnull((sum(clicks)*1.5),0) as spend
      from intl.quality_click_cpc qc
        join reporting.DW_D_DATE dd on date(qc.day) = dd.date
      where dd.year >= year(now())-1
        and dd.date < date(current_date)  
      group by 1,2,3,4,5,6,7
  
  union

  -- MIBABY (DE Seekers)
    select distinct dd.year, dd.month, dd.date, 'DE' as country, 'Other Online' as category, 'MiBaby' as channel, 'EUR' as currency, ifnull(sum(DE_Seeker_Mibaby),0) as spend 
    from intl.DW_MARKETING_SPEND_INTL ms join reporting.DW_D_DATE dd on ms.spend_date = dd.date where dd.year >= year(now())-1 and dd.date < date(current_date)  
    group by 1,2,3,4,5,6,7

  union

  -- AWIN
    select distinct
      dd.year, dd.month, dd.date,
      case when aw.advertiser_id = '10557' then 'DE'
           when aw.advertiser_id = '10709' then 'AT'   
           when aw.advertiser_id = '45671' then 'UK' 
      end as country,  
    	'Other Online' as category,
    	'Awin' as channel,
    	case when aw.advertiser_id in ('10557', '10709') then 'EUR' else 'GBP' end as currency,   	   
      (sum(aw.commission_amount)*1.3) as spend
    from intl.awin_spend aw
      join reporting.DW_D_DATE dd             on date(aw.transaction_Date) = dd.date
    where dd.year >= year(now())-1
      and dd.date < date(current_date)  
      and aw.commission_group_code not in ('REG_P','REGP') 
      and lower(aw.commissionStatus) in ('approved', 'pending')
  	group by 1,2,3,4,5,6,7
	
  union

  -- MEINESTADT (DE Only)
    select distinct
       dd.year, dd.month, dd.date,
       'DE' as country, 'Other Online' as category, 'Meinestadt' as channel, 'EUR' as currency,
       sum(case when spend.year = spend.current_year and spend.month = spend.current_month then ((spend)/current_days) else ((spend)/days_in_month) end) as spend
    from (
      select distinct
        year(sp.subscriptionDateCreated) as year,
        month(sp.subscriptionDateCreated) as month,
        date_part('day', last_day(sp.subscriptionDateCreated)) as days_in_month,
        date_part('day', current_date()-1) as current_days,
        month(current_date()-1) as current_month,
        year(current_date()-1) as current_year,
        'EUR' as currency,
        count(distinct sp.subscriptionId) as premiums,
        case when count(distinct sp.subscriptionId)<=150 then (count(distinct sp.subscriptionId)*80) 
          when count(distinct sp.subscriptionId)>150 then (150*80)+((count(distinct sp.subscriptionId)-150)*120) end as 'Spend' 
      from intl.transaction t
        join intl.hive_subscription_plan sp on sp.subscriptionId = t.subscription_plan_id and sp.countrycode = t.country_code
          and year(sp.subscriptionDateCreated) >= year(current_date())-1 and date(sp.subscriptionDateCreated) < current_date()
        join intl.hive_member m on t.member_id = m.memberid and t.country_code = sp.countrycode and date(m.dateFirstPremiumSignup) = date(sp.subscriptionDateCreated)
          and m.IsInternalAccount is not true
          and lower(m.role) = 'seeker' and lower(m.audience) = 'seeker'
          and lower(m.campaign) = 'online' and lower(m.site) = 'meinestadt.de' 
      where t.type in ('PriorAuthCapture','AuthAndCapture') and t.status = 'SUCCESS' and t.amount > 0
        and t.country_code = 'de'      
        and year(t.date_created) >= year(now())-1 and date(t.date_created) < current_date()
      group by 1,2,3,4,5,6,7
    ) spend
      join reporting.DW_D_DATE dd             on spend.year = dd.year and spend.month = dd.month and dd.date < date(current_date)
      group by 1,2,3,4,5,6,7
      
  union   

  -- PINTEREST (DE, UK, CA + AU Only)
    select distinct dd.year, dd.month, dd.date, 'DE' as country, 'Social' as category, 'Pinterest' as channel, 'EUR' as currency, sum(DE_Seeker_Pinterest) as spend 
    from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE dd on spend_date = dd.date where dd.year >= year(now())-1 and dd.date < date(current_date) group by 1,2,3,4,5,6,7 
    union
    select distinct dd.year, dd.month, dd.date, 'UK' as country, 'Social' as category, 'Pinterest' as channel, 'EUR' as currency, sum(UK_Seeker_Pinterest) as spend  
    from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE dd on spend_date = dd.date where dd.year >= year(now())-1 and dd.date < date(current_date) group by 1,2,3,4,5,6,7  
    union
    select distinct dd.year, dd.month, dd.date, 'CA' as country, 'Social' as category, 'Pinterest' as channel, 'EUR' as currency, sum(CA_Seeker_Pinterest) as spend
    from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE dd on spend_date = dd.date where dd.year >= year(now())-1 and dd.date < date(current_date) group by 1,2,3,4,5,6,7 
    union
    select distinct dd.year, dd.month, dd.date, 'AU' as country, 'Social' as category, 'Pinterest' as channel, 'EUR' as currency, sum(AU_Seeker_Pinterest) as spend 
    from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE dd on spend_date = dd.date where dd.year >= year(now())-1 and dd.date < date(current_date) group by 1,2,3,4,5,6,7
 
  union

  -- IMPACT (CA only)
    select distinct dd.year, dd.month, dd.date, 'CA' as country, 'Other Online' as category, 'Impact' as channel, 'EUR' as currency, sum(CA_OTHER_ONLINE) as spend 
    from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE dd on spend_date = dd.date where dd.year >= year(now())-1 and dd.date < date(current_date) group by 1,2,3,4,5,6,7  

  union    
  -- Spotify  
    select distinct dd.year, dd.month, dd.date, 'DE' as country, 'Other Online' as category, 'Spotify' as channel, 'EUR' as currency, sum(DE_SEEKER_SPOTIFY) as spend
    from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE dd on spend_date = dd.date where dd.year >= year(now())-1 and dd.date < date(current_date) group by 1,2,3,4,5,6,7 
    
  union
  
  -- TV
    select distinct
      dd.year, dd.month, dd.date,
      'DE' as country, 'TV' as category, 'TV' as channel, 'EUR' as currency,
      sum(DE_TV) as spend
    from intl.DW_MARKETING_SPEND_INTL
      join reporting.DW_D_DATE dd on date(spend_date) = dd.date
    where dd.year >= year(now())-1
      and dd.date < date(current_date)
    group by 1,2,3,4,5,6,7     
),

fixed_current_fx as (
   select distinct
    dd.year, dd.month, dd.date,
    source_currency as currency,
    currency_rate as fx_rate
  from reporting.DW_CARE_FX_RATES fx 
  join reporting.DW_D_DATE dd on current_rate_date = dd.date 
  where target_currency = 'USD'
    and currency_rate_type = 'Current'
    and source_currency in ('EUR','GBP','CAD','AUD')
  group by 1,2,3,4,5
)
  
select
  s.year, s.month, s.date,
  s.category as spend_category, s.channel as source,
  s.country, s.currency, f.fx_rate,  
  ifnull(sum(s.spend * f.fx_rate),0) as Spend
  
from spend s
  join fixed_current_fx f         on s.currency = f.currency

group by 1,2,3,4,5,6,7,8 order by 1,2,3 asc
