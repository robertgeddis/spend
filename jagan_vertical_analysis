with spend as ( 
  select 
     dd.year, dd.month, dd.date, 
     upper(sm.country) as country, 
     case when sm.vertical is null then 'Brand' 
          when sm.vertical = 'HomeCare' then 'Housekeeping'     
        else initcap(sm.vertical) end as vertical,
     case when source_type = 'GDN' then 'GDN'
          when source = 'YT' then 'YouTube'
        else 'SEM' end as channel,
     sm.currency,          
     sum(sm.cost) as spend        
  from intl.dw_f_campaign_spend_intl sm
    join reporting.DW_D_DATE dd on date(sm.date) = dd.date                                         
  where sm.country is not null
    and lower(campaign_type) = 'seeker'
    and dd.year >= year(now())-1
    and dd.date < date(current_date)
  group by 1,2,3,4,5,6,7
  
  union
  
  select distinct
    dd.year, dd.month, dd.date, 
    case when upper(fb.country) = 'EN' then 'CA' 
         when upper(fb.country) = '_'  then 'AU' 
         when upper(fb.country) = 'VE'  then 'DE' 
      else upper(fb.country) end as country, 
    case when vertical = 'Child Care' then 'Childcare'
         when vertical = 'PetCare' then 'Petcare'
         when vertical = 'HouseKeeping' then 'Housekeeping'
         when vertical = 'Turtoring' then 'Tutoring'
         when vertical = 'other' then 'Brand' end as vertical, 
    'Facebook' as channel,
    fb.currency,     
    sum(fb.spend) as spend
  from intl.DW_F_FACEBOOK_SPEND_INTL fb
    join reporting.DW_D_DATE dd on date(date_start) = dd.date
  where lower(fb.campaign_Type) = 'seeker'
    and dd.year >= year(now())-1
    and dd.date < date(current_date)
  group by 1,2,3,4,5,6,7
  
  union
  
  select distinct
   dd.year, dd.month, dd.date,
   'DE' as country, vertical, 'Meinestadt' as channel, 'EUR' as currency,
   sum(case when spend.year = spend.current_year and spend.month = spend.current_month then ((spend)/current_days) else ((spend)/days_in_month) end) as spend
  from (
    select distinct
      year(sp.subscriptionDateCreated) as year,
      month(sp.subscriptionDateCreated) as month,
      date_part('day', last_day(sp.subscriptionDateCreated)) as days_in_month,
      date_part('day', current_date()-1) as current_days,
      month(current_date()-1) as current_month,
      year(current_date()-1) as current_year,
      case when lower(m.service) in ('br', 'all') then 'Brand'
           when (lower(m.service) not in ('br', 'all') and m.vertical = 'homeCare') then 'Housekeeping' 
           when (lower(m.service) not in ('br', 'all') and (m.vertical is null or m.vertical = '')) then 'Childcare' 
        else initcap(lower(m.vertical)) end as vertical,
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
    group by 1,2,3,4,5,6,7,8
  ) spend
    join reporting.DW_D_DATE dd             on spend.year = dd.year and spend.month = dd.month and dd.date < date(current_date)
    group by 1,2,3,4,5,6,7
    
    union
    
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
       'Brand' as vertical,
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

  select distinct
    dd.year, dd.month, dd.date,  
    'DE' as country, 'Brand' as vertical, 'Putzchecker' as channel, 'EUR' as currency,
    ifnull((sum(clicks)*1.5),0) as spend
  from intl.quality_click_cpc qc
    join reporting.DW_D_DATE dd on date(qc.day) = dd.date
  where dd.year >= year(now())-1
    and dd.date < date(current_date)  
  group by 1,2,3,4,5,6,7
  
  union

  select distinct dd.year, dd.month, dd.date, 'DE' as country, 'Brand' as vertical, 'MiBaby' as channel, 'EUR' as currency, ifnull(sum(DE_Seeker_Mibaby),0) as spend 
  from intl.DW_MARKETING_SPEND_INTL ms join reporting.DW_D_DATE dd on ms.spend_date = dd.date where dd.year >= year(now())-1 and dd.date < date(current_date)  
  group by 1,2,3,4,5,6,7

  union

  select distinct
    dd.year, dd.month, dd.date,
    case when aw.advertiser_id = '10557' then 'DE'
         when aw.advertiser_id = '10709' then 'AT'   
         when aw.advertiser_id = '45671' then 'UK' 
    end as country,  
  	'Brand' as vertical,
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

  select distinct dd.year, dd.month, dd.date, 'DE' as country, 'Brand' as vertical, 'Pinterest' as channel, 'EUR' as currency, sum(DE_Seeker_Pinterest) as spend 
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE dd on spend_date = dd.date where dd.year >= year(now())-1 and dd.date < date(current_date) group by 1,2,3,4,5,6,7 
  union
  select distinct dd.year, dd.month, dd.date, 'UK' as country, 'Brand' as vertical, 'Pinterest' as channel, 'EUR' as currency, sum(UK_Seeker_Pinterest) as spend  
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE dd on spend_date = dd.date where dd.year >= year(now())-1 and dd.date < date(current_date) group by 1,2,3,4,5,6,7  
  union
  select distinct dd.year, dd.month, dd.date, 'CA' as country, 'Brand' as vertical, 'Pinterest' as channel, 'EUR' as currency, sum(CA_Seeker_Pinterest) as spend
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE dd on spend_date = dd.date where dd.year >= year(now())-1 and dd.date < date(current_date) group by 1,2,3,4,5,6,7 
  union
  select distinct dd.year, dd.month, dd.date, 'AU' as country, 'Brand' as vertical, 'Pinterest' as channel, 'EUR' as currency, sum(AU_Seeker_Pinterest) as spend 
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE dd on spend_date = dd.date where dd.year >= year(now())-1 and dd.date < date(current_date) group by 1,2,3,4,5,6,7
 
  union

  select distinct dd.year, dd.month, dd.date, 'CA' as country, 'Brand' as vertical, 'Impact' as channel, 'EUR' as currency, sum(CA_OTHER_ONLINE) as spend 
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE dd on spend_date = dd.date where dd.year >= year(now()) and dd.date < date(current_date) group by 1,2,3,4,5,6,7  
),

live_fx_rate as (
      select distinct year, month, date, currency,
        cast(case 
               when year = 2022 and month = 1 and currency = 'EUR' then '1.135125807' 
               when year = 2022 and month = 2 and currency = 'EUR' then '1.132148387' 
               when year = 2022 and month = 3 and currency = 'EUR' then '1.102175' 
               when year = 2022 and month = 4 and currency = 'EUR' then '1.089916129' 
               when year = 2022 and month = 5 and currency = 'EUR' then '1.054093333' 
               when year = 2022 and month = 6 and currency = 'EUR' then '1.060787097' 
               when year = 2022 and month = 7 and currency = 'EUR' then '1.024931035' 
               when year = 2022 and month = 8 and currency = 'EUR' then '1.015929032' 
               when year = 2022 and month = 9 and currency = 'EUR' then '0.995993807' 
               when year = 2022 and month = 10 and currency = 'EUR' then '0.978726667' 
               when year = 2022 and month = 11 and currency = 'EUR' then '1.015' 
               when year = 2022 and month = 12 and currency = 'EUR' then '1.053843333'    
               when year = 2022 and month = 1 and currency = 'GBP' then '1.356309677'
               when year = 2022 and month = 2 and currency = 'GBP' then '1.352270968' 
               when year = 2022 and month = 3 and currency = 'GBP' then '1.319542857' 
               when year = 2022 and month = 4 and currency = 'GBP' then '1.304335484' 
               when year = 2022 and month = 5 and currency = 'GBP' then '1.243223333' 
               when year = 2022 and month = 6 and currency = 'GBP' then '1.239858065' 
               when year = 2022 and month = 7 and currency = 'GBP' then '1.201493103' 
               when year = 2022 and month = 8 and currency = 'GBP' then '1.205412903' 
               when year = 2022 and month = 9 and currency = 'GBP' then '1.148293129' 
               when year = 2022 and month = 10 and currency = 'GBP' then '1.117466667' 
               when year = 2022 and month = 11 and currency = 'GBP' then '1.168825807' 
               when year = 2022 and month = 12 and currency = 'GBP' then '1.216903333'   
               when year = 2022 and month = 1 and currency = 'CAD' then '0.791389836'
               when year = 2022 and month = 2 and currency = 'CAD' then '0.785626121' 
               when year = 2022 and month = 3 and currency = 'CAD' then '0.788131793' 
               when year = 2022 and month = 4 and currency = 'CAD' then '0.795315238' 
               when year = 2022 and month = 5 and currency = 'CAD' then '0.777042905' 
               when year = 2022 and month = 6 and currency = 'CAD' then '0.782231497' 
               when year = 2022 and month = 7 and currency = 'CAD' then '0.772645084' 
               when year = 2022 and month = 8 and currency = 'CAD' then '0.776468579' 
               when year = 2022 and month = 9 and currency = 'CAD' then '0.758076978' 
               when year = 2022 and month = 10 and currency = 'CAD' then '0.72823848' 
               when year = 2022 and month = 11 and currency = 'CAD' then '0.743626151' 
               when year = 2022 and month = 12 and currency = 'CAD' then '0.736560791'  
               when year = 2022 and month = 1 and currency = 'AUD' then '0.721275883'
               when year = 2022 and month = 2 and currency = 'AUD' then '0.713160948' 
               when year = 2022 and month = 3 and currency = 'AUD' then '0.734158146' 
               when year = 2022 and month = 4 and currency = 'AUD' then '0.743213151' 
               when year = 2022 and month = 5 and currency = 'AUD' then '0.703994424' 
               when year = 2022 and month = 6 and currency = 'AUD' then '0.706960713' 
               when year = 2022 and month = 7 and currency = 'AUD' then '0.684804955' 
               when year = 2022 and month = 8 and currency = 'AUD' then '0.697490292' 
               when year = 2022 and month = 9 and currency = 'AUD' then '0.658790659' 
               when year = 2022 and month = 10 and currency = 'AUD' then '0.636335818' 
               when year = 2022 and month = 11 and currency = 'AUD' then '0.656732976' 
               when year = 2022 and month = 12 and currency = 'AUD' then '0.674049273'    
           end as float) as fx_rate   
      from
      (select distinct
        dd.year, dd.month, dd.date
       ,case when countrycode in ('at','be','ch','de','dk','es','fb','fi','fr','ie','nl','no','se') then 'EUR'
             when countrycode = 'uk' then 'GBP'
             when countrycode = 'ca' then 'CAD'
             when countrycode in ('au','nz') then 'AUD'
          end as currency       
       from intl.hive_member m
       join reporting.DW_D_DATE dd on date(m.dateProfileComplete) = dd.date 
       where dd.year = 2022) fx_v1
      group by 1,2,3,4,5
         
      union

      select 
        dd.year, dd.month, dd.date,
        source_currency as currency,
        currency_rate as fx_rate
      from 
        (select 
          coalesce(fx.current_rate_date, fx_r.current_rate_date) as current_rate_date,
          coalesce(fx.source_currency, fx_r.source_currency) as source_currency,
          coalesce(fx.currency_rate, fx_r.currency_rate) as currency_rate
        from
          (
            select distinct 
              current_rate_date,
              source_currency,
              currency_rate
            from reporting.DW_CARE_FX_RATES_HISTORY
            where current_rate_date >= '2023-01-01'
            and source_currency in ('EUR','GBP','CAD','AUD') and target_currency = 'USD' 
            group by 1,2,3 order by 1 asc
          ) fx
          full outer join 
              (
              select distinct 
                (date(current_rate_date)+1) as current_rate_date, 
                source_currency, 
                currency_rate 
              from reporting.DW_CARE_FX_RATES_HISTORY 
              where current_rate_date in ('2023-03-23', '2023-05-19') and source_currency in ('EUR','GBP','CAD','AUD') and target_currency = 'USD'
              group by 1,2,3
              ) fx_r on fx.source_currency = fx_r.source_currency and fx.current_rate_date = fx_r.current_rate_date          
        group by 1,2,3) fx_daily
        join reporting.DW_D_DATE dd on current_rate_date = dd.date and dd.date < date(current_date)
      group by 1,2,3,4,5
),

visits as (
   select 
      dd.year, dd.month, dd.date, 
      upper(v.countrycode) as country,
      case when lower(rxsite) in ('qualityclick', 'blog', 'generic', 'ortsdienst', '4everglen', 'snautz', 'marktde', 'stadtlist', 'aktuellejobs', 'alleskralle', 'jobrapido', 'metajob', 'savethestudent', 'studentjob', 'putzchecker', 'awin', 'zanox', 'pinterest', 'impact', 'mibaby') then 'Brand'
           when lower(rxservice) in ('br', 'all', '', 'general', 'op') then 'Brand'
           when lower(rxservice) in ('cc', 'c', 'childcare') then 'Childcare'
           when lower(rxservice) = 'hk' then 'Housekeeping'
           when lower(rxservice) = 'pc' then 'Petcare'
           when lower(rxservice) in ('sc', 'seniorcare') then 'Seniorcare'
           when lower(rxservice) = 'ap' then 'Aupair'
           when lower(rxservice) = 'sn' then 'Specialneeds'
           when lower(rxservice) = 'tt' then 'Tutoring'
        else 'Brand' end as vertical,
      case when lower(rxcampaign) = 'sem' then 'SEM' 
           when lower(rxsite) = 'facebook' then 'Facebook'
           when lower(rxsite) = 'gdn' then 'GDN'
           when lower(rxsite) = 'youtube' then 'YouTube'
           when lower(rxsite) = 'meinestadt.de' then 'Meinestadt'
           when lower(rxsite) in ('qualityclick', 'blog', 'generic', 'ortsdienst', '4everglen', 'snautz', 'marktde', 'stadtlist', 'aktuellejobs', 'alleskralle', 'jobrapido', 'metajob', 'savethestudent', 'studentjob') then 'Quality Click'
           when lower(rxsite) in ('awin', 'zanox') then 'Awin'
           when lower(rxsite) = 'putzchecker' then 'Putzchecker'
           when lower(rxsite) = 'pinterest' then 'Pinterest'
           when lower(rxsite) = 'impact' then 'Impact'
           when lower(rxsite) = 'mibaby' then 'MiBaby'
        end as channel,  
      count(distinct visitorid) as visits        
    from intl.hive_visit v
    join reporting.DW_D_DATE dd on date(v.StartDate) = dd.date
    where ( lower(rxcampaign) = 'sem' or 
            lower(rxsite) in ('facebook', 'gdn', 'youtube', 'meinestadt.de', 'qualityclick', 'blog', 'generic', 'ortsdienst', '4everglen', 'snautz', 'marktde', 'stadtlist', 'aktuellejobs', 'alleskralle', 'jobrapido', 'metajob', 'savethestudent', 'studentjob', 'putzchecker', 'awin', 'zanox', 'pinterest', 'impact', 'mibaby') )
      and (v.memberid is null or v.signup = true)
      and dd.year >= year(now())-1
      and dd.date < date(current_date)
    group by 1,2,3,4,5,6   
),

basics as (
 select
      dd.year, dd.month, dd.date, 
      upper(mm.countrycode) as country,
      case when lower(mm.site) in ('qualityclick', 'blog', 'generic', 'ortsdienst', '4everglen', 'snautz', 'marktde', 'stadtlist', 'aktuellejobs', 'alleskralle', 'jobrapido', 'metajob', 'savethestudent', 'studentjob', 'putzchecker', 'awin', 'zanox', 'pinterest', 'impact', 'mibaby') then 'Brand'
           when lower(mm.service) in ('br', 'all') then 'Brand'
           when (lower(mm.service) not in ('br', 'all') and mm.vertical = 'homeCare') then 'Housekeeping' 
           when (lower(mm.service) not in ('br', 'all') and (mm.vertical is null or mm.vertical = '')) then 'Childcare' 
        else initcap(lower(mm.vertical)) end as vertical, 
      case when lower(mm.campaign) = 'sem' then 'SEM' 
           when lower(mm.site) = 'facebook' then 'Facebook'
           when lower(mm.site) = 'gdn' then 'GDN'
           when lower(mm.site) = 'youtube' then 'YouTube'
           when lower(mm.site) = 'meinestadt.de' then 'Meinestadt'
           when lower(mm.site) in ('qualityclick', 'blog', 'generic', 'ortsdienst', '4everglen', 'snautz', 'marktde', 'stadtlist', 'aktuellejobs', 'alleskralle', 'jobrapido', 'metajob', 'savethestudent', 'studentjob') then 'Quality Click'
           when lower(mm.site) in ('awin', 'zanox') then 'Awin'
           when lower(mm.site) = 'putzchecker' then 'Putzchecker'
           when lower(mm.site) = 'pinterest' then 'Pinterest'
           when lower(mm.site) = 'impact' then 'Impact'
           when lower(mm.site) = 'mibaby' then 'MiBaby'
        end as channel,  
      count(distinct mm.memberid) as basics  
    from intl.hive_member mm   
    join reporting.DW_D_DATE dd on date(mm.dateMemberSignup) = dd.date 
    where (lower(campaign) = 'sem' or lower(site) in ('facebook', 'youtube', 'gdn', 'meinestadt.de', 'qualityclick', 'blog', 'generic', 'ortsdienst', '4everglen', 'snautz', 'marktde', 'stadtlist', 'aktuellejobs', 'alleskralle', 'jobrapido', 'metajob', 'savethestudent', 'studentjob', 'putzchecker', 'awin', 'zanox', 'pinterest', 'impact', 'mibaby'))
      and lower(role) = 'seeker'
      and IsInternalAccount = 'false'
      and dd.year >= year(now())-1 
      and dd.date < date(current_date)
    group by 1,2,3,4,5,6 
),

premiums as (
 select
      dd.year, dd.month, dd.date, 
      upper(mm.countrycode) as country,
      case when lower(mm.site) in ('qualityclick', 'blog', 'generic', 'ortsdienst', '4everglen', 'snautz', 'marktde', 'stadtlist', 'aktuellejobs', 'alleskralle', 'jobrapido', 'metajob', 'savethestudent', 'studentjob', 'putzchecker', 'awin', 'zanox', 'pinterest', 'impact', 'mibaby') then 'Brand'
           when lower(mm.service) in ('br', 'all') then 'Brand'
           when (lower(mm.service) not in ('br', 'all') and mm.vertical = 'homeCare') then 'Housekeeping' 
           when (lower(mm.service) not in ('br', 'all') and (mm.vertical is null or mm.vertical = '')) then 'Childcare' 
        else initcap(lower(mm.vertical)) end as vertical, 
      case when lower(mm.campaign) = 'sem' then 'SEM' 
           when lower(mm.site) = 'facebook' then 'Facebook'
           when lower(mm.site) = 'gdn' then 'GDN'
           when lower(mm.site) = 'youtube' then 'YouTube'
           when lower(mm.site) = 'meinestadt.de' then 'Meinestadt'
           when lower(mm.site) in ('qualityclick', 'blog', 'generic', 'ortsdienst', '4everglen', 'snautz', 'marktde', 'stadtlist', 'aktuellejobs', 'alleskralle', 'jobrapido', 'metajob', 'savethestudent', 'studentjob') then 'Quality Click'
           when lower(mm.site) in ('awin', 'zanox') then 'Awin'
           when lower(mm.site) = 'putzchecker' then 'Putzchecker'
           when lower(mm.site) = 'pinterest' then 'Pinterest'
           when lower(mm.site) = 'impact' then 'Impact'
           when lower(mm.site) = 'mibaby' then 'MiBaby'
        end as channel,        
     count(distinct sp.subscriptionId) as premiums,
     count(distinct case when date(sp.subscriptionDateCreated) = date(mm.dateProfileComplete) then sp.subscriptionId end) as day1s,
     count(distinct case when date(mm.dateFirstPremiumSignup) = date(sp.subscriptionDateCreated) and date(sp.subscriptionDateCreated) != date(mm.dateMemberSignup) then sp.subscriptionId end) as nths,
     count(distinct case when date(mm.dateFirstPremiumSignup) != date(sp.subscriptionDateCreated) then sp.subscriptionId end) as reupgrades     
    from intl.transaction tt
      join intl.hive_subscription_plan sp on sp.subscriptionId = tt.subscription_plan_id and sp.countrycode = tt.country_code
      join reporting.DW_D_DATE dd         on date(sp.subscriptionDateCreated) = dd.date 
      join intl.hive_member mm            on tt.member_id = mm.memberid and tt.country_code = mm.countrycode
   where tt.type in ('PriorAuthCapture','AuthAndCapture')
      and tt.status = 'SUCCESS'
      and tt.amount > 0
      and mm.IsInternalAccount = 'false'
      and lower(mm.role) = 'seeker'
      and (lower(campaign) = 'sem' or lower(site) in ('facebook', 'youtube', 'gdn', 'meinestadt.de', 'qualityclick', 'blog', 'generic', 'ortsdienst', '4everglen', 'snautz', 'marktde', 'stadtlist', 'aktuellejobs', 'alleskralle', 'jobrapido', 'metajob', 'savethestudent', 'studentjob', 'putzchecker', 'awin', 'zanox', 'pinterest', 'impact', 'mibaby'))
      and dd.year >= year(now())-1
      and dd.date < date(current_date)
  group by 1,2,3,4,5,6
)

select
  coalesce(s.year, v.year, b.year, p.year) as Year,
  coalesce(s.month, v.month, b.month, p.month) as Month,
  coalesce(s.date, v.date, b.date, p.date) as Date,
  coalesce(s.country, v.country, b.country, p.country) as Country,
  coalesce(s.channel, v.channel, b.channel, p.channel) as Channel,
  coalesce(s.vertical, v.vertical, b.vertical, p.vertical) as Vertical,
  s.currency,
  f.fx_rate,
  
  ifnull(sum(s.spend*f.fx_rate),0) as Spend_USD,
  ifnull(sum(v.visits),0) as Visits,
  ifnull(sum(b.basics),0) as Basics,
  ifnull(sum(p.premiums),0) as Premiums,
  ifnull(sum(p.day1s),0) as Day1s,
  ifnull(sum(p.nths),0) as Nths,
  ifnull(sum(p.reupgrades),0) as Reupgrades
  
from spend s
       join live_fx_rate f    on s.year = f.year and s.month = f.month and s.date = f.date and s.currency = f.currency   
  full outer join visits v    on s.year = v.year and s.month = v.month and s.date = v.date and s.country = v.country and s.vertical = v.vertical and s.channel = v.channel
  full outer join basics b    on s.year = b.year and s.month = b.month and s.date = b.date and s.country = b.country and s.vertical = b.vertical and s.channel = b.channel
  full outer join premiums p  on s.year = p.year and s.month = p.month and s.date = p.date and s.country = p.country and s.vertical = p.vertical and s.channel = p.channel
  
group by 1,2,3,4,5,6,7,8 order by 1,2,3 asc
