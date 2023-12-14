select 
   Countrycode, Member_Type, Vertical, channel, Subscription_Length, Upgrade_Month, Month_No, Members_Survived, Bookings_Local, RetentionPCT,
   avg(RetentionPCT) over (partition by countrycode, Member_Type, Vertical, channel, Subscription_Length, Month_No order by Upgrade_Month range between interval '5' month preceding and current row) as 'avg_retention_6months'
   
from
(
  select
      Countrycode,Member_Type,Vertical,channel,Subscription_Length/*Upgrade_Year*/,Upgrade_Month,Month_No,Members_Survived,Bookings_Local,
      ifnull(sum(case when Month_No <> 0 then Members_Survived end) / sum(case when Month_No = 0 then Members_Survived end)
           over (partition by Countrycode,Member_Type,Vertical,Subscription_Length/*,Upgrade_Year*/,Upgrade_Month),1) as 'RetentionPCT'
    from (
        select
          countrycode as 'Countrycode',
          initcap(role) as 'Member_Type',
          Vertical,
          channel,
          Subscription_Length as 'Subscription_Length',
          -- cast(Upgrade_Year as varchar) as 'Upgrade_Year',
          Upgrade_Month as 'Upgrade_Month',
          Month_No,
          count(distinct memberid) as 'Members_Survived',
          ifnull(sum(amount/(1+(vat_percentage/100))),0) as 'Bookings_Local'
            from (
              select
                m.countrycode, m.memberid,m.role,case when m.vertical = 'homeCare' then 'Housekeeping' when (m.vertical IS NULL or m.vertical = '') then 'Childcare' else initcap(lower(m.vertical)) end as 'Vertical',
                m.channel,
                s.currency, s.priceplandurationinmonths as 'Subscription_Length',
                d2.year as 'Upgrade_Year',  
                concat(concat(concat('Q', quarter(d2.month_start_date)), ' '), d2.year) as 'Upgrade_Quarter',
                d2.month_start_date as 'Upgrade_Month', 
                d1.month_start_date as 'Transaction_Month',
                datediff('month',d2.month_start_date, d1.month_start_date) as 'Month_No',
                case when lower(p.country_code) = 'ca' and p.region1 = 'Alberta' then 5
                    when lower(p.country_code) = 'ca' and p.region1 = 'British Columbia' then 12
                    when lower(p.country_code) = 'ca' and p.region1 = 'Manitoba' then 12
                    when lower(p.country_code) = 'ca' and p.region1 = 'New Brunswick' then 15
                    when lower(p.country_code) = 'ca' and p.region1 = 'Newfoundland' then 15
                    when lower(p.country_code) = 'ca' and p.region1 = 'Northwest Territories' then 5
                    when lower(p.country_code) = 'ca' and p.region1 = 'Nova Scotia' then 15
                    when lower(p.country_code) = 'ca' and p.region1 = 'Nunavut' then 5
                    when lower(p.country_code) = 'ca' and p.region1 = 'Ontario' then 13
                    when lower(p.country_code) = 'ca' and p.region1 = 'Prince Edward Island' then 15
                    when lower(p.country_code) = 'ca' and p.region1 = 'Quebec' then 15
                    when lower(p.country_code) = 'ca' and p.region1 = 'Saskatchewan' then 11
                    when lower(p.country_code) = 'ca' and p.region1 = 'Yukon Territory' then 5
                    else vat.vat_percentage
                    end as 'vat_percentage',
                t.amount as 'amount'
              from intl.transaction t
                join intl.hive_subscription_plan s on s.subscriptionid = t.subscription_plan_id and t.country_code = s.countrycode
                  and year(s.subscriptionDateCreated) >= year(current_date())-3 -- We only need to consider the last 36 months for the retention curves
                  and month(s.subscriptionDateCreated) <= month(CURRENT_DATE())-2  /* CHECK IF THIS WORKS AT MONTH ROLLOVER */ /*'Added in to give most recent month cohort a full month to renew*/
                  --and s.priceplandurationinmonths = 1 -- NEEDS TO BE REMOVED
                join intl.hive_member m on s.memberID = m.memberId and m.countrycode = t.country_code 
                  and m.IsInternalAccount = 'false'
                  --and m.role = 'seeker' -- NEEDS TO BE REMOVED
                  and m.countrycode in ('de', 'uk') -- NEEDS TO BE REMOVED
                  and lower(m.vertical) in ('childcare', 'petcare') -- NEEDS TO BE REMOVED
                  and m.countrycode not in ('es','fr')
                join intl.vat_rate vat on vat.vat_region = t.country_code and t.when_processed between vat.from_date and vat.to_date
                join intl.postcode p on p.zip_key = m.postcode and lower(p.country_code) = m.countrycode
                join reporting.dw_d_date d1 on d1.date = date(t.date_created)
                join reporting.dw_d_date d2 on d2.date = date(s.subscriptionDateCreated)
              where 1=1
                and t.type in ('PriorAuthCapture', 'AuthAndCapture')
                and t.status = 'SUCCESS'
                and t.amount > 0
                and d1.date >= d2.date 
              ) base
          where 1=1 
             and Month_No < 37 -- ADD THIS ONE LATER ON, PERHAPS DIFFERENT NESTED QUERY
             and
            (
              Subscription_Length = 3 and Month_No in (0, 3, 6, 9, 12, 15, 18, 21, 24, 27, 30, 33, 36) or
              Subscription_Length = 1 and Month_No between 0 and 36 or
              Subscription_Length = 12 and Month_No in (0, 12, 24, 36)
              )
          group by 1,2,3,4,5,6,7
      ) base2
    group by 1,2,3,4,5,6,7,8,9
    order by 1,2,3,4,5,6,7
) abc
group by 1,2,3,4,5,6,7,8,9,10 order by 1,2,3,4,5,6,7
