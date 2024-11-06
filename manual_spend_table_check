select distinct date, member_type, channel, country_code, source_currency, ifnull(sum(spend),0) as spend 
from (

    -- TV
  select distinct d.date, 'Seeker' as member_type, 'TV' as channel,  'DE' as country_code, 'EUR' as source_currency, ifnull(sum(DE_TV),0) as spend 
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE d on spend_date = d.date where d.year >= year(now())-2 and d.date < date(current_date) group by 1,2,3,4,5

  union 

  -- TikTok (DE Seekers - Started using de_seeker_mibaby column on 25.03.2024 for TikTok spend)
  select distinct d.date, 'Seeker' as member_type, 'TikTok' as channel,  'DE' as country_code, 'EUR' as source_currency, ifnull(sum(DE_Seeker_Mibaby),0) as spend 
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE d on spend_date = d.date where d.year >= year(now())-2 and d.date < date(current_date) group by 1,2,3,4,5

  union   

  -- PINTEREST (DE, UK, CA + AU Seekers)
  select distinct d.date, 'Seeker' as member_type, 'Pinterest' as channel, 'DE' as country, 'EUR' as source_currency, ifnull(sum(DE_Seeker_Pinterest),0) as spend
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE d on spend_date = d.date where d.year >= year(now())-2 and d.date < date(current_date) group by 1,2,3,4,5
  union
  select distinct d.date, 'Seeker' as member_type, 'Pinterest' as channel, 'UK' as country, 'EUR' as source_currency, ifnull(sum(UK_Seeker_Pinterest),0) as spend
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE d on spend_date = d.date where d.year >= year(now())-2 and d.date < date(current_date) group by 1,2,3,4,5
  union
  select distinct d.date, 'Seeker' as member_type, 'Pinterest' as channel, 'CA' as country, 'EUR' as source_currency, ifnull(sum(CA_Seeker_Pinterest),0) as spend
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE d on spend_date = d.date where d.year >= year(now())-2 and d.date < date(current_date) group by 1,2,3,4,5
  union
  select distinct d.date, 'Seeker' as member_type, 'Pinterest' as channel, 'AU' as country, 'EUR' as source_currency, ifnull(sum(AU_Seeker_Pinterest),0) as spend 
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE d on spend_date = d.date where d.year >= year(now())-2 and d.date < date(current_date) group by 1,2,3,4,5


  union

  -- IMPACT (CA + AU SEEKERS ONLY)
  select distinct d.date, 'Seeker' as member_type, 'Impact' as channel, 'CA' as country, 'EUR' as source_currency, ifnull(sum(CA_OTHER_ONLINE),0) as spend
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE d on spend_date = d.date where d.year >= year(now())-2 and d.date < date(current_date) group by 1,2,3,4,5
  union
  select distinct d.date, 'Seeker' as member_type, 'Impact' as channel, 'AU' as country, 'EUR' as source_currency, ifnull(sum(AU_OTHER_ONLINE),0) as spend
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE d on spend_date = d.date where d.year >= year(now())-2 and d.date < date(current_date) group by 1,2,3,4,5
  
  union

  -- KLEINANZEIGEN 
  select distinct d.date, 'Seeker' as member_type, 'Kleinanzeigen' as channel, 'DE' as country, 'EUR' as source_currency, ifnull(sum(DE_Seeker_Nebenan),0) as spend
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE d on date(spend_date) = d.date where d.year >= year(now())-2 and d.date < date(current_date) group by 1,2,3,4,5
  union
  select distinct d.date, 'Provider' as member_type, 'Kleinanzeigen' as channel, 'DE' as country, 'EUR' as source_currency, ifnull(sum(DE_Provider_Nebenan),0) as spend
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE d on date(spend_date) = d.date where d.year >= year(now())-2 and d.date < date(current_date) group by 1,2,3,4,5
  
  union

  -- MICROSOFT SPEND FROM MANUAL MARKETING SPEND TABLE (SEEKER ONLY)  
  select distinct d.date, 'Seeker' as member_type, 'Microsoft Ads' as channel, 'AT' as country, 'EUR' as source_currency, ifnull(sum(AT_Seeker_Microsoft),0) as spend
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE d on spend_date = d.date where d.year >= year(now())-2 and d.date < date(current_date) group by 1,2,3,4,5 
  union
  select distinct d.date, 'Seeker' as member_type, 'Microsoft Ads' as channel, 'AU' as country, 'EUR' as source_currency, ifnull(sum(AU_Seeker_Microsoft),0) as spend
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE d on spend_date = d.date where d.year >= year(now())-2 and d.date < date(current_date) group by 1,2,3,4,5 
  union
  select distinct d.date, 'Seeker' as member_type, 'Microsoft Ads' as channel, 'BE' as country, 'EUR' as source_currency, ifnull(sum(BE_Seeker_Microsoft),0) as spend
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE d on spend_date = d.date where d.year >= year(now())-2 and d.date < date(current_date) group by 1,2,3,4,5
  union
  select distinct d.date, 'Seeker' as member_type, 'Microsoft Ads' as channel, 'CA' as country, 'EUR' as source_currency, ifnull(sum(CA_Seeker_Microsoft),0) as spend
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE d on spend_date = d.date where d.year >= year(now())-2 and d.date < date(current_date) group by 1,2,3,4,5 
  union
  select distinct d.date, 'Seeker' as member_type, 'Microsoft Ads' as channel, 'CH' as country, 'EUR' as source_currency, ifnull(sum(CH_Seeker_Microsoft),0) as spend 
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE d on spend_date = d.date where d.year >= year(now())-2 and d.date < date(current_date) group by 1,2,3,4,5
  union
  select distinct d.date, 'Seeker' as member_type, 'Microsoft Ads' as channel, 'DE' as country, 'EUR' as source_currency, ifnull(sum(DE_Seeker_Microsoft),0) as spend
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE d on spend_date = d.date where d.year >= year(now())-2 and d.date < date(current_date) group by 1,2,3,4,5
  union
  select distinct d.date, 'Seeker' as member_type, 'Microsoft Ads' as channel, 'DK' as country, 'EUR' as source_currency, ifnull(sum(DK_Seeker_Microsoft),0) as spend
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE d on spend_date = d.date where d.year >= year(now())-2 and d.date < date(current_date) group by 1,2,3,4,5
  union
  select distinct d.date, 'Seeker' as member_type, 'Microsoft Ads' as channel, 'FI' as country, 'EUR' as source_currency, ifnull(sum(FI_Seeker_Microsoft),0) as spend
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE d on spend_date = d.date where d.year >= year(now())-2 and d.date < date(current_date) group by 1,2,3,4,5
  union
  select distinct d.date, 'Seeker' as member_type, 'Microsoft Ads' as channel, 'IE' as country, 'EUR' as source_currency, ifnull(sum(IE_Seeker_Microsoft),0) as spend 
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE d on spend_date = d.date where d.year >= year(now())-2 and d.date < date(current_date) group by 1,2,3,4,5
  union
  select distinct d.date, 'Seeker' as member_type, 'Microsoft Ads' as channel, 'NL' as country, 'EUR' as source_currency, ifnull(sum(NL_Seeker_Microsoft),0) as spend
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE d on spend_date = d.date where d.year >= year(now())-2 and d.date < date(current_date) group by 1,2,3,4,5
  union
  select distinct d.date, 'Seeker' as member_type, 'Microsoft Ads' as channel, 'NO' as country, 'EUR' as source_currency, ifnull(sum(NO_Seeker_Microsoft),0) as spend
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE d on spend_date = d.date where d.year >= year(now())-2 and d.date < date(current_date) group by 1,2,3,4,5 
  union
  select distinct d.date, 'Seeker' as member_type, 'Microsoft Ads' as channel, 'NZ' as country, 'EUR' as source_currency, ifnull(sum(NZ_Seeker_Microsoft),0) as spend
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE d on spend_date = d.date where d.year >= year(now())-2 and d.date < date(current_date) group by 1,2,3,4,5
  union
  select distinct d.date, 'Seeker' as member_type, 'Microsoft Ads' as channel, 'SE' as country, 'EUR' as source_currency, ifnull(sum(SE_Seeker_Microsoft),0) as spend
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE d on spend_date = d.date where d.year >= year(now())-2 and d.date < date(current_date) group by 1,2,3,4,5
  union
  select distinct d.date, 'Seeker' as member_type, 'Microsoft Ads' as channel, 'UK' as country, 'EUR' as source_currency, ifnull(sum(UK_Seeker_Microsoft),0) as spend
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE d on spend_date = d.date where d.year >= year(now())-2 and d.date < date(current_date) group by 1,2,3,4,5

  union

  -- Offline Flyers (Seekers + DE Only)
  select distinct d.date, 'Seeker' as member_type, 'Flyers' as channel, 'DE' as country, 'EUR' as source_currency, ifnull(sum(DE_OTHER_ONLINE),0) as spend
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE d on spend_date = d.date where d.date >= '2024-09-08' and d.date < date(current_date) group by 1,2,3,4,5

  union
  
  -- APPCAST (DE + UK Providers) 
  select distinct d.date, 'Provider' as member_type, 'AppCast' as channel, 'DE' as country, 'EUR' as source_currency, ifnull(sum(de_provider_appcast),0) as spend 
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE d on spend_date = d.date where d.year >= year(now())-2 and d.date < date(current_date) group by 1,2,3,4,5
  union
  select distinct d.date, 'Provider' as member_type, 'AppCast' as channel, 'UK' as country, 'GBP' as source_currency, ifnull(sum(uk_provider_appcast),0) as spend 
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE d on spend_date = d.date where d.year >= year(now())-2 and d.date < date(current_date) group by 1,2,3,4,5

  union 

  -- MY PERFECT JOB (DE Providers)
  select distinct d.date, 'Provider' as member_type, 'MyPerfectJob' as channel, 'DE' as country, 'EUR' as source_currency, ifnull(sum(DE_Provider_Myperfectjob),0) as spend
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE d on date(spend_date) = d.date where d.year >= year(now())-2 and d.date < date(current_date) group by 1,2,3,4,5
  
  union

  -- JOB LIFT (DE Providers)
  select distinct d.date, 'Provider' as member_type, 'JobLift' as channel, 'DE' as country, 'EUR' as source_currency, ifnull(sum(DE_PROVIDER_JOBLIFT),0) as spend
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE d on date(spend_date) = d.date where d.year >= year(now())-2 and d.date < date(current_date) group by 1,2,3,4,5
  
  union

  -- ALLESKRALLE (DACH Providers)
  select distinct d.date, 'Provider' as member_type, 'AllesKralle' as channel, 'DE' as country, 'EUR' as source_currency, ifnull(sum(DE_PROVIDER_ALLESKRALLE),0) as spend
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE d on date(spend_date) = d.date where d.year >= year(now())-2 and d.date < date(current_date) group by 1,2,3,4,5
  union
  select distinct d.date, 'Provider' as member_type, 'AllesKralle' as channel, 'AT' as country, 'EUR' as source_currency, ifnull(sum(AT_PROVIDER_ALLESKRALLE),0) as spend
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE d on date(spend_date) = d.date where d.year >= year(now())-2 and d.date < date(current_date) group by 1,2,3,4,5
  union
  select distinct d.date, 'Provider' as member_type, 'AllesKralle' as channel, 'CH' as country, 'EUR' as source_currency, ifnull(sum(CH_PROVIDER_ALLESKRALLE),0) as spend
  from intl.DW_MARKETING_SPEND_INTL join reporting.DW_D_DATE d on date(spend_date) = d.date where d.year >= year(now())-2 and d.date < date(current_date) group by 1,2,3,4,5
  ) complete group by 1,2,3,4,5
