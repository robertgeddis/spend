select 
  ab.countrycode, ab.testname, tc.cellname, ab.startdate, ab.enddate, ab.description, testnameDisplay, ab.status, ab.winner
  ,case when tc.cellname = ab.controlcellname then ab.controlCellnameDisplay else ab.testCellnameDisplay end as cellnamedisplay
  ,date(tc.datecreated) as date
  ,case
    when lower(v.rxcampaign)='seo' then 'SEO'
    when (lower(v.rxcampaign)='seo' and rxcampaignname='JobPage' and v.rxsite like 'www.google.%' and v.rxcreativeversion like '%google_jobs_apply%') then 'Google Jobs'  
    when (lower(v.rxsite) like 'careus%' or lower(v.rxcampaign)='direct') then 'Direct'
    when (lower(v.rxcampaign)='online' and lower(v.rxsite)='facebook') then 'Facebook'
    when (lower(v.rxcampaign)='sem' and (lower(v.rxservice) in ('br','all','general') or lower(v.rxcampaignname) like '%_br_%' or lower(v.rxcampaignname) like '%brand%')) then 'SEM-Br'
    when (lower(v.rxcampaign)='sem' and (lower(v.rxservice) not in ('br','all','general') or lower(v.rxcampaignname) not like '%_br_%' or lower(v.rxcampaignname) not like '%brand%')) then 'SEM non-Br'
    when lower(v.rxcampaign) in ('online','display','affiliate') then 'Online/Aff/Disp'
    when (lower(v.rxcampaign) in ('email','onsite','','wps','b2b')) then 'Other'
    when v.rxcampaign is null then 'Other'
    else 'Other'
  end as referrer_category
  ,case 
    when tc.device = 'smartphone' then 'Mobile'
    when tc.device = '' or tc.device is null then 'Mobile' 
    else initcap(lower(tc.device))
  end as device
  ,su.role
  ,case 
    when m.vertical = 'homeCare' then 'Housekeeping' 
    when m.role is not null and (m.vertical IS NULL or m.vertical = '') then 'Childcare' 
    else initcap(lower(m.vertical))
  end as signup_vertical
  ,case 
    when m.role is null then null
    when lower(m.campaign) = 'seo' then 'SEO'
    when (lower(m.site) like 'careus%' or lower(m.campaign) = 'direct') then 'Direct'
    when (lower(m.audience) <> 'provider' and lower(m.campaign) = 'sem') then 'SEM'
    when (lower(m.campaign) = 'online' and lower(m.site) = 'facebook') then 'Facebook'
    when lower(m.campaign) in ('online','display','affiliate') then 'Online/Aff/Disp'
    when (lower(m.campaign) in ('email','onsite','','wps','b2b')) then 'Other'
    when m.campaign IS NULL then 'Other'
    when lower(m.audience) = 'provider' then 'Other'
    else 'Other'
  end as signup_channel
  ,count(distinct tc.visitorid) as assigned
  ,count(distinct case when v.pageviews = 1 and v.durationonsite = 0 then tc.visitorid end) as bounced
  ,sum(v.pageviews) as total_pageviews
  ,count(distinct enr.visitorid) as reached_enrollment
  ,count(distinct m.memberid) as signups
  ,count(distinct case when up.itemtype = 'day1Premium' then up.memberid end) as upgrades_day1
  ,count(distinct case when up.itemtype = 'dayNPremium' then up.memberid end) as upgrades_dayn
  ,count(distinct case when up.itemtype in ('dayNPremium','day1Premium') then up.memberid end) as upgrades_initial
  ,count(distinct case when up.itemtype = 'Reupgrade' then up.memberid end) as upgrades_reupgrade
  ,count(distinct up.memberid) as upgrades_all
  ,count(distinct case when up.itemtype = 'day1Premium' and up.promocode is not null then up.memberid end) as upgrades_promo_day1
  ,count(distinct case when up.itemtype = 'dayNPremium' and up.promocode is not null then up.memberid end) as upgrades_promo_dayn
  ,count(distinct case when up.itemtype in ('dayNPremium','day1Premium') and up.promocode is not null then up.memberid end) as upgrades_promo_initial
  ,count(distinct case when up.itemtype = 'Reupgrade' and up.promocode is not null then up.memberid end) as upgrades_promo_reupgrade
  ,count(distinct case when up.promocode is not null then up.memberid end) as upgrades_promo_all
  ,count(distinct cl.memberid) as closed_same_day
from analytics.vw_INTL_AB_Matrix ab 
  join intl.hive_event tc on tc.countrycode = ab.countrycode and tc.testname = ab.testname and tc.cellname in (ab.controlcellname, ab.testcellname)
    and tc.datecreated between ab.startdate and ab.enddate 
    and tc.name = 'TestCellAssignment' and date(tc.datecreated) <> current_date() and tc.year >= 2021
    and tc.memberid is null and tc.isrobot is not true and tc.iscsr is not true
  join intl.hive_visit v on v.visitorid = tc.visitorid and v.countrycode = tc.countrycode and date(v.startdate) = date(tc.datecreated) 
    and v.sessionid = tc.sessionid and v.rxcampaign = tc.campaign
    and (v.memberid is null or v.signup = true) and v.year >= 2021 and v.visitNumber = 1
  left join intl.hive_event enr on enr.countrycode = tc.countrycode and enr.visitorid = tc.visitorid and enr.datecreated > tc.datecreated
    and enr.datecreated between ab.startdate and ab.enddate
    and enr.name = 'PageView' and (enr.currentpageurl like '%join-now%' or enr.currentpageurl like '%register%') and enr.year >= 2021
  left join intl.hive_event su on tc.countrycode = su.countrycode and tc.visitorid = su.visitorid and su.datecreated > tc.datecreated
    and su.datecreated between ab.startdate and ab.enddate
    and su.name = 'Signup' and su.year >= 2021
  left join intl.hive_member m on m.countrycode = su.countrycode and m.memberid = su.memberid and m.isinternalaccount is not true and m.closedforfraud is not true
  left join intl.hive_event up on up.countrycode = su.countrycode and up.memberid = su.memberid and up.datecreated >= su.datecreated 
    and up.datecreated between ab.startdate and ab.enddate
    and up.name = 'Upgrade' and up.year >= 2021
  left join intl.hive_event cl on cl.countrycode = su.countrycode and cl.memberid = su.memberid and date(cl.datecreated) = date(su.datecreated)
    and cl.datecreated between ab.startdate and ab.enddate
    and cl.name = 'AccountActionRequest' and cl.accountAction = 'Close' and cl.year >= 2021
where ab.testname in ('mobileVhpPages','vhpPages') and ab.status = 'Active'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
