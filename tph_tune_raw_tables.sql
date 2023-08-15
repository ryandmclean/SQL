with conversion_raw as (
  -- Since the conversions and clicks are stored in rows instead of columns
  -- I need similar logic to get each separately. 
  -- Join in the revenue-driving events and clean it up with some metadata
    select
      date(cl.conversion_time) as event_date,
      cl.advertiser_id,
      ad.company as advertiser_name,
      cl.offer_id,
      of.name as offer_name,
      cl.affiliate_id,
      af.company as affiliate_name,
      affiliate_sub3,
      affiliate_sub4,
      affiliate_source,
      cl.transaction_id,
      case when revenue_cents is null then 0 else revenue_cents end as cr_rev,
      payout_cents
    from [HIDDEN_TABLE_NAME] cl
    left join [HIDDEN_TABLE_NAME] ad on cl.advertiser_id=ad.id
    left join [HIDDEN_TABLE_NAME] of on cl.offer_id=of.id
    left join [HIDDEN_TABLE_NAME] af on cl.affiliate_id=af.id
    where cl.status='approved'
    ),
    conversion_clean as (
  -- Group by the dimensions to get the number of conversions and revenue
    select
      event_date,
      advertiser_name,offer_name,affiliate_name,
      affiliate_id, affiliate_source,
      affiliate_sub3,affiliate_sub4,
      count(transaction_id) as conversions,
      0 as clicks,
      sum(cr_rev) as revenue_cents,
      sum(payout_cents) as payout_cents
    from conversion_raw
    group by 1,2,3,4,5,6,7,8
    ),
    click_raw as (
  -- Do the exact same thing, but for clicks instead of conversions
    select
      date(cl.click_time) as event_date,
      cl.advertiser_id,
      ad.company as advertiser_name,
      cl.offer_id,
      of.name as offer_name,
      cl.affiliate_id,
      af.company as affiliate_name,
      affiliate_source,
      affiliate_sub3,
      affiliate_sub4,
      cl.transaction_id,
      case when revenue_cents is null then 0 else revenue_cents  end as cl_rev,
      payout_cents
    from [HIDDEN_TABLE_NAME] cl
    left join [HIDDEN_TABLE_NAME] ad on cl.advertiser_id=ad.id
    left join [HIDDEN_TABLE_NAME] of on cl.offer_id=of.id
    left join [HIDDEN_TABLE_NAME] af on cl.affiliate_id=af.id
    ),
    click_clean as (
  -- Group by dimensions to get an accurate count of clicks and sum of revenue
    select
      event_date,
      advertiser_name,offer_name,affiliate_name,
      affiliate_id, affiliate_source,
      affiliate_sub3,affiliate_sub4,
      0 as conversions,
      count(distinct transaction_id) as clicks,
      sum(cl_rev) as revenue_cents,
      sum(payout_cents) as payout_cents
    from click_raw
    group by 1,2,3,4,5,6,7,8
    ),
    combine_clicks_conversions as (
  -- Union them all together in rows
      SELECT * FROM conversion_clean
      UNION ALL
      SELECT * FROM click_clean
    ),
    campaign_lookup_table as (
  -- Prepare a lookup table to join ID to name
    SELECT DISTINCT
      campaign_id,
      campaign_name,
      ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY created_date DESC) as rank
    FROM [HIDDEN_TABLE_NAME]
    ),
    revenue_final as (
  -- Perform some cleaning on the combined clicks and conversions
  -- Using regex, pull out the campaign_id from aff_sub4
    SELECT
      event_date,
      affiliate_name,
      affiliate_id,
      affiliate_source,
      case when affiliate_sub4 is not null THEN
        case when affiliate_sub4 like '%cmp%'
        then ltrim(regexp_substr(affiliate_sub4 ,'cmp-([0-9]{1,12})') ,'cmp-')
        else replace(regexp_substr(affiliate_sub4,'^([0-9])+_*'),'_','')
        end
      ELSE
        case when affiliate_sub3 like '%cmp%'
        then ltrim(regexp_substr(affiliate_sub3 ,'cmp-([0-9]{1,12})') ,'cmp-')
        else replace(regexp_substr(affiliate_sub3,'^([0-9])+_*'),'_','')
        end
      END as campaign_id,
      SUM(clicks) as tune_clicks,
      SUM(conversions) as tune_conversions,
      SUM(cast(revenue_cents as float)/cast(100 as float)) as tune_revenue
    FROM combine_clicks_conversions
    group by 1,2,3,4,5
    )
  -- Join in the lookup table and finalize the governed table for BI visualization and reporting
    SELECT
    a.event_date,
    a.affiliate_name,
    a.affiliate_id,
    a.affiliate_source,
    b.campaign_name,
    a.campaign_id,
    tune_clicks,
    tune_conversions,
    tune_revenue
FROM revenue_final a
LEFT JOIN campaign_lookup_table b on a.campaign_id = b.campaign_id and rank = 1
where a.event_date >= '2023-01-01'
