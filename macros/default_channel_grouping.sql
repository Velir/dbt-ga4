-- Google's documentation is here: https://support.google.com/analytics/answer/9756891?hl=en
-- source_category Excel file can be downloaded from the above link and may change over time
{% macro default_channel_grouping(source, medium, source_category, campaign) %}
  {{ return(adapter.dispatch('default_channel_grouping', 'ga4')(source, medium, source_category, campaign)) }}
{% endmacro %}

{% macro default__default_channel_grouping(source, medium, source_category, campaign) %}
case
  -- Direct: Source exactly matches "(direct)" AND Medium is one of ("(not set)", "(none)")
  when (
      {{source}} is null
        and {{medium}} is null
    )
    or (
      {{source}} = '(direct)'
      and ({{medium}} = '(none)' or {{medium}} = '(not set)')
    )
    then 'Direct'

  -- Cross-network: Campaign Name contains "cross-network"
  when REGEXP_CONTAINS({{campaign}}, r"cross-network")
    then 'Cross-network'

  -- Paid Shopping:
  --   (Source matches a list of shopping sites
  --   OR
  --   Campaign Name matches regex ^(.*(([^a-df-z]|^)shop|shopping).*)$)
  --   AND
  --   Medium matches regex ^(.*cp.*|ppc|retargeting|paid.*)$
  when (
      {{source_category}} = 'SOURCE_CATEGORY_SHOPPING'
      or REGEXP_CONTAINS({{campaign}}, r"^(.*(([^a-df-z]|^)shop|shopping).*)$")
    )
    and REGEXP_CONTAINS({{medium}},r"^(.*cp.*|ppc|retargeting|paid.*)$")
    then 'Paid Shopping'

  -- Paid Search:
  --   Source matches a list of search sites
  --   AND
  --   Medium matches regex ^(.*cp.*|ppc|retargeting|paid.*)$
  when {{source_category}} = 'SOURCE_CATEGORY_SEARCH'
    and REGEXP_CONTAINS({{medium}}, r"^(.*cp.*|ppc|retargeting|paid.*)$")
    then 'Paid Search'

  -- Paid Social:
  --   Source matches a regex list of social sites
  --   AND
  --   Medium matches regex ^(.*cp.*|ppc|retargeting|paid.*)$
  when {{source_category}} = 'SOURCE_CATEGORY_SOCIAL'
    and REGEXP_CONTAINS({{medium}}, r"^(.*cp.*|ppc|retargeting|paid.*)$")
    then 'Paid Social'

  -- Paid Video:
  --   Source matches a list of video sites
  --   AND
  --   Medium matches regex ^(.*cp.*|ppc|retargeting|paid.*)$
  when {{source_category}} = 'SOURCE_CATEGORY_VIDEO'
    and REGEXP_CONTAINS({{medium}},r"^(.*cp.*|ppc|retargeting|paid.*)$")
    then 'Paid Video'

  -- Display:
  --   Medium is one of ("display", "banner", "expandable", "interstitial", "cpm")
  when {{medium}} in ('display', 'banner', 'expandable', 'interstitial', 'cpm')
    then 'Display'

  -- Paid Other:
  --   Medium matches regex ^(.*cp.*|ppc|retargeting|paid.*)$
  when REGEXP_CONTAINS({{medium}}, r"^(.*cp.*|ppc|retargeting|paid.*)$")
    then 'Paid Other'

  -- Organic Shopping:
  --   Source matches a list of shopping sites
  --   OR
  --   Campaign name matches regex ^(.*(([^a-df-z]|^)shop|shopping).*)$
  when {{source_category}} = 'SOURCE_CATEGORY_SHOPPING'
    or REGEXP_CONTAINS({{campaign}}, r"^(.*(([^a-df-z]|^)shop|shopping).*)$")
    then 'Organic Shopping'

  -- Organic Social:
  --   Source matches a regex list of social sites
  --   OR
  --   Medium is one of ("social", "social-network", "social-media", "sm", "social network", "social media")
  when {{source_category}} = 'SOURCE_CATEGORY_SOCIAL'
    or {{medium}} in ("social","social-network","social-media","sm","social network","social media")
    then 'Organic Social'

  -- Organic Video:
  --   Source matches a list of video sites
  --   OR
  --   Medium matches regex ^(.*video.*)$
  when {{source_category}} = 'SOURCE_CATEGORY_VIDEO'
    or REGEXP_CONTAINS({{medium}}, r"^(.*video.*)$")
    then 'Organic Video'

  -- Organic Search:
  --   Source matches a list of search sites
  --   OR
  --   Medium exactly matches organic
  when {{source_category}} = 'SOURCE_CATEGORY_SEARCH' or {{medium}} = 'organic'
    then 'Organic Search'

  -- Referral:
  --   Medium is one of ("referral", "app", or "link")
  when {{medium}} in ("referral", "app", "link")
    then 'Referral'

  -- Email:
  --   Source = email|e-mail|e_mail|e mail
  --   OR
  --   Medium = email|e-mail|e_mail|e mail
  when REGEXP_CONTAINS({{source}}, r"email|e-mail|e_mail|e mail")
    or REGEXP_CONTAINS({{medium}}, r"email|e-mail|e_mail|e mail")
    then 'Email'

  -- Affiliates:
  --   Medium = affiliate
  when {{medium}} = 'affiliate'
    then 'Affiliates'

  -- Audio:
  --   Medium exactly matches audio
  when {{medium}} = 'audio'
    then 'Audio'

  -- SMS:
  --   Source exactly matches sms
  --   OR
  --   Medium exactly matches sms
  when {{source}} = 'sms'
    or {{medium}} = 'sms'
    then 'SMS'

  -- Mobile Push Notifications:
  --   Medium ends with "push"
  --   OR
  --   Medium contains "mobile" or "notification"
  --   OR
  --   Source exactly matches "firebase"
  when REGEXP_CONTAINS({{medium}}, r"push$")
    or REGEXP_CONTAINS({{medium}}, r"mobile|notification")
    or {{source}} = 'firebase'
    then 'Mobile Push Notifications'

  -- Unassigned is the value Analytics uses when there are no other channel rules that match the event data.
  else 'Unassigned'
end

{% endmacro %}