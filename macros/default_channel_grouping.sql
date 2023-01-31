-- Inspired by https://github.com/llooker/ga_four_block_dev/blob/master/views/sessions.view.lkml
-- Google's documentation is here: https://support.google.com/analytics/answer/9756891?hl=en
-- source_category Excel file can be downloaded from the above link and may change over time

{% macro default_channel_grouping(source, medium, source_category) %}
  {{ return(adapter.dispatch('default_channel_grouping', 'ga4')(source, medium, source_category)) }}
{% endmacro %}

{% macro default__default_channel_grouping(source, medium, source_category) %}
case 
  when {{source}} is null and {{medium}} is null 
    then 'Direct'
  when {{source}} = '(direct)'
    and ({{medium}} = '(none)' or {{medium}} = '(not set)')
    then 'Direct'

  when REGEXP_CONTAINS({{source}}, r"^(facebook|instagram|pinterest|reddit|twitter|linkedin)") = true
    and REGEXP_CONTAINS({{medium}}, r"^(cpc|ppc|paid)") = true
    then 'Paid Social'
  when REGEXP_CONTAINS({{source}}, r"^(facebook|instagram|pinterest|reddit|twitter|linkedin)") = true
    or REGEXP_CONTAINS({{medium}}, r"^(social|social-network|social-media|sm|social network|social media)") = true
    or {{source_category}} = 'SOURCE_CATEGORY_SOCIAL' 
    then 'Organic Social'
  when REGEXP_CONTAINS({{medium}}, r"email|e-mail|e_mail|e mail") = true
    or REGEXP_CONTAINS({{source}}, r"email|e-mail|e_mail|e mail") = true
    then 'Email'
  when REGEXP_CONTAINS({{medium}}, r"affiliate|affiliates") = true
    then 'Affiliates'
  when {{source_category}} = 'SOURCE_CATEGORY_SHOPPING' and REGEXP_CONTAINS({{medium}},r"^(.*cp.*|ppc|paid.*)$")
    then 'Paid Shopping'
  when ({{source_category}} = 'SOURCE_CATEGORY_VIDEO' AND REGEXP_CONTAINS({{medium}},r"^(.*cp.*|ppc|paid.*)$"))
    or {{source}} = 'dv360_video'
    then 'Paid Video'
  when REGEXP_CONTAINS({{medium}}, r"^(display|cpm|banner)$")
    or {{source}} = 'dv360_display'
    then 'Display'
  when REGEXP_CONTAINS({{medium}}, r"^(cpc|ppc|paidsearch)$")
    then 'Paid Search'
  when REGEXP_CONTAINS({{medium}}, r"^(cpv|cpa|cpp|content-text)$")
    then 'Other Advertising'
  when {{medium}} = 'organic' or {{source_category}} = 'SOURCE_CATEGORY_SEARCH'
    then 'Organic Search'
  when REGEXP_CONTAINS({{medium}}, r"^(.*video.*)$") or {{source_category}} = 'SOURCE_CATEGORY_VIDEO'
    then 'Organic Video'
  when {{source_category}} = 'SOURCE_CATEGORY_SHOPPING'
    then 'Organic Shopping'
  when {{medium}} = 'referral'
    then 'Referral'
  when {{medium}} = 'audio'
    then 'Audio'
  when {{medium}} = 'sms'
    then 'SMS'
  else '(Other)' 
end 

{% endmacro %}