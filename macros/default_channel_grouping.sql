-- Inspired by https://github.com/llooker/ga_four_block_dev/blob/master/views/sessions.view.lkml
-- Google's documentation is here: https://support.google.com/analytics/answer/9756891?hl=en

{% macro default_channel_grouping(source, medium) %}

case 
  when {{source}} is null and {{medium}} is null 
    then null
  when {{source}} = '(direct)'
    and ({{medium}} = '(none)' or {{medium}} = '(not set)')
    then 'Direct'
  when {{medium}} = 'organic'
    then 'Organic Search'
  when REGEXP_CONTAINS({{source}}, r"^(facebook|instagram|pinterest|reddit|twitter|linkedin)") = true
    and REGEXP_CONTAINS({{medium}}, r"^(cpc|ppc|paid)") = true
    then 'Paid Social'
  when REGEXP_CONTAINS({{source}}, r"^(facebook|instagram|pinterest|reddit|twitter|linkedin)") = true
    or REGEXP_CONTAINS({{medium}}, r"^(social|social-network|social-media|sm|social network|social media)") = true
    then 'Organic Social'
  when REGEXP_CONTAINS({{medium}}, r"email|e-mail|e_mail|e mail") = true
    or REGEXP_CONTAINS({{source}}, r"email|e-mail|e_mail|e mail") = true
    then 'Email'
  when REGEXP_CONTAINS({{medium}}, r"affiliate|affiliates") = true
    then 'Affiliates'
  when {{medium}} = 'referral'
    then 'Referral'
  when REGEXP_CONTAINS({{medium}}, r"^(cpc|ppc|paidsearch)$")
    then 'Paid Search'
  when REGEXP_CONTAINS({{medium}}, r"^(display|cpm|banner)$")
    then 'Display'
  when REGEXP_CONTAINS({{medium}}, r"^(cpv|cpa|cpp|content-text)$")
    then 'Other Advertising'
  when REGEXP_CONTAINS({{medium}}, r"^(.*video.*)$")
    then 'Organic Video'
  when {{medium}} = 'audio'
    then 'Audio'
  when {{medium}} = 'sms'
    then 'SMS'
  else '(Other)' 
end 

{% endmacro %}