{% macro partner_id_extract() %}
    {{ return(adapter.dispatch('partner_id_extract', 'ga4')()) }}
{% endmacro %}

{% macro default__partner_id_extract() %}
		event_and_query_string as 
		(
				select 
						event_key,
						split(page_query_string, '&') as qs_split
				from {{ref('stg_ga4__events')}}
		),
		flattened_qs as
		(
				select 
						event_key, 
						params 
				from event_and_query_string, unnest(qs_split) as params
		),
		split_param_value as 
		(
				select 
						event_key, 
						split(params,'=')[SAFE_OFFSET(0)] as param, 
				from flattened_qs
		),
		add_parner_id as 
		(
				select 
					*,
						LTRIM(REGEXP_EXTRACT(param,r'^p[0-9]+'),'p') as partner_id
				from split_param_value
		)
{% endmacro %}
