-- Unnests a single key's value from an array. Use value_type = 'lower_string_value' to produce a lowercase version of the string value
-- integrate_string_value_ga_session_id override:
-- string_value ga_session_id in following format: 's1747075570$o5$g1$t1747077281$j36$l0$h0'
-- s1747075570: Session ID
-- $o1: Session Count
-- $g0: Session Engaged
-- $t1747077281: Timestamp of first event in session
-- $j0: Countdown
-- $l0: Unknown
-- $h0: Enhanced Client ID (User ID?)

{%- macro unnest_key(column_to_unnest, key_to_extract, value_type = "string_value", rename_column = "default") -%}
    {{ return(adapter.dispatch('unnest_key', 'ga4')(column_to_unnest, key_to_extract, value_type, rename_column)) }}
{%- endmacro -%}

{%- macro default__unnest_key(column_to_unnest, key_to_extract, value_type = "string_value", rename_column = "default") -%}
    {% if var('integrate_string_value_ga_session_id', false) == true and key_to_extract == "ga_session_id" and column_to_unnest == "event_params"  %}
    coalesce(
        (select value.int_value from unnest(event_params) where key = 'ga_session_id')
        , safe_cast(
            split(
                split(
                    (select value.string_value from unnest(event_params) where key = 'ga_session_id')
                    , '$'              -- split on dollar sign ($)
                )[safe_offset(0)]      -- and get the first element after splitting on dollar sign ($)
                , 's'                  -- split on letter
            )[safe_offset(1)]          -- and get the second element after splitting on letter
        as int64)                      -- cast to int64
        , 0                            -- default value if not found

    ) as
        {% if  rename_column == "default" %}
            {{ key_to_extract }}
        {% else %}
            {{rename_column}}
        {% endif %}
    {% else %}
        (select 
            {% if value_type == "lower_string_value" %}
                lower(value.string_value)   
            {% else %}
                value.{{value_type}}    
            {% endif %}
        from unnest({{column_to_unnest}}) where key = '{{key_to_extract}}') as 
        {% if  rename_column == "default" %}
            {{ key_to_extract }}
        {% else %}
            {{rename_column}}
        {% endif %}
    {% endif %}
{%- endmacro -%}
