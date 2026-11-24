{#
  Issuers type the trading symbol into Form 4 by hand, so silver holds values like
  'NYSE: KRC', '(SIRI)', 'LEN, LEN.B', 'Z AND ZG' or 'N O G'. Silver keeps them exactly as
  filed; this macro turns them into one usable symbol:
    1. upper-case and trim
    2. drop an exchange prefix such as 'NYSE:' or 'NASDAQ/'
    3. drop brackets and '$'
    4. 'N O G' (single letters with spaces) -> 'NOG'
    5. several share classes listed -> keep the first ('LEN, LEN.B' -> 'LEN')
    6. anything still not a plausible symbol -> NULL (no price lookup, no wrong match)
#}
{% macro clean_ticker(column) %}
(
    with step as (
        select regexp_replace(
                   regexp_replace(
                       upper(trim({{ column }})),
                       '^(NYSE AMERICAN|NYSE MKT|NYSE|NASDAQ|NASD|AMEX|OTCQX|OTCQB|OTC|CBOE)\s*[:/]\s*',
                       ''),
                   '[()$]', '', 'g') as t
    ),
    joined as (
        select case when t ~ '^([A-Z] )+[A-Z]$' then replace(t, ' ', '') else t end as t
        from step
    ),
    first_symbol as (
        select split_part(
                   regexp_replace(t, '\s+AND\s+|\s*[,/;&]\s*|\s+', '|', 'g'), '|', 1) as t
        from joined
    )
    select case
        when t in ('', 'NONE', 'N/A', 'NA', 'NULL', '-') then null
        when t ~ '^[A-Z][A-Z0-9]{0,5}([.\-][A-Z0-9]{1,2})?$' then t
        else null
    end
    from first_symbol
)
{% endmacro %}
