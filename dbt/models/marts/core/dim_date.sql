-- Calendar dimension, 2000-2030. Generated in SQL so no seed file or package is needed.
select
    d::date as date_day,
    extract(year from d)::int as year,
    extract(quarter from d)::int as quarter,
    extract(month from d)::int as month,
    to_char(d, 'Mon') as month_name,
    extract(isodow from d)::int as day_of_week,
    to_char(d, 'Dy') as day_name,
    extract(isodow from d) in (6, 7) as is_weekend
from generate_series('2000-01-01'::date, '2030-12-31'::date, interval '1 day') as d
