select 1 as i, {{test_date}} as date_test {{from_dual}}
union all
select 2 as i, {{empty_string_date}} as date_test {{from_dual}}
union all
select 3 as i, null as date_test {{from_dual}}
