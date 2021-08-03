-- date: date_test, inf_date_test
select
    'string' as string_test,
    42 as int_test,
    42.1 as float_test,
    datetime('2020-12-20 00:00:00') as date_test,
    null as null_test,
    '   ' as whitespace_test,
    '' as empty_string_test,
    datetime('9999-12-31 00:00:00') as inf_date_test
