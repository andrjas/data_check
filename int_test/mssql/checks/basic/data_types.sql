select
    'string' as string_test,
    42 as int_test,
    42.1 as float_test,
    CONVERT(DATETIME, '2020-12-20', 120) as date_test,
    null as null_test,
    '   ' as whitespace_test,
    '' as empty_string_test
