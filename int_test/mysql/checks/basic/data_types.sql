select
    'string' as string_test,
    42 as int_test,
    42.1 as float_test,
    DATE_FORMAT(STR_TO_DATE('2020-12-20', '%Y-%m-%d'), '%Y-%m-%d') as date_test,
    null as null_test,
    '   ' as whitespace_test,
    '' as empty_string_test
