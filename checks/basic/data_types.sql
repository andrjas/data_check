select
    'string' as string_test,
    42 as int_test,
    42.1 as float_test,
    {{test_date}} as date_test,
    null as null_test,
    '   ' as whitespace_test,
    '' as empty_string_test,
    {{huge_date}} as inf_date_test
{{from_dual}}
