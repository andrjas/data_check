select
    'string' as string_test,
    42 as int_test,
    42.1 as float_test,
    to_char(to_date('2020-12-20', 'yyyy-mm-dd'), 'yyyy-mm-dd') as date_test,
    null as null_test
from dual
