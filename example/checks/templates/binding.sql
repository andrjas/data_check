with dat as (
    select 1 as a, 'a' as b {{from_dual}}
    union all
    select 2 as a, 'b' as b {{from_dual}}
)
select a
from dat
where b in :b1
 or a in :sub_lkp__b2
