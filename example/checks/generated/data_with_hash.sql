select 0 as id, 'abc' as txt, 1 as num {{from_dual}}
union all
select 1 as id, 'def#' as txt, 2 as num {{from_dual}}
union all
select 2 as id, '#xyz' as txt, 3 as num {{from_dual}}
