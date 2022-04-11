select bkey
from main.simple_fake_table
where col3 not in (1, 2, 3, 5, 8)
