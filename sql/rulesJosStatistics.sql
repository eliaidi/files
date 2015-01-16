select count(*)
from rule_migration; --1.942.204

select count(*)
from rule_migration
where date_created like sysdate; --10.283
select count(*)
from rule_migration
where last_updated like sysdate; --18.687

select count(*)
from rule_migration
where date_created like sysdate-1; --15.222
select count(*)
from rule_migration
where last_updated like sysdate-1; --51.862

select count(*)
from rule_migration
where date_created like sysdate-2; --4.776
select count(*)
from rule_migration
where last_updated like sysdate-2; --16.524

select count(*)
from rule_migration
where date_created like sysdate-3; --0
select count(*)
from rule_migration
where last_updated like sysdate-3; --0




