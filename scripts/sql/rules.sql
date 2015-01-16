

select count (*) from (
select rm.shipping_from_id, rm.shipping_to_id
from shipping_location_migration lm inner join rule_migration rm on (lm.id = rm.shipping_from_id or lm.id = rm.shipping_to_id)
where not exists
(
  select 1 from shipping_location l where lm.id = l.id
)
group by rm.shipping_from_id, rm.shipping_to_id
having count(*) < 16);


select rm.shipping_from_id, rm.shipping_to_id
from shipping_location_migration lm inner join rule_migration rm on (lm.id = rm.shipping_from_id or lm.id = rm.shipping_to_id)
group by rm.shipping_from_id, rm.shipping_to_id;

select * from shipment;
select * from rule_migration;
select * from shipping_location_migration;

select lmFrom.state state_from, lmTo.state state_to, count(*)
from shipment s inner join rule_migration rm on (s.applied_shipping_rule_id = rm.id)
                inner join shipping_location_migration lmFrom on (lmFrom.id = rm.shipping_from_id)
                inner join shipping_location_migration lmTo on (lmTo.id = rm.shipping_to_id)
where s.date_created > sysdate-25 and lmFrom.country = 'BR' and lmTo.country = 'BR'
group by lmFrom.state, lmTo.state
order by count(*) desc;
/*
BR-SP	BR-SP
BR-SP	BR-MG
BR-MG BR-SP
BR-SP	BR-RJ
BR-RJ BR-SP
BR-SP	BR-RS
BR-RS BR-SP
BR-PR	BR-SP
BR-SP BR-PR
BR-RJ BR-RJ
*/
select * from shipping_methods;

select *
from shipment
where id = 20705794020;

