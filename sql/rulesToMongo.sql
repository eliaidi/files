                                        --JUEVES          --LUNES

select count(*)
from shp_w01.rule_migration;            --1.942.204       --2.152.714

select count(*)
from shp_w01.rule_migration
where date_created like sysdate;        --10.283          
select count(*)
from shp_w01.rule_migration
where last_updated like sysdate;        --18.687          

select count(*)
from shp_w01.rule_migration
where date_created like sysdate-1;      --15.222
select count(*)
from shp_w01.rule_migration
where last_updated like sysdate-1;      --51.862

select count(*)
from shp_w01.rule_migration
where date_created like sysdate-2;      --4.776
select count(*)
from shp_w01.rule_migration
where last_updated like sysdate-2;      --16.524

select count(*)
from shp_w01.rule_migration
where date_created like sysdate-3;      --0
select count(*)
from shp_w01.rule_migration
where last_updated like sysdate-3;      --0

-----------------------------------
select * from shp_w01.shipping_location_migration;
select * from shp_w01.rule_migration;

select rm.shipping_from_id, rm.shipping_to_id
from shp_w01.rule_migration rm inner join shp_w01.shipping_location_migration lm on (lm.id = rm.shipping_from_id or lm.id = rm.shipping_to_id)
where rm.date_created > sysdate-7
group by rm.shipping_from_id, rm.shipping_to_id
having count(*) = 16; --240.839  --4.560  --420

select count(*) from (
select /*+ parallel(4)*/ r.shipping_from_id, r.shipping_to_id, count(*) 
from shp_w01.rule_migration r, shp_w01.shipping_location_migration sf, shp_w01.shipping_location_migration st
where r.shipping_from_id = sf.id
and r.shipping_to_id = st.id
and r.shipping_method_id = 500145 
and r.date_created > to_date('31/08/13 00:00:00','DD/MM/YY HH24:MI:SS')
and sf.state = 'BR-MG' and st.state = 'BR-SP'
group by r.shipping_from_id, r.shipping_to_id
having count(*) = 16);

/*
 select r.shippingFrom.id, r.shippingTo.id " +
		        " from Rules as r " +
		        "    inner join r.shippingFrom as sf " +
		        "    inner join r.shippingTo as st " +
		        " where r.shippingMethod.id = 500145 " +
		        "    and r.dateCreated > :date " +
		        " group by r.shippingFrom.id, r.shippingTo.id " +
		        " having count(*) = 16"
*/

select /*+ parallel(4)*/ sf.state, st.state, count(*) cnt
from shp_w01.rule_migration r, shp_w01.shipping_location_migration sf, shp_w01.shipping_location_migration st
where r.shipping_from_id = sf.id
and r.shipping_to_id = st.id
and r.shipping_method_id = 500145 
and r.date_created > to_date('8/09/13 01:00:00','DD/MM/YY HH24:MI:SS')
group by sf.state, st.state
order by cnt desc;


select *
from shp_w01.shipping_location_migration;

select count(*)
from shp_w01.rule_migration
where last_updated > sysdate-7; --298,004

select sysdate-7
from dual;

select * from shp_w01.shipping_methods;

select sender_id, site_id from shp_w01.shipment where date_created > sysdate - 60 and sender_id in (124111736,124157776,124156487,68280533,64947550,52997966) group by sender_id, site_id;
