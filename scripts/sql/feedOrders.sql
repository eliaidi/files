select *
from shp_w01.shipment
where site_id = 'MLB' and date_created > sysdate - 1/24 and status = 'ready_to_ship';
select count(*)
from shp_w01.shipment
where cost = 0 and site_id = 'MLB' and date_created > sysdate - 1;

select *
from shp_w01.shipment
where id = 20710086692;
-- Mi shipment id = 20710086692

select *
from shp_w01.shipping_costs
where order_id in ('786629289');
select *
from shp_w01.shipment
where order_id = '789158766';--'789167072';

select status
from shp_w01.shipment
where order_id in ('789195619','788776618','789229849','789223761','789221216','788789966');

select *
from shp_w01.shipping_costs
where order_id in ('789195619','788776618','789229849','789223761','789221216','788789966');



select *
from shp_w01.shipment
where id in (20707281940,20707250576);
--MLB505457422 MLB508023438

select *
from shp_w01.shipping_costs
where order_id = '786965830';

select *
from shp_w01.rules;
where id = 34001071;

select *
from shp_w01.shipment
where order_id = '784977571';
select *
from shp_w01.shipment
where status = 'ready_to_ship' and date_created > sysdate - 1 and site_id = 'MLB';



select *
from shp_w01.shipment
where status = 'pending' and date_created > sysdate -1 and site_id = 'MLA'
order by date_created desc;

select s.applied_shipping_rule_id, s.date_created, s.date_handling, s.last_updated, s.order_id, s.receiver_address, a.zip_code, s.status, s.shipping_method_id, s.service_id, s.item_id
from shp_w01.shipment s, shp_w01.shipping_address a
where s.id in (20707281940,20707250576) and s.receiver_address = a.id;
--MLB505457422 MLB508023438
-- closed        active
select *
from SHP_W01.zipcode_migration
where zip_code = 49048523;
select *
from shp_w01.shipment
where order_id = '789246455';
select *
from shp_w01.shipping_costs
where order_id = '789246455';

