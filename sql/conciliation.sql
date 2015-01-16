select s.real_cost, d.amount_charged, s.real_cost - d.amount_charged
from shp_w01.shipment s, shp_w01.real_shipment_data d
where s.site_id = 'MLB'
--and s.date_created > sysdate - 1
and s.status in ('ready_to_ship','shipped','delivered','not_delivered')
and s.cost = 0
and s.id = d.shipment_id
and s.id = 20901186363;
order by 3 desc;

select s.id, s.tracking_number, s.index_field, s.item_id, s.category_id, s.sender_id, s.real_cost, r.amount_charged, r.weight, r.volume, r.height, r.length, r.width
from shp_w01.shipment s, shp_w01.real_shipment_data r
where s.id in (20786948406,20787086538,20848533444,20890833339,20900831526,20901237857,20901836939,20901186363,20908431382,20908231745)
and s.id = r.shipment_id
order by (r.amount_charged - s.real_cost) desc;
-- PD325206919BR 
-- JADERRICELLYALMEIDADASILVA,MLB524509974,Celular Smartphone Lg L7 Optimus Android 4.0 Melhor Preço !!,805287570
-- MLB524509974
-- MLB12969


select *
from shp_w01.adoption
where user_id = 128236050;
where shipping_option = 'trial';

select *
from shp_w01.adoption_log l
where user_id = 128236050;
where not exists (select 1 from shp_w01.adoption a where a.user_id = l.user_id and a.site_id = 'MLB');

select s.real_cost, d.amount_charged
from shp_w01.real_shipment_data d, shp_w01.shipment s
where d.shipment_id in (20901186363)
and s.id = d.shipment_Id;

select *
from shp_w01.shipment
where item_id = 'MLB521964285'
and cost = 0;
where id = 20908431382;



