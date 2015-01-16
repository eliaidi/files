select *
from shp_w01.shipping_service;

select *
from shp_w01.shipment
where status = 'ready_to_ship'
and date_first_printed is null
and service_id in (1,2)
and date_handling between sysdate-5 and sysdate;

select *
from shp_w01.shipment
where service_id in (1,2)
and date_created > sysdate - 1;


select status, service_id, override_service_id, shipping_method_id, tracking_number, date_handling, date_ready_to_ship, last_updated, s.*
from shp_w01.shipment s
where receiver_id = 140443460
and sender_id = 145180079
and status <> 'cancelled'
and date_created > to_date('15/11/13 08:10:00','DD/MM/YY HH24:MI:SS');

select sysdate from dual;


select *
from shp_w01.shipment
where order_id = '788914181';



select *
from shp_w01.shipment
where id = 20784702977;

-- TODO: SUMAR 2 DIAS A LAS FECHAS DE LOS ENVIOS
-- Quedarian los envios en 3 de noviembre. Hacer PUT a la api health el 4 a la ma–ana. Correr los jobs y ver que pasa.
select date_created, date_handling, date_ready_to_ship, date_first_printed, service_id, override_service_id
from shp_w01.shipment
where id in (20784736807,20784736704,20784736787,20784736836,20784736891,20784736914,20784737079,20784737156,20784707931,20784708252,20784708287,20784708341,20784708540,20784702977,20784708044,20784708146,20784708316,20784708381,20784708486,
20784708403) or id in (20785049643,20785049670,20784806759,20785049740,20784806772,20785049806);

-- 13 pac, 14 esedex, 1 sedex
select id, status, service_id, override_service_id, shipping_method_id, speed, applied_shipping_rule_id, tracking_number, date_created, date_handling, date_ready_to_ship, last_updated, date_first_printed printed, sender_id, receiver_id, cost, real_cost, item_id, order_id, index_field
from shp_w01.shipment
where id in (20784736807,20784736704,20784736787,20784736836,20784736891,20784736914,20784737079,20784737156,20784707931,20784708252,20784708287,20784708341,20784708540,20784702977,20784708044,20784708146,20784708316,20784708381,20784708486,
20784708403,20785049643,20785049670,20784806759,20785049740,20784806772,20785049806,20785054498,20784809309)
order by service_id, override_service_id;

select *
from shp_w01.tracking_Number
where shipping_id in (20784736807,20784736704,20784736787,20784736836,20784736891,20784736914,20784737079,20784737156,20784707931,20784708252,20784708287,20784708341,20784708540,20784702977,20784708044,20784708146,20784708316,20784708381,20784708486,
20784708403,20785049643,20785049670,20784806759,20785049740,20784806772,20785049806,20785054498,20784809309);

select *
from shp_w01.tracking_number
where date_created > sysdate - (1/24/60*5)
order by date_created desc;



select s.id
from shp_w01.shipment s
where s.id in (20784736807,20784736704,20784736787,20784736836,20784736891,20784736914,20784737079,20784737156,20784707931,20784708252,20784708287,20784708341,20784708540,20784702977,20784708044,20784708146,20784708316,20784708381,20784708486,
20784708403) or s.id in (20785049643,20785049670,20784806759,20785049740,20784806772,20785049806,20785054498,20784809309)
and not exist (
select *
from shp_w01.shipment ss
where ss.site_id ='MLB'
and ss.service_id in (1,2,3)
and ss.date_handling between sysdate - 2 and sysdate
and ss.shipping_mode = 'me2'
and ss.id = s.id
);
(20784736787, 20784736836, 20784736891, 20784736914, 20784737156, 20784707931, 20784708287, 20784708341, 
20784708540, 20784702977, 20784708044, 20784708146, 20784708316, 20784708381, 20784708486, 20784708403, 
20785049740, 20785049643, 20785049670, 20784806772, 20784806759, 20785054498, 20784809309)

select *
from shp_w01.shipping_service
where id in (1,2,3);


select *
from shp_w01.shipment
where service_id in (1,2,3)
and status = 'ready_to_ship'
and date_handling between sysdate -2 and sysdate
and date_first_printed is null;


select *
from shp_w01.shipment
where id in (20784799112,20784797364, 20784805941);

egrep "798694657|798706470|798744971" /usr/local/log/orderFeed.log

select *
from shp_w01.shipment
where order_id in ('798706470','798744971') or id in (20785041227,20785067022);

select *
from shp_w01.shipment
where id in (20785041227);

select max(date_created)
from shp_w01.shipment
where site_id = 'MLA' and date_created > sysdate - (1/24/60);



