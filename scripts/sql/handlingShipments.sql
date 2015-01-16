select service_id, to_char(date_handling, 'dd-MM-yy'), count(*)
from shp_w01.shipment
where status = 'handling' and site_id = 'MLB' and date_created > sysdate - 30 and shipping_mode = 'me2'
group by service_id, to_char(date_handling, 'dd-MM-yy')
order by 2;

select count(*)
from shp_w01.shipment s--, shp_w01.shipping_costs c
where s.status = 'handling' and s.site_id = 'MLB' and s.date_created > sysdate - 30 and s.shipping_mode = 'me2' and s.date_handling <= sysdate - 1-- and s.order_id = c.order_id
order by date_created;



select count(*)
from shp_w01.shipment
where site_id = 'MLB' and date_created > sysdate - 30 and shipping_mode = 'me2' and date_ready_to_ship >= sysdate - 1; --5805


select count(*)
from shp_w01.shipment
where site_id = 'MLB' and date_created > sysdate - 30 and shipping_mode = 'me2' and date_ready_to_ship < sysdate - 1 and date_ready_to_ship >= sysdate - 4; --11059


select *
from shp_w01.shipment
where order_id in ('789451686','788401937','789445924','789322677','788488356','789442350','788411264','789445274','789450472','789438988','788463592','789449251','789448374');




select count(*), site_id, service_id
from shp_w01.shipment s, shp_w01.shipping_address sa, shp_w01.shipping_address ra
where s.date_created > sysdate-31
and s.date_created < sysdate-1
and s.shipping_mode = 'me2'
and s.status = 'handling'
and s.sender_address = sa.id
and sa.address_line is not null
and sa.zip_code is not null
and sa.city_id is not null
and sa.city_name is not null
and sa.state_id is not null
and sa.state_name is not null
and sa.country_id is not null
and sa.country_name is not null
and s.receiver_address = ra.id
and ra.address_line is not null
and ra.zip_code is not null
and ra.city_id is not null
and ra.city_name is not null
and ra.state_id is not null
and ra.state_name is not null
and ra.country_id is not null
and ra.country_name is not null
group by site_id, service_id
order by site_id;

select s.service_id serId, s.override_service_id overSerId,
       sa.zip_code sa_zipcode, sa.street_name sa_sname, sa.street_number sa_snumber, sa.address_line sa_addLine, sa.city_id sa_ciId, sa.city_Name sa_ciName, sa.state_id sa_stId, sa.state_name sa_stName, sa.country_id sa_coId, sa.country_name sa_coName,
       ra.zip_code ra_zipcode, ra.street_name ra_sname, ra.street_number ra_snumber, ra.address_line ra_addLine, ra.city_id ra_ciId, ra.city_Name ra_ciName, ra.state_id ra_stId, ra.state_name ra_stName, ra.country_id ra_coId, ra.country_name ra_coName,
       s.*
from shp_w01.shipment s, shp_w01.shipping_address sa, shp_w01.shipping_address ra
where /*s.date_created > sysdate-30
and s.date_created < sysdate-1
and */ s.shipping_mode = 'me2'
and s.status = 'handling'
and s.sender_address = sa.id
and sa.address_line is not null
and sa.zip_code is not null
and sa.city_name is not null
and sa.state_id is not null
and sa.state_name is not null
and sa.country_id is not null
and sa.country_name is not null
and sa.street_name is not null
and sa.street_number is not null
and s.receiver_address = ra.id
and ra.address_line is not null
and ra.zip_code is not null
and ra.city_name is not null
and ra.state_id is not null
and ra.state_name is not null
and ra.country_id is not null
and ra.country_name is not null
and ra.street_name is not null
and ra.street_number is not null
and s.id in (20708086468,20708000059,20708148640,20708118869,20708251394,20708065927,20707993657,20708268119,20708267987,20708269422,
             20708270848,20708191329,20708191681,20708192367,20708271807,20708194365,20708273851,20708274565,20707607293,20708197448,
             20708276932,20708276976,20708199161,20708199425,20708201539,20708201780,20708280132,20708280174,20708203729,20708205382,
             20708282678,20708283360,20708284117,20708285235,20708208875,20708212791,20708289466);
             
select *
from shp_w01.tracking_Number
where shipping_id in (20707993657,20708000059,20708065927,20708086468,20708118869,20708148640,20708251394);


select *
from shipping_logs
where ( shipment_id in (20707993657,20708000059,20708065927,20708086468,20708118869,20708148640,20708251394)
        or order_id in ('789253258','789264463','789155086','789270517','789449820','789526894','789624206') )
and ds >= '2013-09-16 05:00:00' and ds <= '2013-09-19 12:00:00';














select s.service_id serId, s.override_service_id overSerId,
       sa.zip_code sa_zipcode, sa.street_name sa_sname, sa.street_number sa_snumber, sa.address_line sa_addLine, sa.city_id sa_ciId, sa.city_Name sa_ciName, sa.state_id sa_stId, sa.state_name sa_stName, sa.country_id sa_coId, sa.country_name sa_coName,
       ra.zip_code ra_zipcode, ra.street_name ra_sname, ra.street_number ra_snumber, ra.address_line ra_addLine, ra.city_id ra_ciId, ra.city_Name ra_ciName, ra.state_id ra_stId, ra.state_name ra_stName, ra.country_id ra_coId, ra.country_name ra_coName,
       s.*
from shp_w01.shipment s, shp_w01.shipping_address sa, shp_w01.shipping_address ra
where s.service_id in (21,22) 
and s.date_created > sysdate-30
and s.date_created < sysdate-((1/24/60)*10)
and s.shipping_mode = 'me2'
and s.status = 'handling'
and s.sender_address = sa.id
and sa.address_line is not null
and sa.zip_code is not null
and sa.city_name is not null
and sa.state_id is not null
and sa.state_name is not null
and sa.country_id is not null
and sa.country_name is not null
and sa.street_name is not null
and sa.street_number is not null
and s.receiver_address = ra.id
and ra.address_line is not null
and ra.zip_code is not null
and ra.city_name is not null
and ra.state_id is not null
and ra.state_name is not null
and ra.country_id is not null
and ra.country_name is not null
and ra.street_name is not null
and ra.street_number is not null;

select s.service_id serId, s.override_service_id overSerId, s.tracking_number, s.id ship_id, f.*,
       sa.zip_code sa_zipcode, sa.address_line sa_addLine, sa.city_id sa_ciId, sa.city_Name sa_ciName, sa.state_id sa_stId, sa.state_name sa_stName, sa.country_id sa_coId, sa.country_name sa_coName,
       ra.zip_code ra_zipcode, ra.address_line ra_addLine, ra.city_id ra_ciId, ra.city_Name ra_ciName, ra.state_id ra_stId, ra.state_name ra_stName, ra.country_id ra_coId, ra.country_name ra_coName,
       s.*
from shp_w01.shipment s, shp_w01.shipping_address sa, shp_w01.shipping_address ra, shp_w01.zipcode_migration z1, shp_w01.zipcode_migration z2, shp_w01.correios_shipment_file f
where s.service_id in (1,2)
and s.date_created > sysdate-30
and s.date_created < sysdate-1
and s.shipping_mode = 'me2'
and s.status = 'handling'
and s.sender_address = sa.id
and sa.address_line is not null
and sa.zip_code is not null
and sa.city_name is not null
and sa.state_id is not null
and sa.state_name is not null
and sa.country_id is not null
and sa.country_name is not null
and s.receiver_address = ra.id
and ra.address_line is not null
and ra.zip_code is not null
and ra.city_name is not null
and ra.state_id is not null
and ra.state_name is not null
and ra.country_id is not null
and ra.country_name is not null
and z1.zip_code = sa.zip_code
and z2.zip_code = ra.zip_code
and f.shipping_id = s.id;

select s.sender_id, count(*)
from shp_w01.shipment s, shp_w01.shipping_address sa, shp_w01.shipping_address ra, shp_w01.zipcode_migration z1, shp_w01.zipcode_migration z2
where s.service_id in (1,2)
and s.date_created > sysdate-30
and s.date_created < sysdate-2
and s.shipping_mode = 'me2'
and s.status = 'handling'
and s.sender_address = sa.id
and s.site_id = 'MLB'
and sa.address_line is not null
and sa.zip_code is not null
and sa.city_name is not null
and sa.state_id is not null
and sa.state_name is not null
and sa.country_id is not null
and sa.country_name is not null
and s.receiver_address = ra.id
and ra.address_line is not null
and ra.zip_code is not null
and ra.city_name is not null
and ra.state_id is not null
and ra.state_name is not null
and ra.country_id is not null
and ra.country_name is not null
and z1.zip_code = sa.zip_code
and z2.zip_code = ra.zip_code
group by s.sender_id
order by s.sender_id;



select s.*
from shp_w01.shipment s, shp_w01.shipping_address sa, shp_w01.shipping_address ra, shp_w01.zipcode_migration z1, shp_w01.zipcode_migration z2
where s.service_id in (1,2)
and s.date_created > sysdate-30
and s.date_created < sysdate-1
and s.shipping_mode = 'me2'
and s.status = 'handling'
and s.sender_address = sa.id
and sa.address_line is not null
and sa.zip_code is not null
and sa.city_name is not null
and sa.state_id is not null
and sa.state_name is not null
and sa.country_id is not null
and sa.country_name is not null
and s.receiver_address = ra.id
and ra.address_line is not null
and ra.zip_code is not null
and ra.city_name is not null
and ra.state_id is not null
and ra.state_name is not null
and ra.country_id is not null
and ra.country_name is not null
and z1.zip_code = sa.zip_code
and z2.zip_code = ra.zip_code
and s.id = 20705810013;

select *
from shp_w01.correios_shipment_file
where shipping_id in (20706356609,20705810013);

select *
from shp_w01.shipment
where id = 20705810013;



select *
from shp_w01.correios_shipment_file
where shipping_id = 20705810665;

select *
from shp_w01.tracking_Number
where shipping_id = 20705915501;



select *
from shp_w01.correios_shipment_file f
where f.output_file_status = 'error' and creation_date > sysdate - 30;


select f.shipping_id, f.input_file_name, f.input_file_status, f.output_file_name, f.output_file_status, f.creation_date, f.last_update
from shp_w01.correios_shipment_file f
where f.creation_date > sysdate - 30
order by f.shipping_id, f.last_update desc;
group by f.shipping_id,f.input_file_name, f.input_file_status, f.output_file_name, f.output_file_status;


select *
from shp_w01.correios_shipment_file
where last_update > sysdate - (1/24)*2 and input_file_status = 'request_retry';


select s.status, s.date_created, s.last_updated, s.last_processed_order, s.order_id, s.sender_id, s.*
from shp_w01.shipment s
where order_id in ('789585187');

select *
from shp_w01.shipment
where status = 'ready_to_ship' and date_created > sysdate - 1;