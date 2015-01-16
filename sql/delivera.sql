Select *
from shipment
where service_id = 73 and sender_id = 912473 and status not in ('pending','shipped','delivered','not_delivered');

Select s.id,s.tracking_number, e.*
from shp_w01.tracking_events e, shp_w01.shipment_tracking_events se, shp_w01.shipment s
where s.id = se.shipment_trackings_id and se.tracking_events_id = e.id 
and s.id in (
  Select id
  from shipment
  where service_id = 73 and sender_id in (124128126,68280533) and status in ('shipped','delivered','not_delivered')
);

Select *
from carrier_tracking_notification
where application_id = 4153630522742212
order by date_created desc;

Select id, date_created, date_handling, date_ready_to_ship, date_shipped, date_delivered, date_cancelled, last_updated, receiver_id, sender_id, service_id, status, tracking_number, order_id, date_first_printed, receiver_address
from shipment
where service_id = 73 and sender_id in (124128126,68280533) and status in ('shipped','delivered','not_delivered')
order by last_updated desc;

Select id, date_created, date_handling, date_ready_to_ship, date_shipped, date_delivered, date_cancelled, last_updated, receiver_id, sender_id, service_id, status, tracking_number, order_id, date_first_printed, receiver_address
from shipment
where service_id = 73 and sender_id in (124128126,68280533) and status in ('ready_to_ship') and date_first_printed is not null
order by date_first_printed;



Select *
from shipping_preference
where user_id = 124128126;

0627849802-0681109811-0630699976-0681410043-0626940038-0684109793-0652489956-0689959942-0618259957-0631270050-0643809976

Select id, date_created, date_handling, date_ready_to_ship, date_shipped, date_delivered, date_cancelled, last_updated, receiver_id, sender_id, service_id, status, tracking_number, order_id, date_first_printed, receiver_address
from shipment
where tracking_number = '0621736767';-- '0621736767';--'0554574963';
Select *
from carrier_tracking_notification
where tracking_number = '0516395024';--'0621736767';
Select max(count_repeat_notification)
from carrier_tracking_notification
where application_id = 4153630522742212;
--egrep '(0671429895|0575409331|0588857057|0618290137|0692820020|0634470105|0605040104|0642460097|0615590098|0620150018|0664960087|0641170005|0696860073|0626190075|0624570070|0658139933|0519137959|0519807357)' usr/local/log/app.log
--egrep '(0582337338|0579337047|0619469806|0648549790|0643809976|0677220056|0631270050|0618259957|0689959942|0652489956|0684109793|0626940038|0681410043|0630699976|0657609963|0637959965|0518127056|0544476348)' usr/local/log/app.log
--egrep '(0619429954|0612779953|0625799820|0681109811|0697209816|0664309806|0627849802|0623009807|0693139889|0650179888|0652789880|0625668003|0629807953|0654109836|0674769564|0663597961)' usr/local/log/app.log

--egrep '(0540747030|0554574963|0531394956|0523177638|0549716635|0559803459|0516333962|0583753552|0540264231|0577424232|0545605132|0533655024|0503923764|0508373577|0557773632|0556373636|0522163561)' usr/local/log/app.log

update Shipment
set date_shipped = TO_DATE('20-JUN-13')
where tracking_number in ('0556373636');

update Shipment
set date_shipped = TO_DATE('27-JUN-13'), date_delivered = TO_DATE('29-JUN-13')
where tracking_number in ('0599885840');

update Shipment
set date_shipped = TO_DATE('27-JUN-13'), date_delivered = TO_DATE('29-JUN-13')
where tracking_number in ('0515315138');

update Shipment
set date_shipped = TO_DATE('27-JUN-13')
where tracking_number in ('0522163561');

update Shipment
set date_shipped = TO_DATE('30-JUN-13')
where tracking_number in ('0583753552');

update Shipment
set date_shipped = TO_DATE('02-JUL-13')
where tracking_number in ('0540264231');

update Shipment
set date_shipped = TO_DATE('03-JUL-13')
where tracking_number in ('0508373577', '0533655024', '0554574963', '0577424232', '0545605132', '0557773632', '0503923764');

update Shipment
set date_shipped = TO_DATE('04-JUL-13')
where tracking_number in ('0531394956', '0549716635', '0516333962', '0523177638', '0559803459');

select to_date('05-JUL-13')
from dual;

commit;

select s.date_shipped
from shipment
where id in ();

-- FECHA - ID
-- 20-JUN-13 0556373636 (47)
-- 27-JUN-13 0522163561
-- 30-JUN-13 0583753552
-- 02-JUL-13 0540264231
-- 03-JUL-13 0508373577, 0533655024, 0554574963, 0577424232, 0545605132, 0557773632, 0503923764
-- 04-JUL-13 0531394956, 0549716635, 0516333962, 0523177638, 0559803459, 


Select id, date_created, date_handling, date_ready_to_ship, date_shipped, date_delivered, date_cancelled, date_shipped_processed, receiver_id, sender_id, service_id, status, tracking_number, order_id, date_first_printed
from shipment
where id in (20693334432,20685680264,20685680501,20684914522,20685681653);--,20689134675,20689158970);

select date_created, date_shipped, sender_id, status
from shp_w01.shipment
where service_id = 73
and sender_id in (138352574, 17217497)
and status in ('shipped', 'delivered');
/*
20685679830
20685680264
20685680501
20684914522
20685681653
/*
Select a.state_id
from shipment s, shipping_address a
where s.receiver_address = a.address_id and s.date_created > sysdate-2 and a.state_id like '%-%';
Select *
from shipping_address
where state_id not like '%-%';
Select *
from zipcode;


Select *
from adoption
where user_id = 68280533;

Select *
from shipment
where service_id = 73 and status in ('delivered')
order by date_created desc;

Select *
from shipping_service
where id = 73;

Select *
from all_tables
where owner = 'SHP_W01' and table_name like '%CITI%';

Select *
from zipcode_brasilian
where zipcode_zip_code = 2348080;

Select *
from zipcode
where zip_code = 2348080;

Select *
from shipment_cities;
where cep = 2348080;

select *
from tracking_status
where normalized_status like 'ready_to_ship' and service_id = 73;


select *
from tracking_status
where service_id = 73;

select *
from shipping_methods;

select *--code, description
from carrier_tracking_notification
where application_id = 4153630522742212
--group by code, description
order by to_number(code);