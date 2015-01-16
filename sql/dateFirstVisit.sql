Select date_first_visit
from shipment
where date_first_visit is not null;

select *
from all_tab_columns
where table_name = 'SHIPMENT' and owner like 'SHP_W01';


Select *
from tracking_events t, shipment t
where t.country_id = 'BR' and t.state_id = 'BR-SP' and t.;

select *
from tracking_events;
select date_first_visit
from shipment;


Select s.id,s.tracking_number, e.*
from shp_w01.tracking_events e, shp_w01.shipment_tracking_events se, shp_w01.shipment s
where s.id = se.shipment_trackings_id and se.tracking_events_id = e.id 
and s.id in (
  Select id
  from shipment
  where service_id in (1,2) and status in ('shipped','delivered','not_delivered') and date_created > sysdate-5
);

--CORREIOS

Select *
from tracking_status
where service_id in (1,2) and normalized_status in ('delivered','shipped')
order by service_id, normalized_status, description;

select *
from tracking_status
where status_id in ('BDI01','BDE01','BDR01','BDE02','BDR02','BDI02','BDR20','BDI20','BDI21','BDE21','BDE20')
order by service_id, normalized_status, description;

--Eventos a tener en cuenta para date_first_visit
--BDI01 BDE01 BDR01
--BDE02 BDR02 BDI02 BDR20 BDI20 BDI21 BDE21 BDE20

select * from carrier_tracking_notification;
select * from tracking_events;
select * from tracking_status where status_id = '3';

select distinct n.code, n.description, s.normalized_status
from carrier_tracking_notification n inner join tracking_status s on (n.code = s.status_id)
where application_id = 4153630522742212
order by code;

select distinct code ,description
from carrier_tracking_notification
where application_id = 3106651678217755
order by to_number(code);
--Motornorte: 20 delivered - 22 ausente

select distinct code ,description
from carrier_tracking_notification
where application_id = 3469987440161451
order by code;
--OCA : 15 delivered - 16 ausente

select distinct application_id
from carrier_tracking_notification;

--DELIVERA

Select *
from tracking_events
where tracking_status in ('1','2');


Select *
from tracking_status
where service_id = 73 and normalized_status in ('delivered','shipped')
order by service_id, normalized_status, description;

--Eventos a tener en cuenta para date_first_visit
--1 2


Select count(*)
from shipment
where date_first_visit is not null and service_id not in (1,2);--status not like 'delivered';
