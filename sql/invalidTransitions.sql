select *
from shp_w01.tracking_status
where normalized_status = 'ready_to_ship'
and service_id between 21 and 23;

select *
from shp_w01.tracking_events e, shp_w01.shipment_tracking_events se, shp_w01.shipment s
where s.id = se.shipment_trackings_id and e.id = se.tracking_events_id
and s.date_created > sysdate -1 and s.service_id = 81;

select *
from shp_w01.shipment_log;

select s.id, s.date_handling, s.date_ready_to_ship, s.service_id, s.status, e.event_date, e.tracking_status--, l.ts, l.host
from shp_w01.tracking_events e, shp_w01.shipment_tracking_events se, shp_w01.shipment s--, shp_w01.shipment_log l
where s.id = se.shipment_trackings_id and e.id = se.tracking_events_id --and s.id = l.id
and s.date_created > sysdate - 1
and s.site_id = 'MLA'
and s.service_id between 61 and 64
and e.tracking_status in ('3','52','57','6')
and s.date_handling is not null
and s.status = 'pending'
--and l.ts > s.date_ready_to_ship
--order by s.id, l.ts
;



select *
from shp_w01.shipment
where date_created > sysdate - 7
and status in ('ready_to_ship','shipped','delivered','not_delivered')
and date_handling is null;