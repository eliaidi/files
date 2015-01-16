Select id, date_created, date_handling, date_ready_to_ship, date_shipped, date_delivered, date_cancelled, date_shipped_processed, receiver_id, sender_id, service_id, shipping_method_id ,status, tracking_number, order_id, date_first_printed, receiver_address
from shipment
where sender_id = 912473 and service_id = 73 and status = 'ready_to_ship'
order by date_created desc;

Select id
from shipment
where sender_id = 912473 and service_id = 73 and status = 'ready_to_ship'
order by date_created desc;

select *
from shipping_service
where is_default = 1;



update shipping_service
set is_default = 0
where id in (21,22);

select *
from shipping_service
where id in (21,22);

commit;

select *
from shipping_preference
where shipping_service_id in (21,22);

