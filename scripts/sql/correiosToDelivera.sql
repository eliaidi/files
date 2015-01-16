--Back up
Select *
from shipment
where sender_id = 912473 /*and status = 'ready_to_ship'*/ and service_id <> 73 and date_created >= TO_DATE('20-JUN-2013')
order by date_created desc;

Select *
from tracking_number
where shipping_id in (Select id from shipment where sender_id = 912473 and status = 'ready_to_ship' and service_id <> 73 and date_created >= TO_DATE('20-JUN-2013'))
order by date_created desc;


Select id from shipment where sender_id = 912473 and status = 'ready_to_ship' and service_id <> 73 and date_created >= TO_DATE('20-JUN-2013');
--Updates
Delete from tracking_number
where shipping_id in (Select id from shipment where sender_id = 912473 and status = 'ready_to_ship' and service_id <> 73 and date_created >= TO_DATE('20-JUN-2013'));

update shipment
set service_id = 73, shipping_method_id = 500645, date_ready_to_ship = null, status = 'handling', date_first_printed = null
where id in (Select id from shipment where sender_id = 912473 and status = 'ready_to_ship' and service_id <> 73 and date_created >= TO_DATE('20-JUN-2013')); 

--Check
Select id, date_created, date_handling, date_ready_to_ship, date_shipped, date_delivered, date_cancelled, date_shipped_processed, receiver_id, sender_id, service_id, shipping_method_id ,status, tracking_number, order_id, date_first_printed, receiver_address
from shipment
where sender_id = 912473 and status = 'ready_to_ship' and service_id <> 73 and date_created >= TO_DATE('20-JUN-2013')
order by date_created desc;

Select *
from tracking_number
where shipping_id in (Select id from shipment where sender_id = 912473 and status = 'ready_to_ship' and service_id <> 73 and date_created >= TO_DATE('20-JUN-2013'))
order by date_created desc;

commit;

Select id, date_created, date_handling, date_ready_to_ship, date_shipped, date_delivered, date_cancelled, date_shipped_processed, receiver_id, sender_id, service_id, shipping_method_id ,status, tracking_number, order_id, date_first_printed, receiver_address
from shipment
where sender_id = 912473 and service_id = 73 and status = 'ready_to_ship'
order by date_created desc;









Select *
from shipment
where id in (20696775244,20696347870,20695017167,20693573676);



