
Select count(*), s.sender_id--, s.id 
from SHP_W01.Shipment s, SHP_W01.Tracking_Number t
where s.date_created > '01-JAN-13' and t.shipping_id = s.id and t.tracking_code is not null and s.service_id in (51,52)
group by s.sender_id
having count(*) > 3
order by s.sender_id;--, s.id; --635820
-- OCA = 17500240

-- shipments para probar impresion de etiquetas del sender 635820 (Correios)
Select s.id, s.service_id
from SHP_W01.Shipment s, SHP_W01.Tracking_Number t
where s.date_created > '20-FEB-13' and t.shipping_id = s.id and t.tracking_code is not null and s.sender_id = 635820;

-- shipments para probar impresion de etiquetas del sender 78568223 (OCA)
Select s.id, s.service_id
from SHP_W01.Shipment s, SHP_W01.Tracking_Number t
where s.date_created > '01-FEB-13' and t.shipping_id = s.id and t.tracking_code is not null and s.sender_id = 78568223;

-- shipments para probar impresion de etiquetas del sender 130345625 (OCA)
Select s.id, s.service_id
from SHP_W01.Shipment s, SHP_W01.Tracking_Number t
where s.date_created > '01-FEB-13' and t.shipping_id = s.id and t.tracking_code is not null and s.sender_id = 130345625 and s.service_id is not null;



Select s.id, s.service_id, s.sender_id
from SHP_W01.Shipment s, SHP_W01.Tracking_Number t
where s.date_created > '01-JAN-13' and t.shipping_id = s.id and t.tracking_code is not null and s.service_id in (51,52);

Select s.id, c.* from shp_w01.shipping_service s, shp_w01.companies c where s.company_id = c.id;

