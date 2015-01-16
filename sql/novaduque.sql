select r.amount_charged, r.amount_receivable, s.*
from shp_w01.shipment s,shp_w01.real_shipment_data r
where s.id = r.shipment_id 
and s.date_shipped > sysdate - 2
and s.site_id = 'MLB'
and s.status like 'not_delivered';

select *
from shp_w01.real_shipment_data;



select  rem.*,s.id, s.date_shipped, s.tracking_number, 
        rsd.weight, rsd.amount_charged, 
        rem.zip_code, rem.street_name, rem.street_number, rem.city_name, rem.state_id,
        s.contact,
        dest.zip_code, dest.street_name, dest.street_number, dest.city_name, dest.state_id,
        s.phone
from shp_w01.shipment s, shp_w01.shipping_address rem, shp_w01.shipping_address dest, shp_w01.real_shipment_data rsd
where s.receiver_address = dest.id and s.sender_address = rem.id and s.id = rsd.shipment_id
and s.id = 21003975742;

select phone
from shp_w01.shipment
where date_created > sysdate - 1
and site_id = 'MLB';