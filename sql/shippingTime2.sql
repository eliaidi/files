select s.date_shipped, s.date_delivered, s.date_delivered - s.date_shipped, s.*
from shp_w01.shipment s
where s.date_delivered > sysdate - 90
and s.date_shipped is not null
and s.site_id = 'MLB'
and s.applied_shipping_rule_id = 100109665
order by s.date_delivered desc;


select s.applied_shipping_rule_id, count(*)
from shp_w01.shipment s
where s.date_delivered > sysdate - 90
and s.date_shipped is not null
and s.site_id = 'MLB'
and s.service_id in (21,22)
and to_char(s.date_shipped, 'DD-MM-YYYY') = to_char(s.date_delivered, 'DD-MM-YYYY')
group by s.applied_shipping_rule_id
order by 2 desc;

select s.service_id, count(*)
from shp_w01.shipment s
where s.date_delivered > sysdate - 90
and s.date_shipped is not null
and s.site_id = 'MLB'
and s.service_id in (21,22)
and to_char(s.date_shipped, 'DD-MM-YYYY') = to_char(s.date_delivered, 'DD-MM-YYYY')
group by s.service_id
order by 2 desc;

-- La mayoria de los envios con ST 0 de OCA son para los servicios con colecta


select s.applied_shipping_rule_id, s.date_shipped, s.date_delivered
from shp_w01.shipment s
where s.date_delivered > sysdate - 90
and s.date_shipped is not null
and s.site_id = 'MLB'
and s.service_id in (21,22)
and to_char(s.date_shipped, 'DD-MM-YYYY') = to_char(s.date_delivered, 'DD-MM-YYYY')
and to_char(s.date_shipped, 'HH24') > '08'
and s.applied_shipping_rule_id in (27906757,27569658,27569657,27552023);

-- Los que dan 0 en Argentina tienen date_shipped con horario de madrugada (00 a 03 hs)

