select date_shipped - date_ready_to_ship, date_shipped, date_ready_to_ship, site_id
--select sender_id, site_id
from shp_w01.shipment
where sender_id in (76147858)
order by date_shipped;
--group by sender_id, site_id;
--order by date_created desc;
/*
133868825:10  ok
37990562:13   ok
70325633:13   mmm
59181149:10   este no esta bien, tiene 2 envios con 10 que no deberian haber formado parte de la cuenta
125290459:10  mmm
15867992:14   no, deberia haber funcionado el p80 y tener un ht mas bajo
134042381:11  ok
102813556:10  mas ok que mal
79014643:13   no, deberia haber funcionado p80
72028998:10   no, deberia funcionar p80
98814106:11   no, idem anterior
76147858:17   no, idem anterior
85017962:11   no, idem anterior
*/

select s.handling_time, s.sender_id, s.item_id, to_char(s.date_created, 'DD-MON-YYYY HH:MI:SS'), s.*
from shp_w01.shipment s
where trunc(date_created) = trunc(sysdate) and site_id = 'MLB' and handling_time <> 48 --and s.handling_time > 192
order by date_created desc;

select s.handling_time, s.sender_id, s.item_id, to_char(s.date_created, 'DD-MON-YYYY HH:MI:SS') , s.*
from shp_w01.shipment s
where trunc(date_created) = trunc(sysdate) and site_id = 'MLA' --and s.handling_time > 192 --and service_id = 81
order by date_created desc;






/******************************************************************************/

select count(*) from (select sender_id
from shp_w01.shipment
where site_id = 'MLB' and date_created > sysdate - 21
group by sender_id);

select count(*) from (select sender_id
from shp_w01.shipment
where site_id = 'MLA' and date_created > sysdate - 15
group by sender_id);


select count(*) from (
select /*+ index(s IDX_SHIPPING_DT_CREATED_STATUS) */ sender_id, count(*) cnt
from shp_w01.shipment s
where shipping_mode = 'me2'
and date_created > sysdate - 30
and status in ('ready_to_ship', 'shipped', 'delivered', 'not_delivered')
and site_id = 'MLB'
group by sender_id
having count(*) > 5
--order by cnt desc
);

select count(*) from (
select /*+ index(s IDX_SHIPPING_DT_CREATED_STATUS) */ sender_id, count(*) cnt
from shp_w01.shipment s
where shipping_mode = 'me2'
and date_created > sysdate - 30
and status in ('ready_to_ship', 'shipped', 'delivered', 'not_delivered')
and site_id = 'MLA'
group by sender_id
having count(*) > 5
--order by cnt desc
);

/*
133868825:10
37990562:13
70325633:13
59181149:10
125290459:10
15867992:14
134042381:11
102813556:10
79014643:13
72028998:10
98814106:11
76147858:17
85017962:11
*/