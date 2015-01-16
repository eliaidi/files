select s.id, s.status, s.date_handling, s.last_processed_order, s.date_created, s.order_id, s.site_id, s.*
from shp_w01.shipment s
where s.date_handling > sysdate - ((1/24/60)*10)
order by 3 desc;

select s.id, s.status, s.date_handling, s.date_created, s.order_id, s.site_id, s.*
from shp_w01.shipment s
where s.order_id = '789592810';


select s.id, s.status, s.date_handling, s.date_created, s.order_id, s.site_id, s.*
from shp_w01.shipment s
where s.order_id in ('789322677','786115380');


select s.id, s.status, s.date_handling, s.date_created, s.order_id, s.site_id, s.*
from shp_w01.shipment s
where s.order_id in ('791113507','791390282','791389182');

-- 786115380 . El problema con esta order es que se genero un envio despues de que se habia pagado la order. Posible error en checkout.


select max(s.handling_time), s.item_id, a.zip_code, se.zip_code--s.id, s.status, s.date_handling, s.date_created, s.order_id, s.site_id, s.handling_time, s.item_id, a.zip_code, s.*
from shp_w01.shipment s, shp_w01.shipping_address a, shp_w01.shipping_address se
where s.date_created > sysdate - (1/24/60)*25 and s.receiver_address = a.id and s.handling_time > 48 and s.sender_address = se.id
group by s.item_id, a.zip_code, se.zip_code
order by 1 desc;

select s.id, s.status, s.date_handling, s.date_created, s.order_id, s.site_id, s.handling_time, s.item_id, s.*
from shp_w01.shipment s
where s.date_created > sysdate - (1/24/60)*20 and s.handling_time > 48;

select *
from shp_w01.shipment s
where s.id = 20707675134;--661019278
select *
from shp_w01.shipping_costs
where order_id = '788077498';

--('788077498','788101512','786968787','788107660','788112018','787316897','788113004');
select *
from shp_w01.shipment
where order_id in ('787316897','788077498','788101512');
select *
from shp_w01.shipping_costs
where order_id in ('787316897','788077498','788101512');







SELECT * FROM SHP_W01.SHIPMENT WHERE DATE_CREATED > SYSDATE - 1;

SELECT S.ORDER_ID, S.SENDER_ID, S.ID, S.SITE_ID, S.SERVICE_ID, S.STATUS, S.DATE_CREATED, S.LAST_UPDATED, S.DATE_HANDLING, S.LAST_PROCESSED_ORDER, S.COST, S.REAL_COST
FROM SHP_W01.SHIPMENT S
WHERE S.DATE_HANDLING > sysdate - 10
AND S.DATE_HANDLING IS NOT NULL
AND S.STATUS not like 'pending'
AND S.SHIPPING_MODE = 'me2'
AND NOT EXISTS(
  SELECT 1 FROM SHP_W01.SHIPPING_COSTS C WHERE C.ORDER_ID = S.ORDER_ID
);



select *
from shp_w01.shipping_costs
where order_id in ('791077352','791079903','791079957','791080015','791078628','791080387','791081305','791081386','791082007');

select *
from shp_w01.shipment
where order_id in ('791080387');

select *
from shp_w01.shipment
where order_id in ('790448619');
select *
from shp_w01.shipping_costs
where order_id in ('790448619');



SELECT *
FROM SHP_W01.SHIPPING_COSTS
WHERE ORDER_ID = '789322677';
SELECT *
FROM SHP_W01.SHIPMENT
WHERE ORDER_ID = '789322677';

select count(*)
from shp_w01.shipment
where date_created > date_handling and date_created > sysdate - 30 and date_ready_to_ship is not null;

select count(*)
from shp_w01.shipment
where date_ready_to_ship is not null and date_created > sysdate - 30;



select *
from shp_w01.shipment
where order_id in ('790915138','790662428','790228111','791037413','791001227','790737537','790891494','790915637','790764399','790888782','790678731','790976925','790805361','791086677','790908139','790894846','790276897','790718569','790774165','791086509','790736445','790974540','790915644')
and status = 'pending';

/*
Fallo llamada a la API
790678731
790718569
790774165
790736445
790805361
--
790974540
790894846
790908139
791001227
790888782
790915138
790915637
--
41938969
64174603


No encontro el envio ( no se por que, en zeus figura bien ) VER MAS LOGS
--
790976925
790891494
790915644
790737537
790764399
--

Error no existe regla
--
791086509
791086677
--
*/

select *
from shp_w01.shipment
where order_id in('791119196','791304694','790914034','791173656','791119394','791303004','791187438');


/* 
OrderIds con novedades anteriores al shipment
790024123
789016561


OrderIds con fecha anterior(igual) a la ya procesada
789965059

OrderIds que no se emitieron las novedades (todavia) pero que la api de orders devuelve paid (ni en zeus aparece el pago)
         => el feed de payments esta atrasado
790276897
*/


select *
from shp_w01.shipment
where date_handling > sysdate - (1/24/60)*10
order by date_handling desc;

select *
from shp_w01.shipment
where date_created > sysdate - 1 and shipping_mode = 'me1' and date_handling > sysdate - (1/24)*1
order by date_handling desc;

select *
from shp_w01.custom_shipping
where date_created > sysdate - 30 and site_id = 'MLB';
