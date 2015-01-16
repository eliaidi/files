select to_char(date_shipped, 'DD-MM-YY') as DIA, count(*) as AUTORIZADOS
from shp_w01.shipment
where service_id = 81
and site_id = 'MLA'
and date_shipped between to_date('01-11-13 00:00:00','DD-MM-YY HH24:MI:SS') and to_date('30-11-13 23:59:59','DD-MM-YY HH24:MI:SS')
and index_field not like '%test%' and index_field not like 'Test' and index_field not like 'TEST'
group by to_char(date_shipped, 'DD-MM-YY')
order by 1;


select COUNT(*)
from shp_w01.shipment
where service_id = 81
and site_id = 'MLA'
and date_shipped between to_date('01-11-13 00:00:00','DD-MM-YY HH24:MI:SS') and to_date('30-11-13 23:59:59','DD-MM-YY HH24:MI:SS')
and index_field not like '%test%' and index_field not like '%Test%' and index_field not like '%TEST%';


select to_char(date_shipped, 'DD-MM-YY') as DIA, count(*) as DESPACHADOS
from shp_w01.shipment
where service_id = 81
and site_id = 'MLA'
and ((date_shipped between to_date('01-11-13 00:00:00','DD-MM-YY HH24:MI:SS') and to_date('30-11-13 23:59:59','DD-MM-YY HH24:MI:SS')) 
or (date_shipped is null and date_delivered between to_date('01-11-13 00:00:00','DD-MM-YY HH24:MI:SS') and to_date('30-11-13 23:59:59','DD-MM-YY HH24:MI:SS')))
and index_field not like '%test%' and index_field not like '%Test%' and index_field not like '%TEST%'
group by to_char(date_shipped, 'DD-MM-YY')
order by 1;


select *
from shp_w01.shipment
where service_id = 81
and date_created > sysdate - 1;


select *
from all_indexes
where owner = 'SHP_W01' and table_name = 'SHIPMENT';

select table_owner,index_name,column_position pos,substr(column_name, 1, 30) column_name
from all_ind_columns
where table_name = upper('shipment')
order by table_owner, index_name, pos;

select * /*+ full(te) parallel(te,3) */ 
from shp_w01.tracking_events te 
where date_created > sysdate-12/24 and count_repeat > 50;

