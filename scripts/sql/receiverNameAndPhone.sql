select *
from shipment
where service_id = 73 and date_created < sysdate-30;

select *
from shipment
where service_id = 73 and sender_id = 138352574
order by date_created desc;

select *
from shipping_address
where id in ('21366263618','21365350324','21383868756','21383466270','21378646268','21375096455','21368184294','21375056338','21368126874','21368124930','21368124524','21368123786','21369463704','21366806575','21366806540','21367669899');

select *
from all_tab_columns
where table_name like 'SHIPPING_ADDRESS' and owner like 'SHP_W01'
order by column_id;

select *
from all_tab_columns
where table_name like 'SHIPMENT' and owner like 'SHP_W01'
order by column_id;

select distinct source
from shipment
where source is not null;


select *
from shipment
where contact is not null and date_created < sysdate-30;

select *
from shipment
where phone is not null and date_created < sysdate-30;

select *
from shipment
where contact is not null and date_created > sysdate-360;

select *
from shipment
where phone is not null and date_created > sysdate-180;

select *
from shipment
where area_code is not null;



select *
from custom_shipping
where contact is not null and date_created < sysdate-30;

select *
from custom_shipping
where phone is not null and date_created < sysdate-30;

select *
from custom_shipping
where contact is not null and date_created > sysdate-2 and site_id = 'MLA';

select *
from custom_shipping
where phone is not null and date_created > sysdate-2 and site_id = 'MLB';

select *
from custom_shipping
where area_code is not null;

select contact, phone, status,s.*
from shipment s
where contact is not null and date_created like sysdate;
where id = 20705585817;

select receiver_name, receiver_phone, status, s.*
from custom_shipping s
where id = 20705513389;




select sender_id, count(*)
from shipment
where date_created like sysdate
group by sender_id
having count(*) > 1;

select sysdate
from dual;
select * from custom_shipping where date_created > sysdate-1;

select *
from custom_shipping
where date_created like '15-AUG-13' and site_id = 'MLB' and other_info is not null;


select *
from all_tab_columns
where owner like 'SHP_W01' and table_name like 'CUSTOM_SHIPPING';

select *
from all_tab_columns
where owner like 'SHP_W01' and table_name like 'CUSTOM_SHIPPING' and data_type like 'VARCHAR2';

/*
alter table custom_shipping
add (
  receiver_name varchar2(255),
  receiver_phone varchar2(255)
);

commit;
*/
