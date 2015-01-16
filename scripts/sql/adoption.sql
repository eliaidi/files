--FERTEST_MLA
select *
from shp_w01.adoption
where user_id = 138398894
union
--FERTEST_MLB
select *
from shp_w01.adoption
where user_id = 140443460
union
--FERTEST_RJ_MLB
select *
from shp_w01.adoption
where user_id = 145180079;


select *
from shp_w01.shipping_preference
where user_id in (138398894,140443460,145180079);


select *
from shp_w01.shipping_methods;

select sender_id, service_id, item_id
from shp_w01.shipment
where date_created > sysdate - 1 and service_id = 22 and override_service_id is null;




/******************************************************************************************************/




select distinct status
from shp_w01.adoption;

select user_id, count(*)
from shp_w01.adoption_log
group by user_id
having count(*) > 2
order by 2 asc;

select *
from shp_w01.shipping_preference
where user_id = 140443460;

select *
from shp_w01.adoption
where user_id = 140443460;

select l.*
from shp_w01.adoption_log l
where user_id = 140443460 and comments like 'MarkItemsAdoptionJob'
order by date_created;

select *
from shp_w01.adoption
where user_id in 138398894;

select l.*
from shp_w01.adoption_log l
where user_id = 138398894 and comments like 'MarkItemsAdoptionJob'
order by date_created;


select to_char(a.date_created, 'DD/MON/YY-HH:MI:SS') as F, a.*
from shp_w01.adoption a
where last_updated like sysdate and status = 'pending'
order by 1 desc;

select *
from shp_w01.adoption_log
where user_id in (select user_id from shp_w01.adoption where date_created like sysdate and status = 'pending')
order by user_id;




select l.*
from shp_w01.adoption_log l
where user_id = 140443460 --and comments like 'MarkItemsAdoptionJob'
order by date_created;
select * from adoption where user_id = 140443460;

select l.*
from shp_w01.adoption_log l
where user_id = 140443460 and comments like 'MarkItemsAdoptionJob'
order by date_created;

select l.*
from shp_w01.adoption_log l
where user_id = 138352574 and comments like 'MarkItemsAdoptionJob'
order by date_created;

select *
from shp_w01.adoption_log l
where comments like 'MarkItemsAdoptionJob' and local_pick_up is not null
order by date_created desc;

select *
from all_indexes
where table_name like '%ADOPTION%LOG%';



select *
from shp_w01.adoption
where user_id = 140443460;

update shp_w01.adoption
set shipping_option = 'trial'
where user_id = 140443460;
commit;

update shp_w01.adoption
set shipping_option = 'trial', status='pending'
where user_id = 140443460;