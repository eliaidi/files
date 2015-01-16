select count(*) from (
select zip_code
from zipcode_migration
group by zip_code
having count(*) = 1
);

select count(*)
from dimension_category;

select *
from zipcode_migration
where zip_code = '78130392';

select *
from zipcode_migration
where zip_code = '27170000';

select sysdate
from dual;

select *
from loc_location_tree
where name like '%Rincão%';

SELECT * FROM ZIPCODE_MIGRATION;
SELECT * FROM ZIPCODE_BRASILIAN;

select lm.id/*lm.*, t.name*/ from shipping_location_migration lm inner join loc_location_tree t on (lm.city = t.id) 
where country = 'BR'and city is not null
and not exists
(
  select 1 from shipping_location 
  where city = lm.city
  and state = lm.state
  and country = lm.country
);


select *
from shipping_location;
select *
from shipping_location_migration;
select *
from rule_migration;
select *
from rules;


select count(*) from (
Select rm.shipping_from_id, rm.shipping_to_id, count(*)
from rule_migration rm inner join shipping_location_migration lm on (rm.shipping_from_id = lm.id or rm.shipping_to_id = lm.id)
where country = 'BR'and city is not null
and not exists
(
  select 1 from shipping_location 
  where city = lm.city
  and state = lm.state
  and country = lm.country
)
group by rm.shipping_from_id, rm.shipping_to_id
having count(*) = 16
);

select count(*) from (
select rm.shipping_from_id, rm.shipping_to_id, count(*)
from shipping_location_migration lm inner join rule_migration rm on (lm.id = rm.shipping_from_id or lm.id = rm.shipping_to_id)
where lm.id not in (select l.id from shipping_location l)
group by rm.shipping_from_id, rm.shipping_to_id
having count(*) = 16--;
);

select count(*) from (
select rm.shipping_from_id, rm.shipping_to_id, count(*)
from shipping_location_migration lm inner join rule_migration rm on (lm.id = rm.shipping_to_id)
where lm.id not in (select l.id from shipping_location l)
group by rm.shipping_from_id, rm.shipping_to_id
having count(*) = 16--;
);

select * from shipping_location_migration;
select * from rule_migration;
select * from rule_migration where shipping_from_id = 19351071 and shipping_to_id = 19393072;
select * from shipping_service where shipping_method_id = 182;

select rm.shipping_from_id, rm.shipping_to_id
from shipping_location_migration lm inner join rule_migration rm on (lm.id = rm.shipping_from_id or lm.id = rm.shipping_to_id)
where lm.id not in (select l.id from shipping_location l)
group by rm.shipping_from_id, rm.shipping_to_id
having count(*) = 16;


Select rm.shipping_from_id, rm.shipping_to_id, lm.state, count(*)
from rule_migration rm inner join shipping_location_migration lm on (rm.shipping_to_id = lm.id)
where country = 'BR'and city is not null
and not exists
(
  select 1 from shipping_location 
  where city = lm.city
  and state = lm.state
  and country = lm.country
)
group by rm.shipping_from_id, rm.shipping_to_id, lm.state
having count(*) <> 16
order by rm.shipping_from_id, rm.shipping_to_id;


select * FROM ZIPCODE_MIGRATION WHERE ZIP_CODE = '79117000' AND COUNTRY_ID = 'BR';

INSERT INTO ZIPCODE_MIGRATION 
(ZIP_CODE, COUNTRY_ID, CITY_ID, STATE_ID, DATE_CREATED, COUNTRY_NAME, CITY_NAME, CITY_TYPE, LAST_UPDATED, VERSION, ADDRESS, STATE_NAME, ZIP_CODE_TYPE_ID) 
VALUES 
('65917334', 'BR', 'TUxCQ0lNUDkxNDdk', 'BR-MA', TO_DATE(07-Aug-13), 'Brasil', 'Imperatriz', 'CI', TO_DATE(07-Aug-13), 0, 'Maranh?o do Sul', 'MARANH?O', 772);

INSERT INTO ZIPCODE_MIGRATION 
(ZIP_CODE, COUNTRY_ID, CITY_ID, STATE_ID, DATE_CREATED, COUNTRY_NAME, CITY_NAME, CITY_TYPE, LAST_UPDATED, VERSION, ADDRESS, STATE_NAME, ZIP_CODE_TYPE_ID) 
VALUES 
('57048352', 'BR', 'TUxCQ01BQzFiOTU0', 'BR-AL', TO_DATE('07-Aug-13'), 'Brasil', 'Maceió', 'CI', TO_DATE('07-Aug-13'), 0, 'Projetada B', 'ALAGOAS', 772);

SELECT ZIP_CODE, COUNTRY_ID, CITY_ID, STATE_ID, DATE_CREATED, COUNTRY_NAME, CITY_NAME, CITY_TYPE, LAST_UPDATED, VERSION, ADDRESS, STATE_NAME, ZIP_CODE_TYPE_ID
FROM ZIPCODE_MIGRATION;

SELECT TO_DATE('07-Aug-13')
FROM DUAL;


UPDATE ZIPCODE_MIGRATION 
SET ZIP_CODE = '13060368', CITY_ID = 'BR-SP-42', STATE_ID = 'BR-SP', CITY_NAME = 'Campinas', CITY_TYPE = 'CI', LAST_UPDATED = TO_DATE('07-Aug-13'), VERSION = VERSION + 1, ADDRESS = 'Oséas Cappelette', STATE_NAME = 'SÃO PAULO', ZIP_CODE_TYPE_ID = 772 
WHERE ZIP_CODE = 13060368;

select ZIP_CODE , CITY_ID, STATE_ID , CITY_NAME, CITY_TYPE, LAST_UPDATED, VERSION, ADDRESS, STATE_NAME, ZIP_CODE_TYPE_ID 
from ZIPCODE_MIGRATION
where ZIP_CODE = 13060368;

Select *
from loc_location_tree
where name like '%Miraporanga%';
Select *
from zipcode_migration
where zip_code = '12247004';