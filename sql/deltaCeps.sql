select *
from zipcode_migration
where neighborhood is null and country_id = 'BR';

select count(*)
from zipcode_migration
where country_id in ('BR','AR');

select *
from zipcode_migration
where country_id = 'AR';

select *
from zipcode_brasilian;

select count(*)
from zipcode_migration
where country_id = 'BR';
Select count(*)
from zipcode_brasilian
where zipcode_country_id = 'BR';

select count(*)
from zipcode_migration
where neighborhood is null and country_id = 'BR';--15379 => 12879 .. (2500 actualizados)

select count(*)
from zipcode_brasilian
where neighbourhood is null and zipcode_country_id = 'BR';--66760 => 64261 .. (2499 actualizados)

select *
from zipcode_migration mig
where mig.neighborhood is null and mig.country_id = 'BR' and not exists
(
  select * from zipcode_brasilian bra where bra.zipcode_zip_code = mig.zip_code and bra.zipcode_country_id = mig.country_id and bra.neighbourhood is null
);--15378 (Hay uno que esta actualizado en brasilian y no en migration, antes de correr job)

select neighbourhood from zipcode_brasilian where zipcode_zip_code = '3005030'
union
select neighborhood from zipcode_migration where zip_code = '3005030';


select *
from loc_location_tree
where id = 'QlItTUdQaW5oZWlybyBkZSBNaW5hcw';
--where type = 'CITY' and name like '%Pinheiro de%';

select *
from loc_location_tree
where id = 'QlItUlNSaW5ji28gZGEgQ3J1eg';
--where type = 'CITY' and name like '%Rinc‹o%Cruz%';


/*
Pinheiro de Minas,MG,MINAS GERAIS - QlItTUdQaW5oZWlybyBkZSBNaW5hcw
Rinc‹o da Cruz,RS,RIO GRANDE DO SUL - QlItUlNSaW5ji28gZGEgQ3J1eg
*/


select *
from zipcode_migration
where city_id in ('QlItTUdQaW5oZWlybyBkZSBNaW5hcw','QlItUlNSaW5ji28gZGEgQ3J1eg');

select *
from zipcode_brasilian;


select *
from shipment
where other_info is not null;
select *
from shipping_address
where other_info is not null;