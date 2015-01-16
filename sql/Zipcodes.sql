Select *
from all_indexes
where table_owner = 'SHP_W01' and table_name = 'ADDRESS_ATTRIBUTE';

Select *
from all_ind_columns
where index_name = 'PK_ADDRESS_ATTRIBUTE';

Select *
from SHP_W01.address_attribute
where address_id = '102102252';

Select *
from address_migration
where user_id = '16954333' and address_line like '%Rua%';

Select *
from shipping_address
where address_id = '50980460';

Select count(*)
from SHP_W01.zipcode_brasilian
where neighbourhood is not null
union
Select count(*)
from SHP_W01.zipcode_brasilian
where neighbourhood is null;



Select *
from SHP_W01.zipcode_brasilian
where zipcode_country_id != 'BR';
where neighbourhood is not null;

Select *
from zipcode_brasilian
where zipcode_zip_code = '48907455';


Select *
From all_tables
Where owner = 'SHP_W01' and table_name like '%ZIPCODE%';

Select *
From zipcode_migration--903053
where country_id != 'BR';

Select *
From zipcode;--904908

Select count(*) from (
Select distinct zipcode_zip_code
From zipcode_brasilian
where zipcode_country_id = 'BR'--971638 --Distinct: 912447
);

Select count(*) from (
Select distinct zipcode_zip_code, rownum rn
From zipcode_brasilian
Where zipcode_country_id = 'BR'
);
        
        
Select zipcode_zip_code from (
Select distinct zipcode_zip_code,rownum rn
From zipcode_brasilian
Where zipcode_country_id = 'BR'
) where rn >= 750001;

-- Total 971.638


Select * from zipcode;
Select * from zipcode_brasilian;
 
Select *
from all_columns
where owner = 'SHP_W01' and table_name = 'ZIPCODE_BRASILIAN';