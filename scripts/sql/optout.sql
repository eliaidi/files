SELECT *
FROM SHP_W01.ADOPTION
WHERE USER_ID IN ( 118920168, 80853195 );

select * from shp_w01.adoption a, orange.customers@prod_replication c
where a.user_id = c.cust_id
and c.site_id = 'MLA'
and internal_type is null
and a.date_created > to_date('14/03/2013','dd/mm/yyyy');