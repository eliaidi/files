select id, sender_id, item_id, category_id, service_id, status, index_field, date_ready_to_ship, tracking_Number, date_created
from shp_w01.shipment
where id in (21005041112,21005062400,21004956981,21005017162,21004870991,21004950818,21004962625,21005004726,21005024094,21005084279);
order by date_created;

21005024094 es me1 -> ventas privadas... ok ???

select *
from shp_w01.adoption_log
where user_id = 59359887
order by date_created desc;

select *
from shp_w01.shipment
where date_created > to_date('10-02-14 14:15','dd-MM-YY HH24:MI') --Sender_id must be in (92607234,75338949,36484980,9877009,4542417,85313895,82916233,139060460)
and category_id in (
'MLA123859','MLA123860',
'MLA123861','MLA123862',
'MLA123863','MLA123864',
'MLA123865','MLA123866',
'MLA123867','MLA125067',
'MLA124821','MLA124839',
'MLA124857','MLA124875',
'MLA124893','MLA124911',
'MLA124929','MLA124947',
'MLA124965','MLA124983',
'MLA10557','MLA10561',
'MLA10570','MLA14871',
'MLA14874','MLA14885',
'MLA14904','MLA14936',
'MLA14937','MLA78918',
'MLA81532','MLA81533',
'MLA81534','MLA81535',
'MLA81536','MLA81537',
'MLA81538','MLA81539',
'MLA81990','MLA82030',
'MLA11899','MLA11902',
'MLA14940','MLA14941',
'MLA14942','MLA78913'
);


select *
from shp_w01.dimension_category
where category_id in (
'MLA123859','MLA123860',
'MLA123861','MLA123862',
'MLA123863','MLA123864',
'MLA123865','MLA123866',
'MLA123867','MLA125067',
'MLA124821','MLA124839',
'MLA124857','MLA124875',
'MLA124893','MLA124911',
'MLA124929','MLA124947',
'MLA124965','MLA124983',
'MLA10557','MLA10561',
'MLA10570','MLA14871',
'MLA14874','MLA14885',
'MLA14904','MLA14936',
'MLA14937','MLA78918',
'MLA81532','MLA81533',
'MLA81534','MLA81535',
'MLA81536','MLA81537',
'MLA81538','MLA81539',
'MLA81990','MLA82030',
'MLA11899','MLA11902',
'MLA14940','MLA14941',
'MLA14942','MLA78913'
);

select *
from shp_w01.dimension_category
where category_id = 'MLA124124';