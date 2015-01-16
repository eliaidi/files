Select distinct normalized_status from SHP_W01.Tracking_status;
--Tracking status de Correios
Select distinct c.name from SHP_W01.shipping_methods m inner join SHP_W01.Companies c on (m.company_id = c.id)
where m.id in ( Select distinct t.shipping_method_id from SHP_W01.Tracking_Status t );


Select * from SHP_W01.Tracking_status;


Select * from SHP_W01.Shipment where service_id is not null;

select * from SHP_W01.Companies;

select * from SHP_W01.Shipping_service;
select * from SHP_W01.Shipping_methods;
select /*count(*), t.shipping_method_id,*/t.*, m.description 
from SHP_W01.Tracking_status t inner join SHP_W01.Shipping_methods m on (t.shipping_method_id = m.id) ;
--group by t.shipping_method_id, m.description;

select count(*), t.normalized_status, t.shipping_method_id
from SHP_W01.Tracking_status t inner join SHP_W01.Shipping_methods m on (t.shipping_method_id = m.id)
group by t.normalized_status, t.shipping_method_id
order by t.shipping_method_id;

select t.normalized_status
from SHP_W01.Tracking_status t
group by t.normalized_status;

select * from SHP_W01.Shipping_mode;

--select utl_url.escape('http://www.acme.com/a url with space.html') from dual;

--"http://websro.correios.com.br/sro_bin/txect01\$.QueryList?P_LINGUA=001&P_TIPO=001&P_COD_UNI=#{trackingNumber}"

--select utl_url.unescape(select utl_url.escape('http://www.acme.com/a url with space.html') from dual) from dual;

update SHP_W01.Shipping_service set tracking_url = "http://www.jadlog.com.br/tracking.jsp?cte=#{trackingNumber}" where company_id = 17500340;

select * from SHP_W01.Shipping_service;

