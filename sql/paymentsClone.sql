select t.description ,m.*
from mercadopago.movement m, MERCADOPAGO.type_movement t
where m.operation_id in (624374996) and m.move_type_id = t.move_type_id
order by m.operation_id, move_date desc;


select t.description ,m.*
from mercadopago.movement m, MERCADOPAGO.type_movement t
where m.operation_id in (624374996) and m.move_type_id = t.move_type_id
order by m.operation_id, move_date desc;

select *
from mercadopago.operation o
where o.operation_id in (624374996);

select t.description ,m.*
from mercadopago.movement m, MERCADOPAGO.type_movement t
where m.operation_id in (660834969,660834073,661019278) and m.move_type_id = t.move_type_id
order by m.operation_id, move_date desc;


/*
660834969 '788101512' -
654306190 '786968787'
660841058 '788107660'
660690296 '788112018'
660834073 '787316897' -
660692183 '788113004'
661019278 '788077498' -
*/

select *
from MERCADOPAGO.type_movement;
select *
from mercadopago.movement;


select *
from mercadopago.operation;
where site_id = 'MLB';


--Chequea que todos los free shipping. Se fija que lo que cobra el vendedor sea igual al monto original de la operacion (cuando no es free shipping, cobra el monto original + el costo del envio, que despues es descontado)
select o.operation_id, o.original_amount, mo.amount, mo.move_type_id, t.description
from mercadopago.operation o, mercadopago.movement mo, mercadopago.type_movement t
where o.operation_id = mo.operation_id and mo.move_type_id = t.move_type_id
and o.operation_date between (sysdate-(1/24)*16) and sysdate
and o.operation_type_id = 'PA' 
and (o.ship_cost_amt is null or o.ship_cost_amt = 0)
and o.original_amount <> mo.amount and mo.move_type_id = 'COPTC'
and exists (
  select 1 from mercadopago.movement m where m.operation_id = o.operation_id and m.move_type_id in ('SMLTC','SHML')
);

select sysdate-(1/24)*5 from dual;



