
https://api.mercadolibre.com/items/MLB504319216/shipping_options?zip_code=85866900

curl -X POST -H "Content-Type: application/json" -d '{"order_items":[{"item_id":"MLB504319216","quantity":1}]}' "http://internal.mercadolibre.com/orders?caller.id=138352574&client.id=2222"

curl -X POST -H'Content-Type:application/json' -d '{"receiver_address":{"zip_code":"85866900","country":{"id":"BR"},"address_line":"Calle test fer"},"shipping_option":{"id":27566852}}' 'http://e-00001652:8080/orders/783585000/shipments?caller.id=138352574'

curl -X PUT -H "Content-Type: application/json" -d '{"receiver_address": {"receiver_name": "Camila Oliveira", "receiver_phone": "11-1234-1234"}}' 'http://e-00001652:8080/shipments/20705585817?caller.id=138352574'




https://api.mercadolibre.com/items/MLB504338223/shipping_options?zip_code=85866900

curl -X POST -H "Content-Type: application/json" -d '{"order_items":[{"item_id":"MLB504338223","quantity":1}]}' "http://internal.mercadolibre.com/orders?caller.id=138352574&client.id=2222"

curl -X POST -H'Content-Type:application/json' -d '{"receiver_address":{"zip_code":"85866900","country":{"id":"BR"},"address_line":"Calle test fer"},"shipping_option":{"id":'MLB504338223-1'}}' 'http://e-00001652:8080/orders/783575941/shipments?caller.id=138352574'

curl -X PUT -H "Content-Type: application/json" -d '{"receiver_address": {"receiver_name": "Camila Oliveira", "receiver_phone": "11-1234-1234"}}' 'http://e-00001652:8080/shipments/20705513389?caller.id=138352574'
