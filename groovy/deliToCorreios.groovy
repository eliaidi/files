import mercadoenvios.*

TrackingNumber tn = TrackingNumber.findByShippingId(20696762173)
tn.delete()

Shipment ship = Shipment.get(20696762173)
ship.serviceId = 2
ship.status = 'handling'
ship.shippingMethodId = 182
ship.dateHandling = null
ship.trackingNumber = null
ship.dateReadyToShip = null
ship.dateFirstPrinted = null
ship.save(flush:true)