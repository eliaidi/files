import mercadoenvios.*

Shipment shipment = Shipment.get(20675358696)
//println "status:" + shipment.status
//println "sa:" + shipment.senderAddress.addressLine

ShippingAddress senAdd = shipment.senderAddress
/*senAdd.addressLine = "Avenida Marte, 489"
senAdd.cityName = "Santana de Parnaiba"
senAdd.zipCode = "06541005"
senAdd.stateName = "São Paulo"
senAdd.stateId = "BR-SP"
senAdd.save(flush:true)
*/
println "senAdd: ${senAdd.dump()}"

ShippingAddress recAdd = shipment.receiverAddress
//recAdd.addressLine = "Rua Antonio Carneiro, 125"
recAdd.cityName = "São Paulo"
//recAdd.zipCode = "05780750"
//recAdd.stateName = "São Paulo"
//recAdd.stateId = "BR-SP"
recAdd.save(flush:true)

println "recAdd: ${recAdd.dump()}"

/*
TrackingNumber tn = new TrackingNumber()
tn.trackingNumber = "PH967321528BR"
tn.plpId = 1615619
tn.trackingCode = 96732152
tn.status = "active"
tn.dateCreated = new Date()
tn.lastUpdated = new Date()
tn.save(flush:true)


shipment.status = "ready_to_ship"
shipment.trackingNumber = "PH967321528BR"
shipment.save(flush:true)
*/