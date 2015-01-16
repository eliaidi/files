import mercadoenvios.*
  
def sa = Shipment.findByOrderId('915789509')
/*sa.status = 'delivered'
sa.substatus = null
sa.dateShipped = new Date()
sa.dateDelivered = new Date()*/
println "sa"  
println "st: " + sa.status
println "su: " + sa.substatus
println "dr: " + sa.dateReadyToShip
println "ds: " + sa.dateShipped
println "dd: " + sa.dateDelivered
println "dn: " + sa.dateNotDelivered


def sb = Shipment.findByOrderId('915791112')
/*sb.status = 'delivered'
sb.substatus = null
sb.dateShipped = new Date()
sb.dateDelivered = new Date()*/
println "sb"
println "st: " + sb.status
println "su: " + sb.substatus
println "dr: " + sb.dateReadyToShip
println "ds: " + sb.dateShipped
println "dd: " + sb.dateDelivered
println "dn: " + sb.dateNotDelivered


deliveredMailProcessorService = ctx.getBean('deliveredMailProcessorService')
deliveredStaleMailProcessorService = ctx.getBean('deliveredStaleMailProcessorService')
mongoLocksService = ctx.getBean('mongoLocksService')

def messageSa = [  shipment_id: sa.id,  service_id: sa.serviceId,  shipment_status: sa.status,
  substatus: sa.substatus,  tracking_number: sa.trackingNumber,  mode: sa.shippingMode
]
def messageSb = [  shipment_id: sb.id,  service_id: sb.serviceId,  shipment_status: sb.status,
  substatus: sb.substatus,  tracking_number: sb.trackingNumber,  mode: sb.shippingMode
]

println "begin"
//mongoLocksService.remove('mails', [shipping_id: messageSb.shipment_id, type: 'delivered'])
//println deliveredMailProcessorService.validateMessage(messageSb)
//println deliveredMailProcessorService.validateMessage(messageSa)
println deliveredMailProcessorService.processMessage(messageSb)

//println deliveredStaleMailProcessorService.validateMessage(messageSb)
//println deliveredStaleMailProcessorService.processMessage(messageSb)
println "end"