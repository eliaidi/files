
import mercadoenvios.*

def shipment1 = Shipment.get(20678045884)
def shipment2 = Shipment.get(20675868111)
def shipment3 = Shipment.get(20675695189)
def shipment4 = Shipment.get(20675566768)

/*
shipment1.status='ready_to_ship'
shipment2.status='ready_to_ship'
shipment3.status='ready_to_ship'
shipment4.status='ready_to_ship'
 
shipment2.trackings.each{it.delete()}
shipment3.trackings.each{it.delete()}
shipment4.trackings.each{it.delete()}

shipment2.trackings = []
shipment3.trackings = []
shipment4.trackings = []

shipment2.save(flush:true)
shipment3.save(flush:true)
shipment4.save(flush:true)
*/

/*
def trackingNotifs = TrackingNotification.findAllByApplicationId(4153630522742212l)
trackingNotifs.each{println it.dump()}
println trackingNotifs.size()
*/


println "ship1"
println shipment1.status
println shipment1.trackingNumber
        shipment1.trackings.each{println "$it.event_date $it.tracking_status $it.tracking_description"}
println "ship2"
println shipment2.status
println shipment2.trackingNumber
        shipment2.trackings.each{println "$it.event_date $it.tracking_status $it.tracking_description"}
println "ship3"
println shipment3.status
println shipment3.trackingNumber
        shipment3.trackings.each{println "$it.event_date $it.tracking_status $it.tracking_description"}
println "ship4"
println shipment4.status
println shipment4.trackingNumber
        shipment4.trackings.each{println "$it.event_date $it.tracking_status $it.tracking_description"}


/*
shipment1.serviceId=73
shipment1.overrideServiceId=null
shipment1.trackingNumber='1234567892'
shipment1.save(flush:true)
shipment2.serviceId=73
shipment2.overrideServiceId=null
shipment2.trackingNumber='1234567893'
shipment2.save(flush:true)
shipment3.serviceId=73
shipment3.overrideServiceId=null
shipment3.trackingNumber='1234567894'
shipment3.save(flush:true)
shipment4.serviceId=73
shipment4.overrideServiceId=null
shipment4.trackingNumber='1234567895'
shipment4.save(flush:true)

println "ship1"
println shipment1.dump()
println "ship2"
println shipment2.dump()
println "ship3"
println shipment3.dump()
println "ship4"
println shipment4.dump()
*/
