import mercadoenvios.* 
import mercadoenvios.constants.ShippingConstants
import groovyx.gpars.GParsPool
import org.joda.time.format.ISODateTimeFormat

out = new File('/tmp/ordersFix/ordersLog.txt'); out.write("")
ordersService = ctx.getBean("ordersService")
feedOrder = ctx.getBean("messageProcessorOrdersService")
paymentService = ctx.getBean('paymentService')
services = ['MLA': [51L,52L,61L,62L,63L,64L,81L], 'MLB': [1L,2L,21L,22L]]

DAYS_BEFORE = 31


synchronized void logFile(String text) {
	out << text + "\n"
}

def processOrderForShipmentId(shipmentId) {
	Shipment ship

	try{
		Shipment.withTransaction { tx -> 
			ship = Shipment.get(shipmentId)
	    	def order = ordersService.getOrdersData(ship.orderId, ship.senderId)

	    	if (order?.status == 'paid') {
				def payment = searchForApprovedPayment(order, ship.receiverId)
				
				if (payment && ship.cost <= payment.shipping_cost) {
					if (!isRecentlyPaid(payment)) {
						
						logFile("'${order.id}',${ship.senderId}L")
					
					} else {
						logFile "Payment has been recently approved. Order ${s.orderId} is ignored"
					}
				
				} else if (payment == null) {
					logFile "Approved payment not found for order ${s.orderId}"
				} else {
					logFile "Order ${s.orderId} has status paid with cost ${s.cost} and payment ${payment?.shipping_cost}"
				}
			
			} else
				logFile "Order ${s.orderId} has status ${order?.status}"
        }
    } catch (Exception e){
    	logFile("Exception processing shipment ${ship?.id}: ${e}")
	}
}

def searchForApprovedPayment(order, receiverId) {
	def approvedPaymentIds = order.payments.findAll{it.status == 'approved'}?.id
	
	def payments = approvedPaymentIds.collect{paymentService.getPaymentData(it, receiverId)}.findAll{it}
	
	return payments.find{it.operation_type == 'regular_payment'}
}

boolean isRecentlyPaid(payment) {
	def dateApproved = ISODateTimeFormat.dateTimeParser().parseDateTime(payment.date_approved).toDate()
	return ((new Date().time - dateApproved.time)/60/1000) < 30
}


def processScriptForSite(siteId) {
	
	try {

		out << "Getting pending shipments for services ${services[siteId]}...\n"

		def shipments = Shipment.withCriteria() {
	          isNotNull 'orderId'
	          eq 'shippingMode', 'me2'
	          eq 'status', 'pending'
	          eq 'siteId', siteId
	          'in' 'serviceId', services[siteId]
	          between 'dateCreated', new Date() - DAYS_BEFORE, new Date() - 30

	          projections {
	          	property 'id'
	          }
		}

		def total = shipments.size()
		out << "Total shipments found: ${total}\n"

		GParsPool.withPool(7) { p ->
			shipments.eachWithIndexParallel { id, index ->
		    	
				logFile("Processing shipment ${index+1} of ${total} with id ${id}")
		    	
		    	processOrderForShipmentId(id)
		        
		    	logFile("End processing shipment ${index+1} of ${total} with id ${id}")
		  	}
	  	}
	} catch (Exception e) {
		out << "Exception: ${e}\n"
	}
}

def processScript() {

	out << "Processing site MLA...\n"
	processScriptForSite('MLA')
	out << "End processing site MLA\n"

	
	out << "Processing site MLB...\n"
	processScriptForSite('MLB')
	out << "End processing site MLB\n"

}

processScript()
