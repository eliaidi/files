import mercadoenvios.*
import org.hibernate.FetchMode as FM
import org.hibernate.transform.DistinctRootEntityResultTransformer
import groovyx.gpars.*

out = new File('/tmp/out.log'); out.write('')
log = { text -> out << text + "\n" }

def getShipments(from, to) {
	return Shipment.createCriteria().list {
	    between 'dateCreated', from, to
	    between 'dateShipped', from, to
	    eq 'siteId', 'MLB'
	    'in' 'status', ['shipped','delivered','not_delivered']
	    eq 'shippingMode', 'me2'
	    'in' 'serviceId', [21L,22L,23L]
	    fetchMode 'trackings', FM.JOIN
	    resultTransformer(new DistinctRootEntityResultTransformer())
	}
}
////////////////////////////////////////////////////////////////////////

beginDate = Date.parse('dd-MM-yy kk:mm:ss','01-04-14 00:00:00')
endDate = Date.parse('dd-MM-yy kk:mm:ss','02-04-14 23:59:59')

log "Begin process"
shippedShipments = []  

def from = beginDate
while ( from < endDate ) {
	def to = from + 1 <= endDate ? from + 1: endDate

	log "Getting shipments from $from to $to..."
	shippedShipments << getShipments(from, to)	    
	from++
}

shippedShipments = shippedShipments.flatten()

def totalShipped = shippedShipments.size()
log "(temp)Total shipped: $totalShipped"

def ndShipments
log "Processing ndShipments..."
GParsPool.withPool(7) {
	ndShipments = shippedShipments.findAllParallel{
		'ND10' in it.trackings*.trackingStatus
	}
}

def totalNdShipments = ndShipments.size()
log "(temp)totalNdShipments: $totalNdShipments"

def detailedNdShipments = [
	paidShipments : [stolenOrLost: [], other: []],
	nonPaidShipments: [],
	undefined: []
]

log "Processing detailedNdShipments..."
GParsPool.withPool(7) {
	ndShipments.eachParallel{
		if ('ND03' in it.trackings*.trackingStatus) 
			detailedNdShipments.paidShipments.stolenOrLost << it
		
		else if ('ND12' in it.trackings*.trackingStatus) 
			detailedNdShipments.paidShipments.other << it
		
		else if ('ND13' in it.trackings*.trackingStatus) 
			detailedNdShipments.nonPaidShipments << it
		else 
			detailedNdShipments.undefined << it
	}
}

log "Results:"
log "Total shipped: $totalShipped"
log "Total novaduque: $totalNdShipments"
log "Paid:"
log "	stolenOrLost: ${detailedNdShipments.paidShipments.stolenOrLost.size()}"
log "	other: ${detailedNdShipments.paidShipments.other.size()}"
log "Not Paid: ${detailedNdShipments.nonPaidShipments.size()}"
log "Undefined: ${detailedNdShipments.undefined.size()}"

log "End process"



