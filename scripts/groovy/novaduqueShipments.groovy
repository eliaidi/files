import mercadoenvios.*
import org.apache.log4j.Logger
import groovyx.gpars.*
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorCompletionService
import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit
import org.hibernate.FetchMode as FM

class NovaduqueShipmentService {

	static transactional = false

	static THREADS = 10
	static DAYS_CHUNK = 3

	def executors = Executors.newFixedThreadPool(THREADS)
	def pool = new ExecutorCompletionService(executors)
	
	static class Worker implements Callable<List> {
		def from
		def to
		def novaduqueShipments

		Logger logR = Logger.getRootLogger()

		List call() {
			try {
				//logR.info "Getting shipments from $from and $to..."
				def shipments = Shipment.createCriteria().list {
					/*between 'dateCreated', from, to
					between 'dateShipped', from, to
					eq 'siteId', 'MLB'
					'in' 'status', ['shipped']//,'delivered','not_delivered']
					eq 'shippingMode', 'me2'
					'in' 'serviceId', [21L,22L,23L]*/
					//fetchMode 'trackings', FM.JOIN
					'in' 'id', [21152176699L, 21152176700L, 21151826040L]
			    }
			    //logR.info "Getting shipments from $from and $to done."
				
				novaduqueShipments << shipments

				return null

			} catch (Exception e) {
				logR.error "Error while getting shipments", e
			}

		}

	}

	def process(Date from, Date to) {
		
		def novaduqueShipments = Collections.synchronizedList(new ArrayList<Shipment>())
		
		def limit = to
		while ( from < limit ) {
			to = from + DAYS_CHUNK <= limit ? from + DAYS_CHUNK: limit 
		    
		    def thread = pool.submit(new NovaduqueShipmentService.Worker(from: from, to: to, novaduqueShipments: novaduqueShipments))
		    thread.get()
		    
		    from = to + 1  
		}
		
		/*for (int i=0; i < THREADS; i++) {
			pool.poll(2, TimeUnit.MINUTES).get()
			log.info "getting thread $i"
		}*/

		//TODO sacar los ND03 del 14/04
		return novaduqueShipments.flatten()
	}

}

def outFile = new File("/tmp/out.log"); outFile.write("")
out = { text -> outFile << text + "\n" }

out "Begin process"
def novaduqueShipmentService = new NovaduqueShipmentService()
def fromDate = Date.parse('dd-MM-yy kk:mm:ss','01-04-14 00:00:00')
def toDate = Date.parse('dd-MM-yy kk:mm:ss','30-04-14 23:59:59')

out "Getting shipments..."
def shippedShipments = novaduqueShipmentService.process(fromDate, toDate)

out "Processing..."
def totalShipped = shippedShipments.size()
def totalNdShipments

GParsPool.withPool(7) {
	def ndShipments = shippedShipments.findAllParallel{
		'ND10' in it.trackings*.trackingStatus
	}
	totalNdShipments = ndShipments.size()
}

def detailedNdShipments = [
	paidShipments : [stolenOrLost: [], other: []],
	nonPaidShipments: [],
	undefined: []
]
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

out "Results:"
out "Total shipped: $totalShipped"
out "Total novaduque: $totalNdShipments"
out "Paid:"
out "	stolenOrLost: ${detailedNdShipments.paidShipments.stolenOrLost.size()}"
out "	other: ${detailedNdShipments.paidShipments.other.size()}"
out "Not Paid: ${detailedNdShipments.nonPaidShipments.size()}"
out "Undefined: ${detailedNdShipments.undefined.size()}"

out "End process"

