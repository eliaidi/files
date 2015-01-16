import mercadoenvios.*
import static mercadoenvios.constants.ShippingConstants.*
import groovyx.gpars.GParsPool
import java.util.concurrent.atomic.AtomicInteger


out = new File('/tmp/out.log'); out.write(''); log = { text -> out << text + "\n" }
//results = new File("/tmp/resultsNotDelivered.txt"); results.write(''); logR = { text -> results << text + "\n" }
results = new File("/tmp/resultsDelivered.txt"); results.write(''); logR = { text -> results << text + "\n" }
//events = new File("/tmp/notDeliveredFailed.csv")
events = new File("/tmp/deliveredFailed.csv")

List slice(List l, int s) {
    def list = new ArrayList(l)
    def output = []
    while(list) {
        def chunk = []
        Math.min(s, list.size()).times {
            chunk << list.pop()
        }
        output << chunk
    }

    return output
}

def results = Collections.synchronizedMap(new HashMap<String, Set<Long>>())
def list = events.readLines()

log "Processing file..."
def chunks = slice(list, 100)
def counter = new AtomicInteger(chunks.size())
GParsPool.withPool(10) {
	chunks.eachParallel { chunk ->
		chunk.each{ event ->
			Shipment s = Shipment.findByTrackingNumber(event.split(",")[0])

			//if (s?.status && s.status != SHIP_STATUS_NOT_DELIVERED) {
			if (s?.status && s.status != SHIP_STATUS_DELIVERED) {
				if (results[(s.status)])
			    	results[(s.status)] << s.id
			  	else {
			    	results[(s.status)] = []
			    	results[(s.status)] << s.id
				}
			}
		}
		log "Processed chunk. Still ${counter.getAndDecrement()-1} to go"
	}
}

results.each { status, shipmentIds ->
    logR "$status"
    shipmentIds.each{ logR "$it" }
}

log "Done."