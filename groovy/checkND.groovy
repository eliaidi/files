import mercadoenvios.*
import org.hibernate.FetchMode as FM
import org.hibernate.transform.DistinctRootEntityResultTransformer
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.Future
import static mercadoenvios.constants.ShippingConstants.*
import groovyx.gpars.GParsPool
import java.util.concurrent.atomic.AtomicInteger

out = new File('/tmp/out.log'); out.write(''); log = { text -> out << text + "\n" }
results = new File("/tmp/results0606.txt"); results.write(''); logR = { text -> results << text + "\n" }
events = new File("/tmp/events0606.csv");

eventsByTrackingNumber = Collections.synchronizedMap(new HashMap<String, List<String>>())

def fileEvents = events.readLines()
def fileSize = fileEvents.size()

log "Processing file...($fileSize events)"
GParsPool.withPool(10) {
	fileEvents.eachParallel { event -> 
		def tokens = event.split(",")
        if (!eventsByTrackingNumber[(tokens[0])])
            eventsByTrackingNumber[(tokens[0])] = []
        eventsByTrackingNumber[(tokens[0])] << tokens[2]
	}	
}

def eventsNotFound = Collections.synchronizedMap(new HashMap<String, List<String>>())
def counter = new AtomicInteger(eventsByTrackingNumber.size())

log "Events by TN: ${eventsByTrackingNumber.size()}"
if (eventsByTrackingNumber.collect{k,v -> v.size()}.sum() != fileSize)
	log "ERROR: Sizes mismatch"
else {
	log "OK: Sizes match"

	log "Processing events..."
	GParsPool.withPool(10) {
		eventsByTrackingNumber.eachParallel { tn, events ->

			List<TrackingEvents> trackingEvents = TrackingEvents.findAllByTrackingNumber(tn)
			def eventsNotReceived = events.findAll{!(it in trackingEvents*.trackingStatus)}
            
            log "events for $tn: ${trackingEvents*.trackingStatus}"
            log "events for $tn from nd: $events"
            if (eventsNotReceived) {
                eventsNotFound[(tn)] = []
                eventsNotFound[(tn)] << eventsNotReceived
            }
			log "Still ${counter.getAndDecrement()-1} to process.."
		}
	}

	log "Events not found: ${eventsNotFound.size()}"
	log "Processing results..."
	eventsNotFound.each { tn, events ->
		logR "$tn: $events"
	}
	log "Done."
}