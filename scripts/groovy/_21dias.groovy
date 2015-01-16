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

trackingStatusService = ctx.getBean('trackingStatusService')

out = new File('/tmp/out.log'); out.write('')
results = new File('/tmp/results.txt'); results.write('')
log = { text -> out << text + "\n" }
logR = { text -> results << text + "\n" }
format = { date -> date.format('dd-MM HH:mm') }
getFirstX = { x, list -> x < list.size() ? list[0..(x-1)] : list }

getTime = { init , end -> def time; use(groovy.time.TimeCategory) { time = end - init } }

add12Hours = { date -> new Date(date.time+12*60*60*1000) }

serviceIds = [21L,22L,23L,61L,62L,63L,64L,81L]

allShipments = Collections.synchronizedSet(new HashSet<Shipment>())

getAllShipments = { serviceId, dateShippedFrom, dateShippedTo, lastUpdatedFrom, lastUpdatedTo ->
    log "Getting shipments for serv $serviceId (${format lastUpdatedFrom}->${format lastUpdatedTo}) from $dateShippedFrom to $dateShippedTo..."
    
    def before = new Date()
    def shipments = Shipment.createCriteria().list {
        between 'dateCreated', dateShippedFrom, dateShippedTo
        between 'lastUpdated', lastUpdatedFrom, lastUpdatedTo
        'in' 'status', ['shipped','delivered','not_delivered']
        shippingService {
          eq 'id', serviceId
        }
        eq 'shippingMode', 'me2'
    }
    def after = new Date()
    allShipments << shipments
    log "Getting shipments for serv $serviceId (${format lastUpdatedFrom}->${format lastUpdatedTo}) from $dateShippedFrom to $dateShippedTo finished in ${getTime(before, after).seconds}s..."
}

threadPool = Executors.newFixedThreadPool(10)

beginDate = Date.parse('dd-MM-yy HH:mm:ss','05-06-14 00:00:00')
endDate = Date.parse('dd-MM-yy HH:mm:ss','07-06-14 23:59:59')
today = new Date()

log "Begin process"; def bp = new Date()
try {
  List<Future> futures = new ArrayList<Future>()
  
  def fromDS = beginDate  
  while ( fromDS < endDate ) {
    def _toDS = add12Hours(fromDS) <= endDate ? add12Hours(fromDS) : endDate
    def _fromDS = fromDS 
    
    def fromLU = _fromDS
    while ( fromLU < today ) {
      def _toLU = add12Hours(fromLU) <= today ? add12Hours(fromLU) : today
      def _fromLU = fromLU

      serviceIds.each{ servId ->
        futures << threadPool.submit({-> getAllShipments(servId, _fromDS, _toDS, _fromLU, _toLU)} as Callable)
      }
      fromLU = add12Hours(fromLU)
    }
    fromDS = add12Hours(fromDS)
  }
  futures.each{it.get()}
} finally {
  threadPool.shutdown()
}

log "Querying process done. Sizes:"; allShipments.each{ log "${it.size()}" }; log "Total: ${allShipments.collect{it.size()}.sum()}"
log "Size to process: ${allShipments.size()}"

def counter = new AtomicInteger(allShipments.size())
def shipped21 = []; def delivered21 = []; def notDelivered21 = []

log "Processing shipments..."
try {
  GParsPool.withPool(10) {
    allShipments.eachParallel{ ships ->
    
      ships.each{ ship ->
        
        if (ship?.dateDelivered && getTime(ship?.dateCreated, ship?.dateDelivered)?.days > 21)
            delivered21 << ship

        else if (ship?.dateNotDelivered && getTime(ship?.dateCreated, ship?.dateNotDelivered)?.days > 21)
            notDelivered21 << ship
        
        else if (ship?.dateShipped && getTime(ship?.dateCreated, ship?.dateShipped)?.days > 21)
            shipped21 << ship
      }
      log "Still ${counter.getAndDecrement()-1} to process.."
    }
  }
} catch (Exception e) {
  log "Exception: ${e.getMessage()} - ${e.getCause()} - ${e.printStackTrace()}"
}

def total = allShipments.collect{it.size()}.sum()
def total21 = delivered21.size() + notDelivered21.size() + shipped21.size()
try {
  logR "RESULTS:"
  
  logR "Total: ${total}"
 
  if (total && total21) {
    logR "Total 21: ${total21} - ${total21*100/total}%"
    logR "delivered21: ${delivered21.size()} - ${(delivered21.size()*100)/total21}%"
    logR "notDelivered21: ${notDelivered21.size()} - ${(notDelivered21.size()*100)/total21}%"
    logR "shipped21: ${shipped21.size()} - ${(shipped21.size()*100)/total21}%"
    logR "Del total completo de envios:"
    logR "delivered21: ${(delivered21.size()*100)/total}%"
    logR "notDelivered21: ${(notDelivered21.size()*100)/total}%"
    logR "shipped21: ${(shipped21.size()*100)/total}%"

    logR "\n///////delivered21/////////"
    getFirstX(50, delivered21).each{ s -> logR "${s?.id}: ${s?.dateCreated} -> ${s?.dateDelivered}" }

    logR "\n///////notDelivered21/////////"
    getFirstX(50, notDelivered21).each{ s -> logR "${s?.id}: ${s?.dateCreated} -> ${s?.dateNotDelivered}" }

    logR "\n///////shipped21/////////"
    getFirstX(50, shipped21).each{ s -> logR "${s?.id}: ${s?.dateCreated} -> ${s?.dateShipped}" }
    
  }

  log "\nEnd process"; def ep = new Date()

  def duration = getTime(bp, ep)
  logR "\nProcess time: ${duration.hours}h ${duration.minutes}m ${duration.seconds}s"

} catch (Exception e) {
  log "Exception in process. ${e.getMessage()} - ${e.getCause()} - ${e.printStackTrace()}"
}