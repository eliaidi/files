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
getHours = { init, end -> def time; time = getTime(init, end); time.days*24 + time.hours }

add12Hours = { date -> new Date(date.time+12*60*60*1000) }

findEventsForStatus = { status, shipment -> shipment.trackings.findAll{
  trackingStatusService.getTrackingStatus(it.trackingStatus, shipment.serviceId).normalizedStatus == status}
}

finalStatuses = [SHIP_STATUS_DELIVERED, SHIP_STATUS_NOT_DELIVERED]
serviceIds = [21L,22L,23L,61L,62L,63L,64L,81L]

allShipments = Collections.synchronizedSet(new HashSet<Shipment>())

getAllShipments = { serviceId, dateShippedFrom, dateShippedTo, lastUpdatedFrom, lastUpdatedTo ->
    log "Getting shipments for serv $serviceId (${format lastUpdatedFrom}->${format lastUpdatedTo}) from $dateShippedFrom to $dateShippedTo..."
    
    def before = new Date()
    def shipments = Shipment.createCriteria().list {
        between 'dateShipped', dateShippedFrom, dateShippedTo
        between 'lastUpdated', lastUpdatedFrom, lastUpdatedTo
        'in' 'status', ['delivered','not_delivered']
        shippingService {
          eq 'id', serviceId
        }
        eq 'shippingMode', 'me2'
        fetchMode 'trackings', FM.JOIN
        resultTransformer(new DistinctRootEntityResultTransformer())
    }
    def after = new Date()
    allShipments << shipments
    log "Getting shipments for serv $serviceId (${format lastUpdatedFrom}->${format lastUpdatedTo}) from $dateShippedFrom to $dateShippedTo finished in ${getTime(before, after).seconds}s..."
}

threadPool = Executors.newFixedThreadPool(10)

beginDate = Date.parse('dd-MM-yy HH:mm:ss','15-05-14 00:00:00')
endDate = Date.parse('dd-MM-yy HH:mm:ss','30-05-14 23:59:59')
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
def shipmentsWithBothStatus = []; def shipmentsNotDeli_Deli = []; def shipmentsDeli_notDeli = []; def sameDate = []
def counter = new AtomicInteger(allShipments.size())

log "Processing shipments..."
try {
  allShipments.each{ ships ->
    
    GParsPool.withPool(10) {
      ships.eachParallel{ ship ->
        def eventsForDelivered = findEventsForStatus(SHIP_STATUS_DELIVERED, ship); def deliveredEvent
        if (eventsForDelivered)
          deliveredEvent = eventsForDelivered.sort{it.dateCreated}.first()
        def eventsForNotDelivered = findEventsForStatus(SHIP_STATUS_NOT_DELIVERED, ship); def notDeliveredEvent
        if (eventsForNotDelivered)
          notDeliveredEvent = eventsForNotDelivered.sort{it.dateCreated}.first()
    
        if (deliveredEvent && notDeliveredEvent) {
          shipmentsWithBothStatus << ship
          def ndeDC = notDeliveredEvent.dateCreated
          def deDC = deliveredEvent.dateCreated

          if (ndeDC < deDC) {
            shipmentsNotDeli_Deli << [shipment: ship, time: getHours(ndeDC,deDC)]
          
          } else if (ndeDC > deDC) {

            shipmentsDeli_notDeli << [shipment: ship, time: getHours(deDC,ndeDC)]
          
          } else {
            sameDate << ship
          }
        }
      }
      log "Still ${counter.getAndDecrement()-1} to process.."
    }
  }
} catch (Exception e) {
  log "Exception: ${e.getMessage()} - ${e.getStackTrace()}"
}

try {
  log "shipmentsNotDeli_Deli: ${shipmentsNotDeli_Deli.size()}"; log "shipmentsDeli_notDeli: ${shipmentsDeli_notDeli.size()}"
  logR "RESULTS:"
  if (shipmentsNotDeli_Deli && shipmentsDeli_notDeli) {
    def sizeShipsND_D = shipmentsNotDeli_Deli.size()
    def sizeShipsD_ND = shipmentsDeli_notDeli.size()
    
    logR "Total: ${shipmentsWithBothStatus.size()}"
    logR "    * Not_Delivered->Delivered: ${sizeShipsND_D} => ${shipmentsNotDeli_Deli.collect{it.time}.sum() / sizeShipsND_D}"
    logR "    * Delivered->Not_Delivered: ${sizeShipsD_ND} => ${shipmentsDeli_notDeli.collect{it.time}.sum() / sizeShipsD_ND}"

    logR "Times:"
    logR "    * Not_Delivered->Delivered: ${shipmentsNotDeli_Deli.collect{[it.shipment.id, it.time]}}"
    logR "    * Delivered->Not_Delivered: ${shipmentsDeli_notDeli.collect{[it.shipment.id, it.time]}}"

    logR "\n///////shipmentsNotDeli_Deli/////////"
    getFirstX(50, shipmentsNotDeli_Deli).each{ s -> logR "${s.shipment.id}: ${s.shipment.trackings*.collect{trackingStatusService.getTrackingStatus(it.trackingStatus, s.shipment.serviceId).normalizedStatus}}" }

    logR "\n///////shipmentsDeli_notDeli/////////"
    getFirstX(50, shipmentsDeli_notDeli).each{ s -> logR "${s.shipment.id}: ${s.shipment.trackings*.collect{trackingStatusService.getTrackingStatus(it.trackingStatus, s.shipment.serviceId).normalizedStatus}}" }

    logR "\n///////sameDate/////////"
    getFirstX(50, sameDate).each{ s -> logR "${s.id}: ${s.trackings*.collect{trackingStatusService.getTrackingStatus(it.trackingStatus, s.serviceId).normalizedStatus}}" }

  } else {
    logR "No RESULTS found"
  }
  log "\nEnd process"; def ep = new Date()
  def duration = getTime(bp, ep)
  logR "\nProcess time: ${duration.hours}h ${duration.minutes}m ${duration.seconds}s"

} catch (Exception e) {
  log "Exception in process. ${e.getMessage()} - ${e.getStackTrace()}"
}