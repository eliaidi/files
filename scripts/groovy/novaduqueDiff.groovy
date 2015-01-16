import mercadoenvios.*
import org.hibernate.FetchMode as FM
import org.hibernate.transform.DistinctRootEntityResultTransformer
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.Future
import groovyx.gpars.GParsPool
import java.util.concurrent.atomic.AtomicInteger

PI_ABIERTA = 'ND10';ENTREGADO = 'ND01';INDEMNIZACION_ACEPTADA_EXTRAVIO = 'ND03';EN_DEVOLUCION = 'ND15';DEVUELTO = 'ND02'

out = new File('/tmp/out.log'); out.write('')
results = new File('/tmp/results.txt'); results.write('')
piNotReceived = new File('/tmp/piNotReceived.csv').readLines()
log = { text -> out << text + "\n" }
logR = { text -> results << text + "\n" }
format = { date -> date.format('dd-MM HH:mm') }
getFirstX = { x, list -> x < list.size() ? list[0..(x-1)] : list }
add12Hours = { date -> new Date(date.time+12*60*60*1000) }
serviceIds = [21L,22L,23L]

getTime = { init , end -> 
  def time
  use(groovy.time.TimeCategory) { 
    time = end - init
  }
}

ndShipments = Collections.synchronizedSet(new HashSet<Shipment>())

getNDShipments = { serviceId, dateShippedFrom, dateShippedTo, lastUpdatedFrom, lastUpdatedTo ->
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
        trackings {
          like 'trackingStatus', 'ND%'
        }
        resultTransformer(new DistinctRootEntityResultTransformer())
    }
    def after = new Date()
    ndShipments << shipments
    log "Getting shipments for serv $serviceId (${format lastUpdatedFrom}->${format lastUpdatedTo}) from $dateShippedFrom to $dateShippedTo finished in ${getTime(before, after).seconds}s..."
}

threadPool = Executors.newFixedThreadPool(10)

beginDate = Date.parse('dd-MM-yy kk:mm:ss','28-05-14 00:00:00')
endDate = Date.parse('dd-MM-yy kk:mm:ss','12-06-14 23:59:59')
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
        futures << threadPool.submit({-> getNDShipments(servId, _fromDS, _toDS, _fromLU, _toLU)} as Callable)
      }
      fromLU = add12Hours(fromLU)
    }
    fromDS = add12Hours(fromDS)
  }
  futures.each{it.get()}
} finally {
  threadPool.shutdown()
}

log "Querying process done. Sizes:"; ndShipments.each{ log "${it.size()}" }; log "Total: ${ndShipments.collect{it.size()}.sum()}"

def openedShipments = []; def closedShipments = []; def withoutPIFailed = []; def withoutPINotReceived = []

log "Processing shipments..."
try {
  def counter = new AtomicInteger(ndShipments.size())

  GParsPool.withPool(10) {
    ndShipments.eachParallel { ships ->
      
      ships.each{ ship ->
        def status = ship.trackings*.trackingStatus
        
        if (PI_ABIERTA in status) {
          def finalStatuses = [ENTREGADO,EN_DEVOLUCION,DEVUELTO,INDEMNIZACION_ACEPTADA_EXTRAVIO].intersect(status)
          if (finalStatuses) {
            def piEvent = ship.trackings.find{it.trackingStatus == PI_ABIERTA}
            def finalEvent = ship.trackings.findAll{it.trackingStatus in finalStatuses}.sort{it.eventDate}.first()

            def diffEventDate = getTime(piEvent.eventDate, finalEvent.eventDate)
            def diffDateCreated = getTime(piEvent.dateCreated, finalEvent.dateCreated)

            closedShipments << [shipment: ship, 
                                time_event: diffEventDate.days*24 + diffEventDate.hours,
                                time_created: diffDateCreated.days*24 + diffDateCreated.hours]
          } else
            openedShipments << ship
        } else
          if (ship.trackingNumber in piNotReceived)
            withoutPIFailed << ship
          else
            withoutPINotReceived << ship
      }
      log "Still ${counter.getAndDecrement()-1} to process.."
    }  
  }
  
} catch (Exception e) {
  log "Exception: ${e.getMessage()} - ${e.getStackTrace()}"
}

positiveClosedShipments = closedShipments.findAll{it.time_event >= 0}
negativeClosedShipments = closedShipments.findAll{it.time_event < 0}
positiveClosedDCShipments = closedShipments.findAll{it.time_created >= 0}
negativeClosedDCShipments = closedShipments.findAll{it.time_created < 0}

try {
  if (ndShipments.find{it.size > 0} && closedShipments) {
    logR "RESULTS"
    logR "Total novaduque: ${ndShipments.collect{it.size()}.sum()}"
    logR "closedShipments: ${closedShipments.size()}"
    logR "positiveClosedShipments: ${positiveClosedShipments.size()} - mean: ${positiveClosedShipments.collect{it.time_event}.sum() / positiveClosedShipments.size()}"
    logR "negativeClosedShipments: ${negativeClosedShipments.size()}"
    logR "positiveClosedDCShipments: ${positiveClosedDCShipments.size()} - mean: ${positiveClosedDCShipments.collect{it.time_created}.sum() / positiveClosedDCShipments.size()}"
    logR "negativeClosedDCShipments: ${negativeClosedDCShipments.size()}"
    logR "openedShipments: ${openedShipments.size()}"
    logR "withoutPIFailed: ${withoutPIFailed.size()}"
    logR "withoutPINotReceived: ${withoutPINotReceived.size()}"

    logR "\n///////positiveClosedShipments/////////"
    getFirstX(550, positiveClosedShipments).each{ cs -> logR "${cs?.shipment?.trackingNumber}: ${cs?.shipment?.trackings*.trackingStatus} (${cs?.shipment?.trackings*.trackingDescription?.collect{it[0..7]}})" }

    logR "\n///////positiveClosedDCShipments/////////"
    getFirstX(550, positiveClosedDCShipments).each{ cs -> logR "${cs?.shipment?.trackingNumber}: ${cs?.shipment?.trackings*.trackingStatus} (${cs?.shipment?.trackings*.trackingDescription?.collect{it[0..7]}})" }

    logR "\n///////negativeClosedShipments/////////"
    getFirstX(550, negativeClosedShipments).each{ cs -> logR "${cs?.shipment?.trackingNumber}: ${cs?.shipment?.trackings*.trackingStatus} (${cs?.shipment?.trackings*.trackingDescription?.collect{it[0..7]}})" }
    
    logR "\n///////negativeClosedDCShipments/////////"
    getFirstX(550, negativeClosedDCShipments).each{ cs -> logR "${cs?.shipment?.trackingNumber}: ${cs?.shipment?.trackings*.trackingStatus} (${cs?.shipment?.trackings*.trackingDescription?.collect{it[0..7]}})" }  

    logR "\n///////openedShipments/////////"
    getFirstX(550, openedShipments).each{ s -> logR "${s?.trackingNumber}: ${s?.trackings*.trackingStatus} (${s?.trackings*.trackingDescription?.collect{it[0..7]}})" }

    logR "\n///////withoutPIFailed/////////"
    getFirstX(550, withoutPIFailed).each{ s -> logR "${s?.trackingNumber}: ${s?.trackings*.trackingStatus} (${s?.trackings*.trackingDescription?.collect{it[0..7]}})" }

    logR "\n///////withoutPINotReceived/////////"
    getFirstX(550, withoutPINotReceived).each{ s -> logR "${s?.trackingNumber}: ${s?.trackings*.trackingStatus} (${s?.trackings*.trackingDescription?.collect{it[0..7]}})" }

  } else
    logR "No RESULTS found"
} catch (Exception e) {
  log "Exception: ${e.getMessage()} - ${e.getStackTrace()}"
}

log "\nEnd process"; def ep = new Date()
def duration = getTime(bp, ep)
logR "\nProcess time: ${duration.minutes}m ${duration.seconds}s"