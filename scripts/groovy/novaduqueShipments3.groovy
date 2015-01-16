import mercadoenvios.*
import org.hibernate.FetchMode as FM
import org.hibernate.transform.DistinctRootEntityResultTransformer
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.Future
import groovyx.gpars.GParsExecutorsPool

PI_ABIERTA = 'ND10';INDEMNIZACION_ACEPTADA_EXTRAVIO = 'ND03';INDEMNIZACION_ACEPTADA_ATRASO = 'ND12'
INDEMNIZACION_RECHAZADA = 'ND13';DEVUELTO = 'ND02';EN_DEVOLUCION = 'ND15';ENTREGADO = 'ND01'

out = new File('/tmp/out.log'); out.write('')
results = new File('/tmp/results.txt'); results.write('')
tnError = new File('/tmp/tnND1404.txt')
log = { text -> out << text + "\n" }
logR = { text -> results << text + "\n" }
format = { date -> date.format('dd-MM HH:mm') }
getFirstX = { x, list -> x < list.size() ? list[0..(x-1)] : list }

getTime = { init , end -> 
  def time
  use(groovy.time.TimeCategory) { 
    time = end - init
  }
}

ndShipments = Collections.synchronizedSet(new HashSet<Shipment>())

getNDShipments = { dateShippedFrom, dateShippedTo, lastUpdatedFrom, lastUpdatedTo ->
    log "Getting shipments (${format lastUpdatedFrom}->${format lastUpdatedTo}) from $dateShippedFrom to $dateShippedTo..."
    
    def before = new Date()
    def shipments = Shipment.createCriteria().list {
        between 'lastUpdated', lastUpdatedFrom, lastUpdatedTo
        'in' 'status', ['shipped','delivered','not_delivered']
        shippingService {
          'in' 'id', [21L,22L,23L]
        }
        trackings {
          like 'trackingStatus', 'ND%'
          gt 'eventDate', dateShippedFrom
        }
        eq 'shippingMode', 'me2'
        between 'dateShipped', dateShippedFrom, dateShippedTo
        resultTransformer(new DistinctRootEntityResultTransformer())
    }
    def after = new Date()
    ndShipments << shipments
    log "Getting shipments (${format lastUpdatedFrom}->${format lastUpdatedTo}) from $dateShippedFrom to $dateShippedTo finished in ${getTime(before, after).seconds}s..."
}

threadPool = Executors.newFixedThreadPool(3)

beginDate = Date.parse('dd-MM-yy kk:mm:ss','01-04-14 00:00:00')
endDate = Date.parse('dd-MM-yy kk:mm:ss','30-04-14 23:59:59')
today = new Date()

log "Begin process"; def bp = new Date()
try {
  List<Future> futures = new ArrayList<Future>()
  
  def fromDS = beginDate  
  while ( fromDS < endDate ) {
    def _toDS = fromDS + 1 <= endDate ? fromDS + 1: endDate
    def _fromDS = fromDS 
    
    def fromLU = _fromDS
    while ( fromLU < today ) {
      def _toLU = fromLU + 1 <= today ? fromLU + 1 : today
      def _fromLU = fromLU

      futures << threadPool.submit({-> getNDShipments(_fromDS, _toDS, _fromLU, _toLU)} as Callable)
      fromLU++
    }
    fromDS++
  }
  futures.each{it.get()}
} finally {
  threadPool.shutdown()
}

log "Querying process done. Sizes:"; ndShipments.each{ log "${it.size()}" }; log "Total: ${ndShipments.collect{it.size()}.sum()}"

def paidStolenOrLostShipments = [];def paidDelayedShipments = [];def nonPaidShipments = [];def undefinedShipments = []
def openedShipments = [];def closedShipments = [];def withoutPI = []

log "Processing shipments..."
try {
  ndShipments.eachWithIndex { ships, i ->

    log "Processing detailedNdShipments $i..."
    ships.each{ ship ->
      def status = ship.trackings*.trackingStatus
      if (INDEMNIZACION_ACEPTADA_EXTRAVIO in status) 
        paidStolenOrLostShipments << ship
      
      else if (INDEMNIZACION_ACEPTADA_ATRASO in status) 
        paidDelayedShipments << ship
      
      else if (INDEMNIZACION_RECHAZADA in status) 
        nonPaidShipments << ship
      else 
        undefinedShipments << ship

      if (PI_ABIERTA in status) {
        def finalStatuses = [ENTREGADO,EN_DEVOLUCION,DEVUELTO,INDEMNIZACION_ACEPTADA_EXTRAVIO].intersect(status)
        if (finalStatuses) {
          def piEvent = ship.trackings.find{it.trackingStatus == PI_ABIERTA}
          def finalEvent = ship.trackings.findAll{it.trackingStatus in finalStatuses}.sort{it.eventDate}.first()

          closedShipments << [shipment: ship, 
                              time_event: getTime(piEvent.eventDate, finalEvent.eventDate).days,
                              time_created: getTime(piEvent.dateCreated, finalEvent.dateCreated).days]
        } else
          openedShipments << ship
      } else
          withoutPI << ship
    }
  }
} catch (Exception e) {
  log "Exception: ${e.getMessage()} - ${e.getStackTrace()}"
}

def tnND1404 = tnError.readLines()
def errorND03 = paidStolenOrLostShipments.findAll{it.trackingNumber in tnND1404}
paidStolenOrLostShipments = paidStolenOrLostShipments - errorND03

positiveClosedShipments = closedShipments.findAll{it.time_event >= 0}
negativeClosedShipments = closedShipments.findAll{it.time_event < 0}
positiveClosedDCShipments = closedShipments.findAll{it.time_created >= 0}
negativeClosedDCShipments = closedShipments.findAll{it.time_created < 0}

if (ndShipments.find{it.size > 0}) {
  logR "RESULTS"
  logR "Total novaduque: ${ndShipments.collect{it.size()}.sum()}"
  logR "    * Paid:"
  logR "      - Stolen | Lost: ${paidStolenOrLostShipments.size()}  => ${paidStolenOrLostShipments.collect{it.realCost}.sum()}"
  logR "        - Error ND03:  ${errorND03.size()}                  => ${errorND03.collect{it.realCost}.sum()}"
  logR "      - Delayed:       ${paidDelayedShipments.size()}       => ${paidDelayedShipments.collect{it.realCost}.sum()}"
  logR "    * Not Paid:        ${nonPaidShipments.size()}           => ${nonPaidShipments.collect{it.realCost}.sum()}"
  logR "    * Undefined:       ${undefinedShipments.size()}         => ${undefinedShipments.collect{it.realCost}.sum()}"
  logR ""
  logR "closedShipments: ${closedShipments.size()} - OK: ${positiveClosedShipments.size()} - NEG: ${negativeClosedShipments.size()}"
  logR "mean OK: ${positiveClosedShipments.collect{it.time_event}.sum() / positiveClosedShipments.size()}"
  logR "(+)DC: ${positiveClosedDCShipments.size()} .. (-)DC: ${negativeClosedDCShipments.size()}"
  logR "mean (+): ${positiveClosedDCShipments.collect{it.time_created}.sum() / positiveClosedDCShipments.size()}"
  logR "openedShipments: ${openedShipments.size()}"
  logR "withoutPI: ${withoutPI.size()}"

  logR "\n///////paidStolenOrLostShipments/////////"
  getFirstX(50, paidStolenOrLostShipments).each{ s -> logR "${s.id}: ${s.trackings*.trackingStatus} (${s.trackings*.trackingDescription.collect{it[0..7]}})" }

  logR "\n///////paidDelayedShipments/////////"
  getFirstX(50, paidDelayedShipments).each{ s -> logR "${s.id}: ${s.trackings*.trackingStatus} (${s.trackings*.trackingDescription.collect{it[0..7]}})" }

  logR "\n///////nonPaidShipments/////////"
  getFirstX(50, nonPaidShipments).each{ s -> logR "${s.id}: ${s.trackings*.trackingStatus} (${s.trackings*.trackingDescription.collect{it[0..7]}})" }

  logR "\n///////undefinedShipments/////////"
  getFirstX(50, undefinedShipments).each{ s -> logR "${s.id}: ${s.trackings*.trackingStatus} (${s.trackings*.trackingDescription.collect{it[0..7]}})" }

  logR "\n///////positiveClosedShipments/////////"
  getFirstX(50, positiveClosedShipments).each{ cs -> logR "${cs.shipment.id}: ${cs.shipment.trackings*.trackingStatus} (${cs.shipment.trackings*.trackingDescription.collect{it[0..7]}})" }

  logR "\n///////openedShipments/////////"
  getFirstX(50, openedShipments).each{ s -> logR "${s.id}: ${s.trackings*.trackingStatus} (${s.trackings*.trackingDescription.collect{it[0..7]}})" }

  logR "\n///////withoutPI/////////"
  getFirstX(50, withoutPI).each{ s -> logR "${s.id}: ${s.trackings*.trackingStatus} (${s.trackings*.trackingDescription.collect{it[0..7]}})" }

  logR "\n///////negativeClosedShipments/////////"
  getFirstX(50, negativeClosedShipments).each{ cs -> logR "${cs.shipment.id}: ${cs.shipment.trackings*.trackingStatus} (${cs.shipment.trackings*.trackingDescription.collect{it[0..7]}})" }

  logR "\n///////positiveClosedDCShipments/////////"
  getFirstX(50, positiveClosedDCShipments).each{ cs -> logR "${cs.shipment.id}: ${cs.shipment.trackings*.trackingStatus} (${cs.shipment.trackings*.trackingDescription.collect{it[0..7]}})" }

  logR "\n///////negativeClosedDCShipments/////////"
  getFirstX(50, negativeClosedDCShipments).each{ cs -> logR "${cs.shipment.id}: ${cs.shipment.trackings*.trackingStatus} (${cs.shipment.trackings*.trackingDescription.collect{it[0..7]}})" }  

} else
  logR "No RESULTS found"

log "\nEnd process"; def ep = new Date()
def duration = getTime(bp, ep)
logR "\nProcess time: ${duration.minutes}m ${duration.seconds}s"