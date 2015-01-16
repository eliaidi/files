import mercadoenvios.*
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import groovyx.gpars.*
import java.math.*


  def siteId = 'MLA'
  String outPath = "/tmp/out"+siteId+".txt"
  String allPath = "/tmp/all"+siteId+".txt"
  def servicesBySite = ['MLA': [61L,62L,63L,64L], 'MLB': [21L,22L]]

  out = new File(outPath); out.write('')
  all = new File(allPath); all.write('')
  c = ctx.getBean('computeHandlingTimeService')
  def cal = Calendar.instance

  //Main
  try {
    def allShipments = []
  
    for ( i in (7..9) ) {
      log("i: $i")
      
      cal.set(year:2013, month:i)
      def from = cal.time
      cal.set(year:2013, month:i+1)
      def to = cal.time
            
      log("--- ${from.format('dd/MM/yy hh:mm:ss')} to ${to.format('dd/MM/yy hh:mm:ss')} ---")
    
      def shipments = getDeliveredShipments(from, to, siteId, servicesBySite[siteId])
      
      log("shipments: ${shipments.size()}")
           
      allShipments << shipments
    }
    
    def shipmentsByRuleId = allShipments.flatten().groupBy{it.appliedShippingRuleId}.findAll{it.value.size() > 100}
    
    log("shipmentsByRuleId: ${shipmentsByRuleId.size()}")
    shipmentsByRuleId.sort{e1, e2 -> e2.value.size() <=> e1.value.size()}.each{ entry ->
      try {
        log("size: ${entry.value.size()}")

        def shippingTimes = Collections.synchronizedList(new ArrayList<Integer>())
        
        GParsPool.withPool(7) {
          entry.value.eachParallel { shipment ->
              
            shippingTimes << c.getWorkingDays(shipment.dateShipped, shipment.dateDelivered, siteId)         
          }
        }
        shippingTimes = shippingTimes.collect{if (it==0) it++; it} //Transformo 0s en 1s (no hay envios con 0 dias habiles)
        
        def stats = new DescriptiveStatistics(shippingTimes as double[])
        def p99 = stats.getPercentile(99d)
        log("p99: $p99")
        log("out: ${shippingTimes.findAll{it > p99}}")
        
        shippingTimes = shippingTimes.findAll{it <= p99} //Saco envios raros con altos tiempos que deforman estadistica
        log("size: ${shippingTimes.size()}")
        log("${entry.key}: ${shippingTimes}")

        stats = new DescriptiveStatistics(shippingTimes as double[])
        int estimated = entry.value.first().speed
        def nailed = shippingTimes.findAll{(it as int) == estimated}.size() * 100 / shippingTimes.size()
        def more = shippingTimes.findAll{(it as int) > estimated}.size() * 100 / shippingTimes.size()
        def less = shippingTimes.findAll{(it as int) < estimated}.size() * 100 / shippingTimes.size()

        all << "${entry.key}:\t n:${stats.n}\t e: ${estimated}\t p: ${round(stats.mean)} d: ${round(stats.standardDeviation)} m: ${stats.min} M: ${stats.max}\t\t - accuracy: ${round(nailed)}% - less: ${round(less)}% - more: ${round(more)}%\n"

      } catch(Exception e) {
        log("ERROR2. Exception: $e")
      }
    }
    
  } catch(Exception e) {
      log("ERROR. Exception: $e")
  }
  

  //Functions
  def getDifference(shipment, siteId){
    def estimatedTime = shipment.speed
    def realTime = c.getWorkingDays(shipment.dateShipped, shipment.dateDelivered, siteId)
    return estimatedTime - realTime
  }
  
  def getDeliveredShipments(from, to, siteId, serviceIds) {
    
    return Shipment.withCriteria {
      eq 'siteId', siteId
      'in' 'serviceId', serviceIds
      between 'dateDelivered', from, to
      isNotNull 'dateShipped'
      isNotNull 'speed'
      eq 'shippingMode', 'me2'
    }
  }

  def round(value) {
    return new BigDecimal(value).setScale(2, RoundingMode.HALF_UP)
  }

  void log(String line) {
    out << line + "\n"
  }
