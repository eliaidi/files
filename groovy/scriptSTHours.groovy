import mercadoenvios.*
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import groovyx.gpars.*
import java.math.*
import groovy.time.TimeCategory

  nonWorkingDays = ["MLB":["12/25/12", "12/31/12", "01/01/13", "02/12/13", "03/29/13", "04/21/13",
                           "05/01/13", "05/30/13", "11/15/13", "12/25/13", "11/25/13", "07/09/13", "11/20/13"],
                    "MLA":["01/01/13", "01/31/13", "02/11/13", "02/12/13", "02/20/13", "03/24/13",
                           "03/29/13", "04/01/13", "04/02/13", "05/01/13", "05/25/13", "06/20/13", "06/21/13",
                           "07/09/13", "08/19/13", "10/14/13", "11/25/13", "12/08/13", "12/25/13"]]

  def siteId = 'MLA'
  String outPath = "/tmp/out"+siteId+".txt"
  String allPath = "/tmp/all"+siteId+".txt"
  def servicesBySite = ['MLA': [61L,62L,63L,64L], 'MLB': [21L,22L]]

  out = new File(outPath); out.write('')
  all = new File(allPath); all.write('')
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

        def shippingTimes = Collections.synchronizedMap(new HashMap<Long, Integer>())
        
        GParsPool.withPool(7) {
          entry.value.eachParallel { shipment ->
              
            shippingTimes[shipment.id] = getWorkingHours(shipment.dateShipped, shipment.dateDelivered, siteId)         
          }
        }
        
        def stats = new DescriptiveStatistics(shippingTimes*.value as double[])
        def p99 = stats.getPercentile(99d)
        log("p99: $p99")
        log("out: ${shippingTimes*.value.findAll{it > p99}}")
        
        shippingTimes = shippingTimes.findAll{it.value <= p99} //Saco envios raros con altos tiempos que deforman estadistica
        log("size: ${shippingTimes.size()}")
        log("${entry.key}: ${shippingTimes}")

        stats = new DescriptiveStatistics(shippingTimes*.value as double[])
        int estimated = entry.value.first().speed * 24
        def more = shippingTimes.findAll{(it.value as int) > estimated}.size() * 100 / shippingTimes.size()
        def less = shippingTimes.findAll{(it.value as int) <= estimated}.size() * 100 / shippingTimes.size()

        all << "${entry.key}:\t n:${stats.n}\t e: ${estimated}\t p: ${round(stats.mean)} d: ${round(stats.standardDeviation)} m: ${stats.min} M: ${stats.max}\t\t - less: ${round(less)}% - more: ${round(more)}%\n"

      } catch(Exception e) {
        log("ERROR2. Exception: $e")
      }
    }
    
  } catch(Exception e) {
      log("ERROR. Exception: $e")
  }
  

  //Functions
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

  def getWorkingHours(start, end, siteId) {

    def holidays = nonWorkingDays[siteId].collect { new Date(it) }.findAll { !(it.toCalendar()[Calendar.DAY_OF_WEEK] in [1, 7]) }
    def weekMap = [ 1:-1, 2:0, 3:1, 4:2, 5:3, 6:4, 7:-2 ]

    def w1 = weekMap[start.toCalendar()[Calendar.DAY_OF_WEEK]]
    def ww1 = w1>0?w1:0
    def c1 = start - w1
    
    def w2 = weekMap[end.toCalendar()[Calendar.DAY_OF_WEEK]]
    def ww2 = w2>0?w2:0
    def c2 = end - w2
    
    long days = c2 - c1
    long weekendDays = (days/7)*2
    long noWE = days-weekendDays
    long hol = holidays.findAll {it > start && it < end}.size()
    
    h1 = c1.toCalendar().get(Calendar.HOUR_OF_DAY)
    h2 = c2.toCalendar().get(Calendar.HOUR_OF_DAY)
    
    def hourDiff = c1 < c2 ? (-h1+h2) : (h1-h2)
  
    return ((noWE-ww1+ww2-hol)*24)+hourDiff
  }
