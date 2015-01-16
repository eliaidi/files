import grails.util.GrailsUtil
import org.codehaus.groovy.grails.commons.ConfigurationHolder as CH
import org.codehaus.groovy.grails.web.json.JSONArray
import org.hibernate.FetchMode
import mercadoenvios.Shipment
import mercadoenvios.constants.ShippingConstants
import mercadoenvios.utils.JobUtil
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
  
out = new File('/tmp/out.log'); out.write("")
log = { text -> out << text + "\n" }
logCSV = { file, text -> file << text + "\n" }
calculatorHolidayService = ctx.getBean('calculatorHolidayService')
  
def getSentShipments(def from, def to, def siteId) {		
	def shps = Shipment.withCriteria {
      between 'dateCreated', from, to
      isNotNull 'dateReadyToShip'
	  isNotNull 'dateShipped'
      eq 'shippingMode', 'me2'
      shippingMethod {
        eq 'siteId', siteId
      }
      ne 'shippingService.id', 81L
	  ne 'marketplace', 'MELI_NONE'
      'in' 'senderId', [92607234L] //MLA
    }
    return shps
}

def getWorkingDays(Date start, Date end, String siteId){
	def workingWeekMap = [ 1:-1, 2:0, 3:1, 4:2, 5:3, 6:4, 7:-2 ]
	
	def dayOfWeekFrom = workingWeekMap[start.toCalendar()[Calendar.DAY_OF_WEEK]]
	def differenceForDayOfWeekFrom = dayOfWeekFrom>0?dayOfWeekFrom:0
	def resultantDayOfWeekFrom = start - dayOfWeekFrom

	def dayOfWeekTo = workingWeekMap[end.toCalendar()[Calendar.DAY_OF_WEEK]]
	def differenceForDayOfWeekTo = dayOfWeekTo>0?dayOfWeekTo:0
	def resultantDayOfWeekTo = end - dayOfWeekTo

	//end Saturday to start Saturday
	long days = resultantDayOfWeekTo - resultantDayOfWeekFrom
	long weekendDays = (days/7)*2
	long noWE = days-weekendDays
	
	def nonWorkingDays = getNonWorkingDays(siteId)
	long hollydays = nonWorkingDays.findAll {it > start && it < end}.size()

	def workingDays = noWE-differenceForDayOfWeekFrom+differenceForDayOfWeekTo-hollydays		
	//log.debug "workingDays: ${workingDays}"
	return workingDays
}

def getNonWorkingDays(String siteId) {
	def nonWorkingDaysList = CH.config.handlingTime.nonWorkingDays[siteId]
	//log.debug "handling compute nonWorkingDaysList ${nonWorkingDaysList}"

	def nonWorkingDays = nonWorkingDaysList.collect { new Date(it) }.findAll { !(it.toCalendar()[Calendar.DAY_OF_WEEK] in [1, 7]) }
	//log.debug "nonWorkingDays ${nonWorkingDays}"
	return nonWorkingDays
}

def calcExpect(shipments, siteId){
	def descStats = getDescriptiveStatistics(shipments, siteId)
	def workingDaysInShipments = descStats.stats	
	def wdis90 = descStats.stats90
	def ADJUSTMENT_FACTOR = 0.25d
	def ponderate = descStats.above.size() / descStats.stats90.n + ADJUSTMENT_FACTOR
	ponderate = ponderate < 1d ? ponderate : 1d
	[
		 'qty':wdis90.n, 
		 'mean':wdis90.mean,
		 'deviation':wdis90.standardDeviation,
		 'ponderate':ponderate
	] 
}

def getDescriptiveStatistics(def shipments, def siteId){
	def data = shipments.collect {
		def realDateShipped = calculateRealDateShipped(it.dateShipped, siteId)
		it.dateReadyToShip < realDateShipped ? getWorkingDays(it.dateReadyToShip, realDateShipped, siteId) : 0		
	} as double[]
	def outCSV = new File("/tmp/ht-${shipments.first().senderId}.csv"); outCSV.write("")
	data.each { logCSV outCSV, "${it as int}" }
	
	def stats = new DescriptiveStatistics(data)
	
	def data90 = data.findAll{it <= stats.getPercentile(90)} as double[]
	def stats90 = new DescriptiveStatistics(data90)
	
	def above = data90.findAll{it > Math.round(stats90.mean)}

	return [stats: stats, stats90: stats90, above: above]
}

//Si el dateShipped es de madrugada, devuelve el día anterior; para no perjudicar y sumar 1 dia de HT al envío
def calculateRealDateShipped(def dateShipped, siteId) {
	int shippedHour = dateShipped.format('HH') as int
	
	Calendar realDateShipped = Calendar.getInstance()
	if (shippedHour >= 0 && shippedHour < 8) {
		realDateShipped.setTime(dateShipped - 1)
		while (calculatorHolidayService.isHoliday(realDateShipped, siteId) || realDateShipped.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY)
			realDateShipped.add(Calendar.DATE, -1)
	} else
		realDateShipped.setTime(dateShipped)
	
	return realDateShipped.getTime()
}

def siteId = 'MLA'
log "Getting shipments"
def shps = getSentShipments(new Date()-30, new Date(), siteId)
log "Processing"
shps.groupBy {it.senderId}.each{ senderId, shipments ->
	def partialStatistics = calcExpect(shipments, siteId)
	if (partialStatistics) {
		def htMeanDevPercent3 = Math.round(partialStatistics.mean + (partialStatistics.deviation * partialStatistics.ponderate))
		log "${senderId}: $partialStatistics - $htMeanDevPercent3"
	} else
		log "${senderId}: no stats"
}
log "Done"