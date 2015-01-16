import mercadoenvios.*
  
import grails.util.GrailsUtil;
import org.codehaus.groovy.grails.commons.ConfigurationHolder as CH;
import org.codehaus.groovy.grails.web.json.JSONArray;
import org.hibernate.FetchMode;
import mercadoenvios.constants.ShippingConstants;
import mercadoenvios.utils.JobUtil;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;


def computeHandlingTimeService = new ComputeHandlingTimeService()  
def dateShipped = new GregorianCalendar(2013, Calendar.APRIL, 23, 0, 0, 0).getTime()
def siteId = 'MLB'

/********************* Last shipments *************************/
def shipments = Shipment.withCriteria {
	isNotNull 'dateReadyToShip'
	eq 'shippingMode', 'me2'
	gt 'dateShipped', dateShipped
	shippingMethod {
		eq 'siteId', siteId
	}
}
//println "shipments found: ${shipments.size()}"

def realHandlingTimeByUser = [:]
shipments.groupBy{it.senderId}.each { userId, shipList ->	
  def handlingTimes = []
  shipList.each{ shp ->
	  def handlingTime = computeHandlingTimeService.getWorkingDays(shp.dateReadyToShip, shp.dateShipped, siteId)
	  handlingTimes.add(handlingTime)
  }
  realHandlingTimeByUser[userId] = handlingTimes
}
realHandlingTimeByUser = realHandlingTimeByUser.findAll{it.value.size > 10}
//println "realHandlingTimeByUser: ${realHandlingTimeByUser.sort{it.key}}"

/********************* Statistics *************************/
def shps = getSentShipments(new Date() - 15,new Date(), siteId)
def shpsBackup = getSentShipments(new Date() - 15*2,new Date() - 15, siteId)

def handlingTimeByUser = [:]
shps.groupBy {it.senderId}.each{ userId, shipList ->
	if (userId in realHandlingTimeByUser.keySet())
  		handlingTimeByUser[userId]= calcExpect(shipList, shpsBackup.findAll{it.senderId == userId}, siteId)
}
//println "handlingTimeByUser: ${handlingTimeByUser.sort{it.key}}"

/********************* Comparison *************************/
realHandlingTimeByUser.each{ userId, handlingTimes ->
	println "User: ${userId} - HandlingTimes: ${handlingTimes} - HT: ${handlingTimeByUser[userId]}"
}

/********************* Functions *************************/
def getNonWorkingDays(String siteId){
	def nonWorkingDaysList = CH.config.handlingTime.nonWorkingDays[siteId]
	def nonWorkingDays = nonWorkingDaysList.collect { new Date(it) }.findAll { !(it.toCalendar()[Calendar.DAY_OF_WEEK] in [1, 7]) }
	return nonWorkingDays
}

def getWorkingDays(Date start, Date end, String siteId){
	def workingWeekMap = [ 1:-1, 2:0, 3:1, 4:2, 5:3, 6:4, 7:-2 ]
	def dayOfWeekFrom = workingWeekMap[start.toCalendar()[Calendar.DAY_OF_WEEK]]
	def differenceForDayOfWeekFrom = dayOfWeekFrom>0?dayOfWeekFrom:0
	def resultantDayOfWeekFrom = start - dayOfWeekFrom
	def dayOfWeekTo = workingWeekMap[end.toCalendar()[Calendar.DAY_OF_WEEK]]
	def differenceForDayOfWeekTo = dayOfWeekTo>0?dayOfWeekTo:0
	def resultantDayOfWeekTo = end - dayOfWeekTo
	long days = resultantDayOfWeekTo - resultantDayOfWeekFrom
	long weekendDays = (days/7)*2
	long noWE = days-weekendDays	
	def nonWorkingDays = getNonWorkingDays(siteId)
	long hollydays = nonWorkingDays.findAll {it > start && it < end}.size()
	def workingDays = noWE-differenceForDayOfWeekFrom+differenceForDayOfWeekTo-hollydays		
	return workingDays
}

def getSentShipments(Date from, Date to, siteId){
	def shps = Shipment.withCriteria {
		isNotNull 'dateReadyToShip'
		eq 'shippingMode', 'me2'
		between 'dateShipped', from, to
		shippingMethod{
			eq 'siteId', siteId
		}
	}
	return shps
}

def calcExpect(shipments, backup, siteId){
	def groupedShipments = shipments.groupBy { ((new Date() - it.dateShipped)/5) as Integer }
	
	def ll = groupedShipments.collect {	k, l ->
		def shipmentStats
		def workingDaysInShipments = l.collect {getWorkingDays(it.dateReadyToShip, it.dateShipped, siteId)} as double[]
		
		if (workingDaysInShipments.size() > 5) {
			shipmentStats = new DescriptiveStatistics(workingDaysInShipments)
		
		} else { //Sino uso la suma del backup y los shipments que haya
			
			def workingDaysInBackup = backup?.collect {getWorkingDays(it.dateReadyToShip, it.dateShipped, siteId)} as double[]
			def wdInShipmentsAndBackup = []; wdInShipmentsAndBackup.addAll(workingDaysInShipments); wdInShipmentsAndBackup.addAll(workingDaysInBackup)
			shipmentStats = new DescriptiveStatistics(wdInShipmentsAndBackup as double[])
		}
		
		def r = [
			desde:		k,
			bestGuess: 	shipmentStats.mean + shipmentStats.variance
		]
		return r
	  }*.bestGuess.findAll{it}
	  ll? Math.round(new DescriptiveStatistics(ll as double[]).mean) : null
}
