package mercadoenvios

import grails.util.GrailsUtil;

import org.codehaus.groovy.grails.commons.ConfigurationHolder as CH;
import org.codehaus.groovy.grails.web.json.JSONArray;
import org.hibernate.FetchMode;

import mercadoenvios.constants.ShippingConstants;
import mercadoenvios.utils.JobUtil;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;


def redisHandlingTimeService = ctx.getBean('redisHandlingTimeService')


def getNonWorkingDays(String siteId){
	def nonWorkingDaysList = CH.config.handlingTime.nonWorkingDays[siteId]
	println "handling compute nonWorkingDaysList ${nonWorkingDaysList}"

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

	//end Saturday to start Saturday
	long days = resultantDayOfWeekTo - resultantDayOfWeekFrom
	long weekendDays = (days/7)*2
	long noWE = days-weekendDays
	
	def nonWorkingDays = getNonWorkingDays(siteId)
	long hollydays = nonWorkingDays.findAll {it > start && it < end}.size()

	def workingDays = noWE-differenceForDayOfWeekFrom+differenceForDayOfWeekTo-hollydays		
	println "workingDays: ${workingDays}"
	return workingDays
}
	
def calcExpect(shipments, backup, siteId){
		//Se separan los envios en 4 grupos ( diferencia dias 0:0-4 , 1:5-9 , 2:10-14 y 3:15 )
		def groupedShipments = shipments.groupBy { ((new Date() - it.dateShipped)/5) as Integer }
		println "en calcExpect - groupedShipments ${groupedShipments}"
		
		def ll = groupedShipments.collect {	k, l ->
			println "k ${k}"
			println "l ${l}"

			//Se da mas peso a los envios mas recientes
			def ponderatedShipments = l + ( l*(3-k) )
			def workingDaysInShipments = ponderatedShipments.collect {getWorkingDays(it.dateReadyToShip, it.dateShipped, siteId)} as double[]
			println "workingDaysInShipments ${workingDaysInShipments}"
			
			def shipmentStats
			def daysInShipments

			//Si hay mas de 5 envios, uso esos shipments
			if (l.size() > 5) {
				shipmentStats = new DescriptiveStatistics(workingDaysInShipments)
				daysInShipments = workingDaysInShipments

			} else { //Sino uso la suma del backup y los shipments que haya
				
				def workingDaysInBackup = backup?.collect {getWorkingDays(it.dateReadyToShip, it.dateShipped, siteId)} as double[]
				println "workingDaysInBackup ${workingDaysInBackup}"

				def wdInShipmentsAndBackup = []; wdInShipmentsAndBackup.addAll(workingDaysInShipments); wdInShipmentsAndBackup.addAll(workingDaysInBackup)
				println "wdInShipmentsAndBackup ${wdInShipmentsAndBackup}"

				if ((l + backup).size() > 5) {
					shipmentStats = new DescriptiveStatistics(wdInShipmentsAndBackup as double[])
				
					daysInShipments = wdInShipmentsAndBackup
				}
				
			}

			def updatedShipmentStats
			if (shipmentStats) {
				def p80 = shipmentStats.getPercentile(80d)
				println "percentile $p80"

				updatedShipmentStats = new DescriptiveStatistics(daysInShipments.findAll{it <= p80} as double[])
				println "shipmentStats ${updatedShipmentStats}"
				println "shipmentStats.variance ${updatedShipmentStats.variance}"	
			}
						
			def r = [
				desde:		k,
				bestGuess: 	((updatedShipmentStats?.mean ?: 0) + (updatedShipmentStats?.variance ?: 0) ?: null)
			]
			println "valor------>${r}"
			return r
	
		  }*.bestGuess.findAll{it}
		  println "ll ${ll}"
		 
		  ll? Math.round(new DescriptiveStatistics(ll as double[]).mean) : null
	}

def getSentShipments(Date from, Date to, siteId){
	def shps = Shipment.withCriteria {
		isNotNull 'dateReadyToShip'
		eq 'shippingMode', 'me2'
		between 'dateShipped', from, to
		shippingMethod{
			eq 'siteId', siteId
		}
		//'in' 'senderId' [79014643L,72028998L,98814106L,76147858L,85017962L]
		eq 'senderId', 76147858L
	}
	return shps
}

def computeHandlingByUser(String siteId) {
	try{
			//tomo los shipments de los últimos 15 días
			def shps = getSentShipments(new Date() - 15,new Date(), siteId)
			println "shipments ${shps}"
			//tomo los shipments de los penúltimos 15 días, por si no tengo de los últimos 15
			def shpsBackup = getSentShipments(new Date() - 15*2,new Date() - 15, siteId)
			println "backup ${shpsBackup}" 
			
			//calculo para cada usuario el tiempo de handling
			def handlingTimeByUser = [:]
			shps.groupBy {it.senderId}.each{k,l ->
			  handlingTimeByUser[k]= calcExpect(l, shpsBackup.findAll{it.senderId==k}, siteId)
			}
			println "handlingTimeByUser ${handlingTimeByUser}"
			
		} catch(Exception e){
			println "Error al procesar job de handling time para site: ${siteId}. Error: ${e}"
		}

}

computeHandlingByUser('MLA')