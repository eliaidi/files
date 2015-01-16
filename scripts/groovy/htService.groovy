import mercadoenvios.*
import grails.util.GrailsUtil;

import org.codehaus.groovy.grails.commons.ConfigurationHolder as CH;
import org.codehaus.groovy.grails.web.json.JSONArray;
import org.hibernate.FetchMode;

import mercadoenvios.constants.ShippingConstants;
import mercadoenvios.utils.JobUtil;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;


def redisHandlingTimeService = ctx.getBean('redisHandlingTimeService')

    def getSentShipments(Date from, Date to, siteId, userId){
		def shps = Shipment.withCriteria {
			isNotNull 'dateReadyToShip'
			eq 'shippingMode', 'me2'
			between 'dateShipped', from, to
			eq 'senderId', userId
		}
		return shps
	}
	
	def calcExpect(shipments, backup, siteId){
		//Se separan los envios en 4 grupos ( diferencia dias 0:0-4 , 1:5-9 , 2:10-14 y 3:15 )
		def groupedShipments = shipments.groupBy { (((new Date()-1) - it.dateShipped)/5) as Integer }.sort{it.key}
        println "en calcExpect - groupedShipments:"
        groupedShipments.each{ println "${it.key}:${it.value.size()}"}
		
		def ll = groupedShipments.collect {	k, l ->
			println "k ${k}"
			println "l.size ${l.size()}"
          
			def shipmentStats
			
			//Si hay mas de 5 envios, uso esos shipments
			if (l.size() > 5) {
				//Se da mas peso a los envios mas recientes
				def ponderatedShipments = l + ( l*(3-k) )
				def workingDaysInShipments = ponderatedShipments.collect {getWorkingDays(it.dateReadyToShip, it.dateShipped, siteId)} as double[]
				println "workingDaysInShipments ${workingDaysInShipments}"
				shipmentStats = new DescriptiveStatistics(workingDaysInShipments)
			
			} else { //Sino uso la suma del backup y los shipments que haya
				
				def workingDaysInBackup = backup?.collect {getWorkingDays(it.dateReadyToShip, it.dateShipped, siteId)} as double[]
				println "workingDaysInBackup ${workingDaysInBackup}"
				def wdInShipmentsAndBackup = []; wdInShipmentsAndBackup.addAll(workingDaysInShipments); wdInShipmentsAndBackup.addAll(workingDaysInBackup)
				println "wdInShipmentsAndBackup ${wdInShipmentsAndBackup}"
				shipmentStats = new DescriptiveStatistics(wdInShipmentsAndBackup as double[])
			}
			println "shipmentStats ${shipmentStats}"
			println "shipmentStats.variance ${shipmentStats.variance}"
			
			def r = [
				desde:		k,
				bestGuess: 	shipmentStats.mean + shipmentStats.variance
			]
			println "valor------>${r}"
			return r
	
		  }*.bestGuess.findAll{it}
		  println "ll ${ll}"
		 
		 def meanResult = new DescriptiveStatistics(ll as double[]).mean
		 println "meanResult: $meanResult"
		  ll? Math.round(meanResult) : null
	}
	
    def computeHandlingByUser(String siteId, userId) {
		
				//tomo los shipments de los últimos 15 días
				def shps = getSentShipments(new Date() - 16,new Date()-1, siteId, userId)
				//println "shipments ${shps}"
				//tomo los shipments de los penúltimos 15 días, por si no tengo de los últimos 15
				def shpsBackup = getSentShipments(new Date() - 15*2 -1,new Date() - 16, siteId, userId)
				//println "backup ${shpsBackup}" 
				
				//calculo para cada usuario el tiempo de handling
				def handlingTimeByUser = [:]
				shps.groupBy {it.senderId}.each{k,l ->
				  handlingTimeByUser[k]= calcExpect(l, shpsBackup.findAll{it.senderId==k}, siteId)
				}
				println "handlingTimeByUser ${handlingTimeByUser}"
								
			

    }
	
	def getNonWorkingDays(String siteId){
		def nonWorkingDaysList = CH.config.handlingTime.nonWorkingDays[siteId]
		//println "handling compute nonWorkingDaysList ${nonWorkingDaysList}"

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
		//println "workingDays: ${workingDays}"
		return workingDays
	}


	computeHandlingByUser('MLB', 82916233L)
	
