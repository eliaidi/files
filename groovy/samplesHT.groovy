def redisHandlingTimeService = ctx.getBean('redisHandlingTimeService')

def users = [
    [id: 92607234,name:'DATA_COMPUTACION'],
    [id: 25704919,name:'SHOWS'],
    [id: 136165659,name:'PROSMARTS'],
    [id: 137339001,name:'CEL'],
    [id: 79715665,name:'CELUGADGETS'],
    [id: 4641264,name:'PHOTOSTORE'],
    [id: 75338949,name:'PLAZAPC'],
    [id: 716559,name:'DECOHOGAR'],
    [id: 4542417,name:'NECXUS_BAIRES'],
    [id: 20601524,name:'SIDARTA1'],
    [id: 85057345,name:'MULTIRRUBRO'],
    [id: 13262009,name:'MATAFUEGOSONLINE'],
    [id: 96338200,name:'SHOP2014'],
    [id: 120289637,name:'VENTAIMPORTACION'],
    [id: 82916233,name:'NEXXCOMPUTACION'],
    [id: 43275244,name:'CELULAR'],
    [id: 24853391,name:'HOMERO'],
    [id: 109089,name:'REAL'],
    [id: 4808670,name:'01ENLINEA']
]

users.each { user ->
	def samples = redisHandlingTimeService.lrange("HANDLING-TIME-SAMPLES-STATISTICS-${user.id}", 0, -1)
	def ht = redisHandlingTimeService.get("HANDLING-TIME-USER-${user.id}")

	println "----------------------------------------------------------"
	println "User: ${user.name}"
	println "ht: $ht"
	println "samples: $samples"
	println ""

	def statisticsParameters = getStatisticsParameters(samples)					
	if(statisticsParameters.totalSamplesSize >= 5) { 
		def handlingTime = Math.round(statisticsParameters.mean + ponderate(statisticsParameters.standarDesviation, statisticsParameters.ponderation).d1) ?: 1
		def handlingTime_2 = Math.round(statisticsParameters.mean + ponderate(statisticsParameters.standarDesviation, statisticsParameters.ponderation).d2) ?: 1
	    println "mean: ${statisticsParameters.mean}"
	    println "desv: ${statisticsParameters.standarDesviation}"
	    println "pond: ${statisticsParameters.ponderation}"
	    println "d.p1: ${ponderate(statisticsParameters.standarDesviation, statisticsParameters.ponderation).d1}"
	    println "d.p2: ${ponderate(statisticsParameters.standarDesviation, statisticsParameters.ponderation).d2}"			
		println "htd1: ${statisticsParameters.mean + ponderate(statisticsParameters.standarDesviation, statisticsParameters.ponderation).d1}"
		println "htd2: ${statisticsParameters.mean + ponderate(statisticsParameters.standarDesviation, statisticsParameters.ponderation).d2}"
		println "ht-1: ${handlingTime}"
		println "ht-2: ${handlingTime_2}"
	}
}

//----------------------------------------------------------------------------------------------
def ponderate(deviation, ponderation) {
	def ADJUSTMENT_FACTOR = 0.25d
    def ADJUSTMENT_FACTOR_2 = 0.15d
    def adjustedPonderation = ponderation + ADJUSTMENT_FACTOR
    def adjustedPonderation_2 = ponderation + ADJUSTMENT_FACTOR_2
	return [
      d1: adjustedPonderation < 1 ? deviation * adjustedPonderation : deviation,
      d2: adjustedPonderation_2 < 1 ? deviation * adjustedPonderation_2 : deviation
    ]
}

def getStatisticsParameters(def samples){
	def sampleSizeAcu = 0
	def sampleMeanAcu = 0
	def sampleVarianceAcu = 0
	def samplePonderationAcu = 0
	samples.each{ sample ->
		sampleSizeAcu += sample.qty
		sampleMeanAcu += sample.mean * sample.qty
		sampleVarianceAcu += sample.qty * (sample.variance + sample.mean)
		samplePonderationAcu += sample.qty * sample.ponderation
	}
	def mean = sampleMeanAcu/sampleSizeAcu
	def variance =  (sampleVarianceAcu/(sampleVarianceAcu ?: 1)) - mean
	def standarDesviation = Math.sqrt(Math.abs(variance))
	def ponderation = samplePonderationAcu/sampleSizeAcu
	return [
		mean: mean, 
		variance: variance, 
		standarDesviation: standarDesviation,
		totalSamplesSize : sampleSizeAcu,
		ponderation: ponderation
	]
}