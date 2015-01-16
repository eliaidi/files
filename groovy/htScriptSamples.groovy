ADJUSTMENT_FACTOR = 0.25d

def samples = [
	[qty:111,mean:1.4954954,variance:0.45225224,ponderation:0.5945946],
	[qty:116,mean:0.8189655,variance:0.35824588,ponderation:0.10344828],
	[qty:83,mean:0.4698795,variance:0.39847192,ponderation:0.39759037],
	[qty:89,mean:1.0561798,variance:0.14453524,ponderation:0.101123594],
	[qty:70,mean:0.5,variance:0.2536232,ponderation:0],
	[qty:156,mean:1.2243589,variance:0.42030603,ponderation:0.34615386],
	[qty:154,mean:1.512987,variance:0.8004838,ponderation:0.13636364],
	[qty:141,mean:1.2695036,variance:1.0554205,ponderation:0.53900707],
	[qty:98,mean:1.2244898,variance:0.44393015,ponderation:0.35714287],
	[qty:73,mean:0.69863015,variance:0.54680365,ponderation:0.16438356],
	[qty:140,mean:1.8428571,variance:0.66577595,ponderation:0.21428572],
	[qty:384,mean:1.4609375,variance:0.5833265,ponderation:0.29427084],
	[qty:243,mean:2.3374486,variance:0.2245009,ponderation:0.33744857]
]

def statisticsParameters = getStatisticsParameters(samples)					
if(statisticsParameters.totalSamplesSize >= 5) { 
	def handlingTime = Math.round(statisticsParameters.mean + ponderate(statisticsParameters.standarDesviation, statisticsParameters.ponderation)) ?: 1
    println "m: ${statisticsParameters.mean}"
    println "d: ${statisticsParameters.standarDesviation}"
    println "p: ${statisticsParameters.ponderation}"
    println "pd: ${ponderate(statisticsParameters.standarDesviation, statisticsParameters.ponderation)}"			
	println "ht sin round: ${statisticsParameters.mean + ponderate(statisticsParameters.standarDesviation, statisticsParameters.ponderation)}"
	println "ht: ${handlingTime}"
}

def ponderate(deviation, ponderation) {
	def adjustedPonderation = ponderation + ADJUSTMENT_FACTOR
	return adjustedPonderation < 1 ? deviation * adjustedPonderation : deviation
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