import static groovyx.gpars.GParsPool.withPool

mongoService = ctx.getBean('mongoService')

zipCodesBA = new File('/tmp/CP-BA.csv').text.split('\\n').collect{it as Integer}

prMethod = 73330l
stMethod = 73328l

collection = 'rulesMLA'

out = new File('/tmp/script.out')
lock = new Object()

def log(def l) {
	synchronized(lock) {
		out << l + '\n'
	}
}

def updateRule(def zcf, def zct, def methodId, def speed) {
	try {
		mongoService.basicUpdate(collection,['from.zip_code':zcf, 'to.zip_code':zct, method_id: methodId],[$set:[speed:speed, last_updated: new Date()], $inc:[version:1]], false, false)
	} catch (Exception e) {
		log "Failed update ${methodId} for ${zcf} ${zct}"
	}
}

log "Starting BA-BA operation"	
withPool() {
	zipCodesBA.eachParallel { zipFrom ->
		log "Processing BA $zipFrom"
		zipCodesBA.each { zipTo -> 
			updateRule(zipFrom, zipTo, prMethod, 2)
			updateRule(zipFrom, zipTo, stMethod, 4)
		}
	}
}
log "BA-BA operation finished"
