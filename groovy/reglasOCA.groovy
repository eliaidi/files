import static groovyx.gpars.GParsPool.withPool


mongoService = ctx.getBean('mongoService')

ba = new File('/tmp/CP-BA.csv').text.split('\\n').collect{it as Integer}
interior = new File('/tmp/CP-interior.csv').text.split('\\n').collect{it as Integer}

prMethod = 73330l
stMethod = 73328l

collection = 'rulesMLASplit'
delCollection = 'rulesOCABackup'

out = new File('/tmp/script.out')
lock = new Object()

def log(def l) {
	synchronized(lock) {
		out << l + '\n'
	}
}

def removeRule(def zcf, def zct, def methodId) {
	try {
		def toDelete = mongoService.findOne(collection,['from.zip_code':zcf, 'to.zip_code':zct, method_id: methodId])
		assert toDelete != null
		toDelete.date_created = new Date
		toDelete.last_updated = new Date
		toDelete.version += 1
		mongoService.basicUpdate(delCollection, ['from.zip_code':zcf, 'to.zip_code':zct, method_id: methodId], toDelete, true, false)
		mongoService.remove(collection,['from.zip_code':zcf, 'to.zip_code':zct, method_id: methodId])
	} catch (Exception e) {
		log "Failed remove ${methodId} for ${b} ${i}"
	}
}

def updateRule(def zcf, def zct, def methodId, def speed) {
	try {
		mongoService.basicUpdate(collection,['from.zip_code':zcf, 'to.zip_code':zct, method_id: methodId],[$set:[speed:speed, last_updated: new Date()], $inc:[version:1]], false, false)
	} catch (Exception e) {
		log "Failed update ${methodId} for ${b} ${i}"
	}
}

assert interior.intersect(ba).isEmpty()

log "Starting BA-INT operation"
withPool() {
	ba.eachParallel { b ->
		log "Processing BA $b"
		interior.each { i ->
			removeRule(b,i,prMethod)
			updateRule(b,i,stMethod,5)
		}		
	}
}
log "BA-INT operation finished"

log "Starting BA-BA operation"	
withPool() {
	ba.eachParallel { b ->
		log "Processing BA $b"
		ba.each { i -> 
			updateRule(b,i,prMethod,3)
			updateRule(b,i,stMethod,5)
		}
	}
}
log "BA-BA operation finished"
