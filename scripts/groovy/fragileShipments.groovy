import mercadoenvios.*

mongoService = ctx.getBean('mongoService')
redis = ctx.getBean('redisGeneralService')
out = new File('/tmp/shipments.log')

log = { text -> out << text + "\n" }

def CHUNK_SIZE = 500

List drop(List data, int size) {
    def output = []
    while (data && size--) {
        def elem = data.first()
        output << elem
        data.remove(elem)
    }
    return output
}

trustedUsers = redis.get('trusted_users')
log "trustedUsers: $trustedUsers"

def siteId = 'MLA'	

log "getting shipments for $siteId"
def shipments = Shipment.createCriteria().list(){
	eq "siteId", siteId
	eq 'shippingMode', 'me2'
	lt "dateCreated", new Date()
	gt "dateCreated", new Date()-34
}

log "total shipments: ${shipments.size()}"
def fragileShipments = []
try {
	def chunk = drop(shipments, CHUNK_SIZE)
	while (chunk) {
		log "remaining ${shipments.size()}.."
		chunk.each { ship ->
			if (mongoService.findOne('categories', [id: ship.categoryId]).settings.fragile)
                fragileShipments << ship
		}
		chunk = drop(shipments, CHUNK_SIZE)
	}
	
} catch (Exception e){
	log "Exception: $e"
}

fragileShipments.groupBy{it.status}.each{ status, ships ->
  log "$status: ${ships.size()}"
  ships.each{ s ->
    def isAllowed = trustedUsers.contains(s.senderId as Integer)
    log "${s.id} ${s.itemId} ${s.categoryId} ${s.senderId} ${s.dateCreated} ${s.status} -> ${isAllowed ? 'OK' : 'ERROR'}"
  }
}

log "done."