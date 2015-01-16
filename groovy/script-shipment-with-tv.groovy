import mercadoenvios.*

allowed = [
'MLB119300','MLB119301','MLB119299','MLB119296','MLB119292','MLB119297','MLB119302','MLB119295','MLB119294','MLB119293','MLB119298','MLB119311','MLB119312',
'MLB119310','MLB119307','MLB119303','MLB119308','MLB119313','MLB119306','MLB119305','MLB119304','MLB119309','MLB119366','MLB119367','MLB119365','MLB119362',
'MLB119358','MLB119363','MLB119368','MLB119361','MLB119360','MLB119359','MLB119364','MLB119429','MLB119430','MLB119428','MLB119425','MLB119433','MLB119421',
'MLB119426','MLB119431','MLB119424','MLB119423','MLB119432','MLB119422','MLB119427','MLB119442','MLB119443','MLB119441','MLB119438','MLB119446','MLB119434',
'MLB119439','MLB119444','MLB119437','MLB119436','MLB119445','MLB119435','MLB119440','MLB119455','MLB119456','MLB119454','MLB119451','MLB119459','MLB119447',
'MLB119452','MLB119457','MLB119450','MLB119449','MLB119458','MLB119448','MLB119453','MLB119468','MLB119469','MLB119467','MLB119464','MLB119460','MLB119465',
'MLB119470','MLB119463','MLB119462','MLB119461','MLB119466','MLB119534','MLB119535','MLB119533','MLB119530','MLB119526','MLB119531','MLB119536','MLB119529',
'MLB119528','MLB119527','MLB119532','MLB119545','MLB119546','MLB119544','MLB119541','MLB119537','MLB119542','MLB119547','MLB119540','MLB119539','MLB119538',
'MLB119543','MLB119611','MLB119612','MLB119610','MLB119607','MLB119603','MLB119608','MLB119613','MLB119606','MLB119605','MLB119604','MLB119609'
]
rootCategories = ['MLA': ['MLA1002','MLA5726'], 'MLB': []]

def siteId = 'MLA'
rootCategIds = rootCategories[siteId]

categService = ctx.getBean('categoriesService')
redis = ctx.getBean('redisGeneralService')
out = new File('/tmp/shipments.log')

log = { text -> out << text + "\n" }

trustedUsers = redis.get('trusted_users')
log "trustedUsers: $trustedUsers"

def getCategory(categId) {
	sleep 100
	return categService.getCategory(categId)
}

boolean shipInCategWithTv(category){
	boolean isShipInTv = false
	category.path_from_root.each{categ ->
			if (!(categ.id in rootCategIds) && categ.path_from_root && categ.path_from_root.size() > 1){
				shipInCategWithTv(getCategory(categ.id))
			} else {
				if (categ.id in rootCategIds){
					isShipInTv = true
				}
				return
			}
		}
	return isShipInTv
}

def processShipments(shipments, siteId, total){
	def count = 0
	shipments.each {ship ->
		//log "Check shipmentId = ${ship.id} - categoryId = ${ship.categoryId}"
		def dataCateg = getCategory(ship.categoryId)
		switch (siteId) {
			case 'MLA': 
				if (shipInCategWithTv(dataCateg)) {
					def isOK = trustedUsers.contains(ship.senderId as Integer)
					log "${ship.id} ${ship.categoryId} ${ship.senderId} -> ${isOK ? 'OK' : 'ERROR'}"
				}
				break

			case 'MLB':
				if (shipInCategWithTv(dataCateg) && !allowed.contains(ship.categoryId))
					log "Found! Follow this ${ship.id} ${ship.categoryId}"	
				break
		}
		
		if (++count%1000 == 0)
          log "$count of $total"
	}
}

log "getting shipments"
def shipments = Shipment.createCriteria().list(){
	eq "siteId", siteId
	lt "dateCreated", new Date()
	gt "dateCreated", new Date()-4
}

def total = shipments.size()
log "shipments = ${total}"
try {
	log "processing $siteId"
	processShipments(shipments, siteId, total)
} catch (Exception e){
	log "Exception: $e"
}
log "done."