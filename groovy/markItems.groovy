import grails.util.GrailsUtil
import groovyx.gpars.*
import static mercadoenvios.constants.ShippingConstants.*
import mercadoenvios.*

OFFSET = 0
LIMIT = 50

out = new File('/tmp/items.log'); out.write("")
itemService = ctx.getBean('itemService')
shippingModesService = ctx.getBean('shippingModesService')

def logInfo(String message) {
	out << message + "\n"
}

def itemHasValidStatus(item) {
	return !(item.status in [ITEM_CLOSED_STATUS, ITEM_INACTIVE_STATUS])
}

def getAllItemsUser(Long sellerId) {
	def result = []
	try {
	 	logInfo "Getting items for seller $sellerId"
		def itemsData = itemService.search(["seller_id": sellerId],OFFSET,LIMIT)
		if (itemsData) {
			def offset = 0
			while (itemsData) {
				result << itemsData
				offset += LIMIT
				itemsData = itemService.search(["seller_id": sellerId],offset,LIMIT)
			}
			result = result.flatten()
		} 
		return result
	} catch (Exception e) {
		logInfo "Error obteniendo los items del sellerId ${sellerId}"
		logInfo e.getMessage()
		throw new Exception(e)
	}
}

def markItem = { option, shippingMode, itemData ->
	
	def shippingModes = shippingModesService.getShippingModesData(itemData.seller_id, itemData.category_id)

	if(!(itemData?.shipping?.mode in [SHIPPING_MODE_ME2,SHIPPING_MODE_ME1])) {
		return shippingMode in shippingModes*.mode
	} 
	return false
}

def processUserItems(user) {
	//Busca items en mongo
	def items = getAllItemsUser(user.userId)
	logInfo "Items a procesar para ${user.userId}: ${items.size()}"

	GParsPool.withPool { 
		items.eachParallel{ item ->
			//Se excluiyen los items cuyas categorías no aceptan mode me2 o sean subastas o free listing			
			if(PUBLICATION_FREE != item.listing_type_id
				&& PUBLICATION_PAY_MODE != item.buying_mode
				&& itemHasValidStatus(item)) {
									
				if(markItem(user.option, user.shippingMode, item)) {
					
					if(item.shipping.free_shipping)
						logInfo "mark ${item.id} as free"
					else
						logInfo "mark ${item.id} (no free)"
				}
			}
		}
	}
}

//def users = Adoption.findAllByOptionAndStatus('in','processed')
def users = Adoption.findAllByUserId(138352574L)
users.each { user ->
	processUserItems(user)
}