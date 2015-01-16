import mercadoenvios.* 
import mercadoenvios.constants.ShippingConstants

def ordersService = ctx.getBean("ordersService")
def feedOrder = ctx.getBean("messageProcessorOrdersService")
def out = new File('/tmp/ordersLog.txt')

def processOrderId(orderId,ordersService,feedOrder,out) {
	def shps
	def order
	def json
	def status
	def lastShipment

	try{
		Shipment.withTransaction { tx ->

			lastShipment = Shipment.createCriteria().list {
	        	eq 'orderId', orderId
	    	}.sort{-it.dateCreated.time}.first()

		    if (lastShipment){
		        out << "-----------------------------------------------------------\n"
		        order = ordersService.getOrdersData(orderId, lastShipment?.senderId)

		        if(order){
		            out << "order_id= ${order.id}\n"
		            out << "order_status= ${order.status}\n"
		            json = [id:order.id , status:order.status, order_items:order.order_items]
		          
		            if (order.status == ShippingConstants.ORDERS_PAID_STATUS && ShippingConstants.SHIP_STATUS_PENDING == lastShipment.status) {
						if (feedOrder.processOrder(json, lastShipment)) {
			            	out << "Se procesó la orderId. ${orderId}\n"
			            } else {
			            	tx.setRollbackOnly()
			            	out << "No se pudo procesar la orderId. ${orderId}\n"
			            }
		            } else {
		            	out << "Se descartó esta orderId = ${orderId} - satus: ${order.status}\n"
		            }
		        } else {
		            out << "no existe la orderId\n"
		        }
		        out << "-----------------------------------------------------------\n"
		    }
		}
	    
    } catch (Exception e){
		out << "Exception ${e}\n"
	}
}

def orderDir = new File('/tmp/orders/')

orderDir.listFiles().each { file ->
    
  if (file.isFile()) {
    
    out << "Processing file ${file.getName()}\n"
    
    file.eachLine{ orderId ->
      
        out << "Processing orderId ${orderId}\n"
        processOrderId(orderId,ordersService,feedOrder,out)
        
    }
 
    file.renameTo(new File('/tmp/orders/procesados/', file.getName()))
    
    out << "End processing file ${file.getName()}\n"
  } 
  
}




















/*import mercadoenvios.*
  
def mapa = [
  'MLB': ctx.getBean("rulesMongoMLBService"),
  'MLA': ctx.getBean("rulesMongoMLAService")
  ]
  


def orderIds = ['787720755'] //,'787720839','787720920','787720956','787721271','787721316','787721595','787721637','787722009','787722630','787722888','787722921','787723257','787723629','787724145','787724196','787724235','787724325','787724334','787724592','787724721','787724826','787724922','787725102','787725243','787725321','787725465','787725639','787725672','787725759','787725780','787725951','787726005','787726122','787726179','787726227','787726239','787726245','787726257','787726365','787726545','787726851','787727178','787727271','787727589','787728507','787728543','787728606','787728846','787729017','787729149','787729230','787729707','787730058','787730082','787730139','787730196','787730397','787730478','787730595','787730613','787730853','787730865','787730904','787731018','787731078','787731081','787731171','787731234','787731312','787731342','787731372','787731582','787731627','787731753','787731783','787732140','787732155','787732254','787732602','787732617','787732680','787732941','787733211','787733490','787733535','787733724','787733859','787733910','787733937','787733991','787734120','787734411','787734525','787734765','787734777','787735245','787735470','787735680','787735803','787735899','787735965','787736145','787736148','787736202','787736772','787736775','787736814','787736991','787736994','787737006','787737051','787737066','787737147','787737249']

orderIds.each{ orderId ->
 
  def shipment = Shipment.findByOrderId(orderId)
  
  def result = mapa[shipment.siteId].getRuleMethod(shipment.appliedShippingRuleId)
  
  println "result $result"
}  */





/*Shipment.withTransaction{ tx ->
	try {

		lastShipment = Shipment.createCriteria().list {
			eq 'orderId', orderId.toString()
			order 'dateCreated', 'desc'
		}.first()
		
	} catch (Exception e) {
		log.debug "Shipment not found for order ${json.id}"
	}



	status = processOrder(json, lastShipment)

	}
}*/