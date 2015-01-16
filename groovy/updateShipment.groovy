import mercadoenvios.Shipment

['916351281'].each {
	def s = Shipment.findByOrderId(it)
	println "id: ${s.id}"
	println "st: ${s.status}"
	println "su: ${s.substatus}"
	println "dr: ${s.dateReadyToShip}"
	println "ds: ${s.dateShipped}"
	println "dd: ${s.dateDelivered}"
	println "dn: ${s.dateNotDelivered}"

	//s.dateShipped = new Date(s.dateReadyToShip.getTime() + 1000*60)
	s.status = 'delivered'
	s.dateDelivered = new Date()
	//s.status = 'not_delivered'
	//s.dateNotDelivered = new Date()

	println "st: ${s.status}"
	println "su: ${s.substatus}"
	println "dr: ${s.dateReadyToShip}"
	println "ds: ${s.dateShipped}"
	println "dd: ${s.dateDelivered}"
	println "dn: ${s.dateNotDelivered}"
}