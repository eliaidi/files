import grails.converters.JSON
import mercadoenvios.Shipment

out = new File('/tmp/out.log'); out.write('') //Archivo de log
log = { text -> out << text + "\n" }

ordersService = ctx.getBean('ordersService')

new File('/tmp/notProc.txt').eachLine { line ->
  
  def ship = Shipment.get(line)
  def order = ordersService.getOrdersData(ship.orderId, ship.senderId)
  if (order?.status != 'paid' && !order.mediations && ship?.status == 'shipped')
    log "${ship.orderId}"
}
log "end."