import mercadoenvios.*
  
def mongo = ctx.getBean('mongoService')
out = new File('/tmp/out.log')
out16 = new File('/tmp/_1608.csv'); out16.write('')
out17 = new File('/tmp/_1708.csv'); out17.write('')
  
addLine = { text, file -> file << text + "\n" }
log = { text -> out << text + "\n" }
getDate = { date -> new Date(date.getTime()+(1000*60*60)).format('dd/MM/yyyy HH:mm:ss') }

log "Processing 16/08.."
def _1608 = mongo.find('novaduqueShipmentLocks',[
  $and:[
    [date_created:[$gte: Date.parse('ddMMyy HHmmss','150814 230000')]],
    [date_created:[$lte: Date.parse('ddMMyy HHmmss','160814 230000')]]
  ]
])

def _16 = _1608.sort{it.date_created}
_16.each{
  def shipment = Shipment.get(it.shipping_id as long)
  addLine("${shipment.trackingNumber},${getDate(it.date_created)}", out16)
}

log "Processing 17/08.."
def _1708 = mongo.find('novaduqueShipmentLocks',[
  $and:[
    [date_created:[$gte: Date.parse('ddMMyy HHmmss','160814 230000')]],
    [date_created:[$lte: Date.parse('ddMMyy HHmmss','170814 230000')]]
  ]
])

def _17 = _1708.sort{it.date_created}
_17.each{
  def shipment = Shipment.get(it.shipping_id as long)
  addLine("${shipment.trackingNumber},${getDate(it.date_created)}", out17)
}

log "End."