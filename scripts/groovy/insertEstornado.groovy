import mercadoenvios.*
import org.hibernate.FetchMode as FM

correiosDS2TrackingProcessorService = ctx.getBean('correiosDS2TrackingProcessorService')

def event = [
  description:'Favor desconsiderar a informacao anterior',
  code:'EST09', 
  date:'2014-06-26T14:41:00-04:00',
  country_id:'BR', 
  comments: [
    agency:'', 
    code:'', 
    description:''
  ], 
  tracking_number:'PD764978005BR', 
  service_id:21
]

def shipment = Shipment.createCriteria().get {
            eq 'trackingNumber', 'PD764978005BR'
            eq 'shippingMode', 'me2'
            fetchMode 'trackings', FM.JOIN
        }

def result = correiosDS2TrackingProcessorService.processTrackingInfo(shipment, event)

println "result: $result"