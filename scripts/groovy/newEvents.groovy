import mercadoenvios.*
  
def sm1 = ShippingMethods.get(100009)  
def ts1 = new TrackingStatus()
ts1.description = 'PI Aberta'
ts1.statusId = 'ND10'
ts1.shippingMethod = sm1
ts1.normalizedStatus = 'shipped'
ts1.substatus = null
ts1.serviceId = 21L
ts1.save()

def sm2 = ShippingMethods.get(182)  
def ts2 = new TrackingStatus()
ts2.description = 'PI Aberta'
ts2.statusId = 'ND10'
ts2.shippingMethod = sm2
ts2.normalizedStatus = 'shipped'
ts2.substatus = null
ts2.serviceId = 22L
ts2.save()

def sm3 = ShippingMethods.get(500145)  
def ts3 = new TrackingStatus()
ts3.description = 'PI Aberta'
ts3.statusId = 'ND10'
ts3.shippingMethod = sm3
ts3.normalizedStatus = 'shipped'
ts3.substatus = null
ts3.serviceId = 23L
ts3.save()
