
def rulesLoaderCorreiosCityService = ctx.getBean('rulesLoaderCorreiosCityService')

//Rutas entre estados mas usadas en los ultimos 25 dias
def states = [['BR-SP','BR-SP'],['BR-SP','BR-MG'],['BR-MG','BR-SP'],['BR-SP','BR-RJ'],['BR-RJ','BR-SP'],['BR-SP','BR-RS'],['BR-RS','BR-SP'],['BR-PR','BR-SP'],['BR-SP','BR-PR'],['BR-RJ','BR-RJ']]

rulesLoaderCorreiosCityService.process(states, [500145l])


//2da corrida 10/09
//Rutas entre estados mas usadas en los ultimos 6 dias
def states = [['BR-PR','BR-SP'],['BR-SP','BR-RS'],['BR-RS','BR-SP']]


//3ra corrida 12/09
//Se vuelve a correr lo mismo
def states = [['BR-PR','BR-SP'],['BR-SP','BR-RS'],['BR-RS','BR-SP']]