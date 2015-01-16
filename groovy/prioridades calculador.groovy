//------------------- Reglas de ejemplo ----------------------------------------------------------------------------

def rules = [
    [display: 'always',  cost: 8, speed: [shipping: 10], shippingMethod: [isPromoted: false]], //always
    [display: 'always',  cost: 1, speed: [shipping: 15], shippingMethod: [isPromoted: false]], //always
    [display: 'optional',cost: 4, speed: [shipping: 20], shippingMethod: [isPromoted: false]], //optional
    [display: 'always',  cost: 2, speed: [shipping: 7],  shippingMethod: [isPromoted: true]],  //promoted
    [display: 'always',  cost: 0, speed: [shipping: 10], shippingMethod: [isPromoted: false]], //free shipping
    [display: 'always',  cost: 2, speed: [shipping: 5],  shippingMethod: [isPromoted: true]],  //promoted
    [display: 'always',  cost: 5, speed: [shipping: 12], shippingMethod: [isPromoted: false]], //always
]

//------------------- Closures - configuración (Esto iria en Configurations.groovy) -------------------------------

def isPromoted =     {rule -> rule.shippingMethod.isPromoted}
def isOptional =     {rule -> rule.display == 'optional'}
def isFreeShipping = {rule -> !isPromoted(rule) && !isOptional(rule) && rule.cost == 0}
def isAlways =       {rule -> !isPromoted(rule) && !isOptional(rule) && rule.cost != 0}

//Define las prioridades en que se agrupan las reglas, ordenadas de acuerdo a cómo queremos mostrarlas
def priorities = [
    promoted: isPromoted, freeShipping: isFreeShipping, always: isAlways, optional: isOptional
]
//Define el orden que debe tener cada grupo de reglas
def sorting = [
    cost: {a,b -> a.cost <=> b.cost ?: a.speed.shipping <=> b.speed.shipping},
    speed: {a,b -> a.speed.shipping <=> b.speed.shipping ?: a.cost <=> b.cost}
]

//------------------- Logica del calculador (PriceCalculatorDSBaseService.getPreferenceMode()) --------------------

def rulesByPriority = [:] //Mapa con reglas agrupadas por prioridad
rules.each{ rule -> 
    //Busco la prioridad a la que pertenece la regla
    def priority = priorities.find{p -> p.value(rule)}
    
    //Guardo regla en mapa
    if (rulesByPriority[priority.key] == null)
        rulesByPriority[priority.key] = [rule]
    else
        rulesByPriority[priority.key] << rule
}
//Ordeno las reglas de cada grupo 
def sortedRules = priorities.collect{ p -> rulesByPriority[p.key].sort(sorting.cost) }.flatten()


println sortedRules //Solo para ver resultados en la consola