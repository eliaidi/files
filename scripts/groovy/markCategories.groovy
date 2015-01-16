def mongoService = ctx.getBean('mongoService')

//MLA1002 y MLA5726 son las categorias padres de TV, LED, LCD y Plasmas
['MLA1002','MLA5726'].each { categId ->
	def result = mongoService.update(
		'categories',
		[children_categories:["\$size":0], path_from_root:["\$elemMatch":[id:categId]], 'settings.fragile':true],
		[ "\$set": [status: 'MARK_PENDING']]
	)
	println "${categId}: ${result.size()}" //Debe ser > 0
}