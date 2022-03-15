let simulation, svg, info, transform
let pos = 0
let poll = null
let clicked = {}
const graph = {}
const stash = new Map()

const color = d3.scaleOrdinal(d3.schemeCategory20),
	uuid = window.location.pathname.substr(window.location.pathname.length - 36)

function export_svg(){
	let e = document.createElement('script')
	e.setAttribute('src', '/static/svg-crowbar.js')
	e.setAttribute('class', 'svg-crowbar')
	document.body.appendChild(e)
}

function sortObject(obj) {
	if(obj === null || obj.constructor !== Object)
		return obj
	let tmp = {}
	let keys = []
	for(let key in obj)
		keys.push(key)
	keys.sort()
	for(let index in keys)
		tmp[keys[index]] = sortObject(obj[keys[index]])
	return tmp
}

function sortTopObject(obj, preload) {
	let tmp = {}
	let keys = []
	for(let index in preload)
		tmp[preload[index]] = sortObject(obj[preload[index]])
	for(let key in obj)
		if(!(key in tmp))
			keys.push(key)
	keys.sort()
	for(let index in keys)
		tmp[keys[index]] = sortObject(obj[keys[index]])
	return tmp
}

function restart_sim(){
	simulation.alpha(1).restart()
	if(poll === null)
		poll = window.setTimeout(updategraph, 1000)
}

function halt_sim(){
	simulation.stop()
	if(poll !== null){
		window.clearTimeout(poll)
		poll = null
	}
}

function updategraph(){
	let qs = window.location.search
	if (qs == "?" || qs == ""){
		qs = "?pos=" + pos
	}else{
		qs += "&pos=" + pos
	}
	d3.json("/lg/delta/" + uuid + qs, makegraph)
}

function makegraph(error, updates) {
	if(error){
		poll = window.setTimeout(updategraph, 1000)
		return
	}
	let header, tags = new Map()
	for(const row of updates){
		// first row is a header object
		if(header === undefined){
			if(pos === row.pos){
				poll = window.setTimeout(updategraph, 1000)
				return
			}
			header = row
			for(const [i,tag] of header.tags.entries())
				tags.set(tag, 1<<i)
			continue
		}
		const [bits, data] = row
		if(bits === 0){
			graph.meta = data
		}else{
			let v = stash.get(data.ID)
			if(v === undefined)
				stash.set(data.ID, v={})
			v.bits = bits
			v.data = data
		}
	}
	if(simulation !== undefined)
		simulation.stop()
	svg.selectAll("*").remove()
	pos = graph.pos = header.pos
	graph.nodes = []
	graph.edges = []
	graph.max_nc = 0
	const Node = tags.get('Node')
	const Edge = tags.get('Edge')
	const Filter = tags.get('filter') || 0
	const Mark = tags.get('mark') || 0
	for(let [k,v] of stash.entries()){
		v.filter = v.bits & Filter
		v.mark = v.bits & Mark
		if(v.bits & Node){
			v.neighbors = {}
			v.ln = {}
			if(!v.filter)
				graph.nodes.push(v)
		}else if(v.bits & Edge){
			v.source = stash.get(v.data.srcID)
			v.target = stash.get(v.data.tgtID)
			v.filter |= v.source.filter | v.target.filter
			if(!v.filter)
				graph.edges.push(v)
		}
		if(clicked.ID === k)
			clicked.d = v.filter ? undefined : v
	}

	for(let e of graph.edges){
		let src = e.source
		let tgt = e.target
		let tID = tgt.data.ID
		let sID = src.data.ID
		src.neighbors[tID] = tgt
		tgt.neighbors[sID] = src
		if(tID in src.ln)
			e.linknum = ++src.ln[tID]
		else
			e.linknum = src.ln[tID] = 1
	}

	for(let n of graph.nodes){
		n.nc = Object.keys(n.neighbors).length
		if(graph.max_nc < n.nc)
			graph.max_nc = n.nc
	}

	for(let n of graph.nodes){
		n.max_neighbor_nc = 0
		for(let neighborID in n.neighbors){
			let neighbor = n.neighbors[neighborID]
			if(n.max_neighbor_nc < neighbor.nc)
				n.max_neighbor_nc = neighbor.nc
		}
		delete n.neighbors
		delete n.ln
	}

	function sw(d){
		return (d.mark || d.data.ID === clicked.ID) ? 1.5 : 1
	}
	function sk(d){
		return (d.data.ID === clicked.ID) ? "#f11" : d.mark ? "#1e1" : d.nc ? "#fff" : "#bbb"
	}
	function cf(d){
		return d.toobig ? "#000" : color(d.data.type)
	}
	function id(d){
		return 'ID' + d.data.ID
	}
	let link = svg.append("g")
		.attr("class", "links")
		.attr("transform", transform)
		.selectAll("line")
		.data(graph.edges)
		.enter().append("path")
			.attr("id", id)
			.attr("stroke", sk)
			.attr("stroke-width", sw)
			.attr("fill", "none")
			.on("click", click)

	let node = svg.append("g")
		.attr("class", "nodes")
		.attr("transform", transform)
		.selectAll("circle")
		.data(graph.nodes)
		.enter().append("circle")
			.attr("id", id)
			.attr("r", radius)
			.attr("fill", cf)
			.attr("stroke", sk)
			.attr("stroke-width", sw)
			.on("click", click)
			.call(d3.drag()
				.on("start", dragstarted)
				.on("drag", dragged)
				.on("end", dragended))

	link.append("title")
		.text(function(d) { return JSON.stringify(sortTopObject(d.data,['ID','type','value','srcID','tgtID']), null, 2) })

	node.append("title")
		.text(function(d) { return JSON.stringify(sortTopObject(d.data,['ID','type','value']), null, 2) })

	function radius(d){
		return 5 + (graph.max_nc ? 4*Math.sqrt(Math.min(d.nc, graph.max_nc)/graph.max_nc) : 0)
	}

	function click(d){
		let old = info
		info = old.cloneNode(false)
		old.parentNode.replaceChild(info, old)

		if(d.data.ID == clicked.ID){
			delete clicked.ID
			info.style.visibility = "hidden"
		}else{
			let prevID = clicked.ID
			clicked.ID = d.data.ID
			if(prevID){
				d3.select("#ID" + prevID)
					.style("stroke", sk)
					.style("stroke-width", sw)
			}

			const tree = JsonView.createTree(d.data)
			JsonView.render(tree, info)
			JsonView.expandChildren(tree)
			info.style.visibility = "visible"
		}
		d3.select(this)
			.style("stroke", sk)
			.style("stroke-width", sw)
	}

	simulation = d3.forceSimulation()
		.velocityDecay(.11)
		.alphaDecay(0.019)
		.force("collide", d3.forceCollide().radius(radius))
		.force("center", d3.forceCenter())
		.force("fX", d3.forceX())
		.force("fY", d3.forceY())
		.force("link", d3.forceLink().strength(function(link){
			let f = 1
			if(clamp_nc(Math.min(link.source.nc, link.target.nc)) > 1)
				f = Math.pow((1 + graph.max_nc - clamp_nc(Math.min(link.source.max_neighbor_nc, link.target.max_neighbor_nc))) / graph.max_nc, .9)
			return f
		}))

		.force("charge", d3.forceManyBody().strength(function(n){
			let f = n.nc ? -30 : -20
			if(n.nc > 1)
				f = - 100 * (clamp_nc(n.nc) + clamp_nc(n.max_neighbor_nc)) / graph.max_nc
			return f
		}))

	function curve(d){
		let p1x = d.source.x
		let p1y = d.source.y

		if(d.source === d.target){
			let t = radius(d.source) + Math.pow(d.linknum - 1, .75) * 3
			return "M" + p1x + "," + p1y + "a" + t + "," + t + " 0 1,1 " + 1.5 + "," + 1
		}

		let p2x = d.target.x
		let p2y = d.target.y

		let dx = p1x - p2x
		let dy = p1y - p2y
		let dist = Math.sqrt(dx*dx + dy*dy)

		// mid-point of line:
		let mpx = (p2x + p1x) * 0.5
		let mpy = (p2y + p1y) * 0.5

		// angle of perpendicular to line:
		let theta = Math.atan2(p2y - p1y, p2x - p1x) - Math.PI / 2

		// distance of control point from mid-point of line:
		let offset = d.linknum * dist * .2 * Math.pow(.98, d.linknum)

		// location of control point:
		let c1x = mpx + offset * Math.cos(theta)
		let c1y = mpy + offset * Math.sin(theta)

		// construct the command to draw a quadratic curve
		return "M" + p1x + " " + p1y + " Q " + c1x + " " + c1y + " " + p2x + " " + p2y
	}

	function clamp_nc(nc){
		return Math.min(nc, graph.max_nc)
	}

	simulation
			.nodes(graph.nodes)
			.on("tick", ticked)

	simulation.force("link")
			.links(graph.edges)

	function ticked() {
		link
//				.attr("x1", function(d) { return d.source.x })
//				.attr("y1", function(d) { return d.source.y })
//				.attr("x2", function(d) { return d.target.x })
//				.attr("y2", function(d) { return d.target.y })
				.attr("d", curve)

		node
				.attr("cx", function(d) { return d.x })
				.attr("cy", function(d) { return d.y })
	}
	poll = window.setTimeout(updategraph, 1000)

	if(clicked.d !== undefined){
		let old = info
		info = old.cloneNode(false)
		old.parentNode.replaceChild(info, old)

		const tree = JsonView.createTree(clicked.d.data)
		JsonView.render(tree, info)
		JsonView.expandChildren(tree)

		delete clicked.d
		info.style.visibility = "visible"
	}else if(clicked.ID){
		let old = info
		info = old.cloneNode(false)
		old.parentNode.replaceChild(info, old)
		info.style.visibility = "hidden"
	}
}

function dragstarted(d) {
	if (!d3.event.active) simulation.alphaTarget(0.2).restart()
	d.fx = d.x
	d.fy = d.y
}

function dragged(d) {
	d.fx = d3.event.x
	d.fy = d3.event.y
}

function dragended(d) {
	if (!d3.event.active) simulation.alphaTarget(0)
	d.fx = null
	d.fy = null
}

function lg_graph(svg_target, info_target){
	const w = window,
		d = document,
		e = d.documentElement,
		g = d.getElementsByTagName('body')[0]

	info = document.querySelector(info_target)

	svg = d3.select(svg_target)
		.classed("svg-content-responsive", true)
		.attr("preserveAspectRatio", "xMidYMid meet")
		.call(d3.zoom().scaleExtent([0.125, 8])
		.on("zoom", function(){
			svg.selectAll("g").attr("transform", transform=d3.event.transform)
		}))
		.on("dblclick.zoom", null)

	let qrt = null

	function resize(){
		let x = w.innerWidth  || e.clientWidth  || g.clientWidth
		let y = w.innerHeight || e.clientHeight || g.clientHeight
		let vb = -x/2 + " " + -y/2 + " " + x + " " + y
		svg.attr("width", x)
			.attr("height", y)
			.attr("viewBox", vb)
		qrt = null
	}

	function qresize(){
		if(qrt === null)
			qrt = setTimeout(resize, 33)
	}

	w.addEventListener('resize', qresize);
	resize()

	updategraph()
}
