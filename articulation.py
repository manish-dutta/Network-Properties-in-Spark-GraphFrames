import sys
import time
import networkx as nx
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions
from graphframes import *
from copy import deepcopy

sc=SparkContext("local", "degree.py")
sqlContext = SQLContext(sc)

def articulations(g, usegraphframe=False):
    # Get the starting count of connected components
    # YOUR CODE HERE
    components = g.connectedComponents().select('component').distinct().count()
    result = []
    # Default version sparkifies the connected components process 
    # and serializes node iteration.
    if usegraphframe:
        # Get vertex list for serial iteration
        # YOUR CODE HERE
        verticeList = g.vertices.map(lambda x:x.id).collect()
        # For each vertex, generate a new graphframe missing that vertex
        # and calculate connected component count. Then append count to
        # the output
        # YOUR CODE HERE
        for vertex in verticeList:
		gMiss = GraphFrame(g.vertices.filter('id != "'+ vertex +'"'),g.edges.filter('src != "'+ vertex +'"').filter('dst != "'+ vertex +'"'))
		components2 = gMiss.connectedComponents().select('component').distinct().count()
		result.append((vertex,1 if components2>components else 0))
		
	output = sqlContext.createDataFrame(sc.parallelize(result),['id','articulation']) 
	return output
    # Non-default version sparkifies node iteration and uses networkx 
    # for connected components count.
    else:
        # YOUR CODE HERE
        gspark = nx.Graph()
        gspark.add_nodes_from(g.vertices.map(lambda x: x.id).collect())
        gspark.add_edges_from(g.edges.map(lambda x: (x.src, x.dst)).collect())
	verticeList = g.vertices.map(lambda x:x.id).collect()
	for vertex in verticeList:
		newG = deepcopy(gspark)
		newG.remove_node(vertex)
		components2 = nx.number_connected_components(newG)
		result.append((vertex, 1 if components2 > components else 0))

	output = sqlContext.createDataFrame(sc.parallelize(result), ['id','articulation'])
	return output

filename = sys.argv[1]
lines = sc.textFile(filename)

pairs = lines.map(lambda s: s.split(","))
e = sqlContext.createDataFrame(pairs,['src','dst'])
e = e.unionAll(e.selectExpr('src as dst','dst as src')).distinct() # Ensure undirectedness 	

# Extract all endpoints from input file and make a single column frame.
v = e.selectExpr('src as id').unionAll(e.selectExpr('dst as id')).distinct()	

# Create graphframe from the vertices and edges.
g = GraphFrame(v,e)

#Runtime approximately 5 minutes
print("---------------------------")
print("Processing graph using Spark iteration over nodes and serial (networkx) connectedness calculations")
init = time.time()
df = articulations(g, False)
print("Execution time: %s seconds" % (time.time() - init))
print("Articulation points:")
df.filter('articulation = 1').show(truncate=False)
print("---------------------------")
print("Writing distribution to file articulation_out.csv")
df.filter('articulation = 1').toPandas().to_csv("articulation_out.csv")
#Runtime for below is more than 2 hours
#print("Processing graph using serial iteration over nodes and GraphFrame connectedness calculations")
#init = time.time()
#df = articulations(g, True)
#print("Execution time: %s seconds" % (time.time() - init))
#print("Articulation points:")
#df.filter('articulation = 1').show(truncate=False)

