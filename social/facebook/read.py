import networkx as x
import percolation as P
import re
c = P.check


def readGML2(filename="../data/RenatoFabbri06022014.gml"):
    with open(filename, "r") as f:
        data = f.read()
    lines = data.split("\n")
    nodes = []  # list of dicts, each a node
    edges = []  # list of tuples
    state = "receive"
    for line in lines:
        if state == "receive":
            if "node" in line:
                state = "node"
                nodes.append({})
            if "edge" in line:
                state = "edge"
                edges.append({})
        elif "]" in line:
            state = "receive"
        elif "[" in line:
            pass
        elif state == "node":
            var, val = re.findall(r"(.*?) (.*)", line.strip())[0]
            if var == "id":
                var = "name"
                val = "user_{}".format(val)
            elif '"' in val:
                val = val.replace('"', "")
            else:
                val = int(val)
            nodes[-1][var] = val
        elif state == "edge":
            var, val = line.strip().split()
            edges[-1][var] = val
        else:
            c("SPURIOUS LINE: "+line)
    keys = set([j for i in nodes for j in i.keys()])
    nodes_ = {}
    for key in keys:
        if key == "id":
            nodes_["name"] = [None]*len(nodes)
            i = 0
            for node in nodes:
                nodes_["name"][i] = "user_{}".format(node[key])
                i += 1
        else:
            nodes_[key] = [None]*len(nodes)
            i = 0
            for node in nodes:
                if key in node.keys():
                    nodes_[key][i] = node[key]
                i += 1
    c("para carregar as amizades")
    edges_ = {"node1": [None]*len(edges), "node2": [None]*len(edges)}
    i = 0
    for edge in edges:
        u1 = "user_{}".format(edge["source"])
        u2 = "user_{}".format(edge["target"])
        edges_["node1"][i] = u1
        edges_["node2"][i] = u2
        i += 1

    return {"relations": edges_,
            "individuals": nodes_}






    gg=x.read_gml(filename)
    nodes=gg.nodes(data=True)
    nodes_=[i[1] for i in nodes]
    nodes__={}
    nkeys=[]
    c("para carregar os individuos")
    for node in nodes_:
        nkeys+=list(node.keys())
    nkeys=set(nkeys)
    for key in nkeys:
        if key == "id":
            nodes__["name"]=[None]*len(nodes_)
            i=0
            for node in nodes_:
                nodes__["name"][i]="user_{}".format(node[key])
                i+=1
        else:
            nodes__[key]=[None]*len(nodes_)
            i=0
            for node in nodes_:
                if key in node.keys():
                    nodes__[key][i]=node[key]
                i+=1

    c("para carregar as amizades")
    edges=gg.edges(data=True)
    edges_={"node1":[None]*len(edges), "node2":[None]*len(edges)}
    i=0
    for edge in edges:
        u1="user_{}".format(edge[0])
        u2="user_{}".format(edge[1])
        edges_["node1"][i]=u1
        edges_["node2"][i]=u2
        i+=1
#    if edges[0][2]:
#        edges_=[i[2] for i in edges]
#        edges__={}
#        ekeys=edges_[0].keys()
#    for key in ekeys:
#       edges__[key]=[]
#       for edge in edges_:
#           edges__[key]+=[edge[key]]

    return {"relations": edges_,
            "individuals": nodes__}

def readGML(filename="../data/RenatoFabbri06022014.gml"):
    gg=x.read_gml(filename)
    nodes=gg.nodes(data=True)
    nodes_=[i[1] for i in nodes]
    nodes__={}
    nkeys=[]
    c("para carregar os individuos")
    for node in nodes_:
        nkeys+=list(node.keys())
    nkeys=set(nkeys)
    for key in nkeys:
        if key == "id":
            nodes__["name"]=[None]*len(nodes_)
            i=0
            for node in nodes_:
                nodes__["name"][i]="user_{}".format(node[key])
                i+=1
        else:
            nodes__[key]=[None]*len(nodes_)
            i=0
            for node in nodes_:
                if key in node.keys():
                    nodes__[key][i]=node[key]
                i+=1

    c("para carregar as amizades")
    edges=gg.edges(data=True)
    edges_={"node1":[None]*len(edges), "node2":[None]*len(edges)}
    i=0
    for edge in edges:
        u1="user_{}".format(edge[0])
        u2="user_{}".format(edge[1])
        edges_["node1"][i]=u1
        edges_["node2"][i]=u2
        i+=1
#    if edges[0][2]:
#        edges_=[i[2] for i in edges]
#        edges__={}
#        ekeys=edges_[0].keys()
#    for key in ekeys:
#       edges__[key]=[]
#       for edge in edges_:
#           edges__[key]+=[edge[key]]

    return {"relations": edges_,
            "individuals": nodes__}
    return gg
def readGDF(filename="../data/RenatoFabbri06022014.gdf"):
    """Made to work with gdf files from my own network and friends and groups"""
    with open(filename,"r") as f:
        data=f.read()
    lines=data.split("\n")
    columns=lines[0].split(">")[1].split(",")
    column_names=[i.split(" ")[0] for i in columns]
    data_friends={cn:[] for cn in column_names}
    for line in lines[1:]:
        if not line:
            break
        if ">" in line:
            columns=line.split(">")[1].split(",")
            column_names2=[i.split(" ")[0] for i in columns]
            data_friendships={cn:[] for cn in column_names2}
            continue
        fields=line.split(",")
        if "column_names2" not in locals():
            for i, field in enumerate(fields):
                if column_names[i] in ("name","groupid"): pass
                elif field.isdigit(): field=int(field)
                data_friends[column_names[i]].append(field)
        else:
            for i, field in enumerate(fields):
                if column_names2[i]=="name": pass
                elif field.isdigit(): field=int(field)
                data_friendships[column_names2[i]].append(field)
    return {"relations":data_friendships,
            "individuals":data_friends}
