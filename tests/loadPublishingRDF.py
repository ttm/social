import sys
keys=tuple(sys.modules.keys())
for key in keys:
    if "social" in key or "percolation" in key:
        del sys.modules[key]
import social as S, os
from percolation import check as c
snapdirs=os.listdir("./fb/")
snapdirs=[i for i in snapdirs if os.path.isdir("./fb/"+i)]
for snapdir in snapdirs:
    files=os.listdir("./fb/"+snapdir)
    files=[i for i in files if (i.endswith(".rdf") or i.endswith(".ttl"))]
    files.sort()
    for file_ in files:
        c("loading: ", file_, len(S.P.percolation_graph))
        if file_.endswith(".rdf"):
            S.P.percolation_graph.parse("./fb/"+snapdir+"/"+file_)
        elif file_.endswith(".ttl"):
            S.P.percolation_graph.parse("./fb/"+snapdir+"/"+file_,format="turtle")
        else:
            raise ValueError("Only rdf files please")
        c("loaded: ", file_, "ntriples in percolation graph: ", len(S.P.percolation_graph))
