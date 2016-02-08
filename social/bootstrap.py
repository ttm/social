import os, percolation as P
from percolation.rdf import NS
PERCOLATIONDIR="~/.percolation/rdf/"
PACKAGEDIR=os.path.dirname(__file__)
DATADIR=PACKAGEDIR+"/../data/"
P.start(start_session=False)
P.percolation_graph.bind("po",NS.po)
