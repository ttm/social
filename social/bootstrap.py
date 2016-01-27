import os, percolation as P
from percolation.rdf import NS
QUIET=False
PERCOLATIONDIR="~/.percolation/rdf/"
PACKAGEDIR=os.path.dirname(__file__)
DATADIR=PACKAGEDIR+"/../data/"
P.start(start_session=False)
P.percolation_graph.bind("po",NS.po)
P.percolation_graph.bind("facebook",NS.facebook)
