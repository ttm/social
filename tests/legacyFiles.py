import sys
keys=tuple(sys.modules.keys())
for key in keys:
    if "social" in key or "percolation" in key:
        del sys.modules[key]

import social as S, percolation as P
from percolation.rdf import NS, a, po, c
#ss=S.facebook.access.parseLegacyFiles() # parse all gdf gml tab files under social/data/facebook/
#ss=S.twitter.access.parseLegacyFiles() # parse all pickle files under social/data/twitter/
ss=S.irc.access.parseLegacyFiles() # parse all log files under social/data/irc/



