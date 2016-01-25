import sys
keys=tuple(sys.modules.keys())
for key in keys:
    if "social" in key or "percolation" in key:
        del sys.modules[key]

import social as S, percolation as P
from percolation.rdf import NS, a
po=NS.po
ss=S.facebook.access.parseLegacyFiles()
S.facebook.render.publishAll(ss)


