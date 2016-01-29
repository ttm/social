import sys
keys=tuple(sys.modules.keys())
for key in keys:
    if "social" in key or "percolation" in key:
        del sys.modules[key]

import social as S, percolation as P
from percolation.rdf import NS, a
po=NS.po
ss=S.facebook.access.parseLegacyFiles()
#ss=[i for i in ss if i.endswith("gdf_fb")]
last_triplification_class=S.facebook.render.publishAll(ss)


