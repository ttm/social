import cureimport
cureimport.cure("social","percolation")
import sys
keys=tuple(sys.modules.keys())
for key in keys:
    if "social" in key:
        del sys.modules[key]

import social as S, percolation as P
from percolation.rdf import NS, a
ss=S.facebook.access.parseLegacyFiles()


