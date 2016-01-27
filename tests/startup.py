import sys
keys=tuple(sys.modules.keys())
for key in keys:
    if "social" in key or "percolation" in key:
        del sys.modules[key]
import social as S
