import percolation as P
from percolation.rdf import NS, a, po, c
from .pickle2rdf import PicklePublishing


def publishAll(snapshoturis=None):
    """express tweets as RDF for publishing"""
    if not snapshoturis:
        c("getting twitter snapshots, implementation needs verification TTM")
        uridict = {}
        for snapshoturi in P.get(None, a, NS.po.TwitterSnapshot,
                                 minimized=True):
            uridict[snapshoturi] = 0
            for rawFile in P.get(snapshoturi, NS.po.rawFile, strict=True,
                                 minimized=True):
                uridict[snapshoturi] += P.get(rawFile, NS.po.fileSize,
                                              minimized=True).toPython()
        snapshoturis = [i for i in list(uridict.keys()) if i.endswith(".gml")]
        snapshoturis.sort(key=lambda x: uridict[x])
    for snapshoturi in snapshoturis:
        triplification_class = publishAny(snapshoturi)
    # writePublishingReadme()
    return triplification_class


def publishAny(snapshoturi):
    # publish to umbrelladir
    triples = [
            (snapshoturi,      po.rawFile, "?fileurifoo"),
            ("?fileurifoo",    po.fileName, "?filename"),
            ]
    filenames = P.get(triples, join_queries="list", strict=True)
    filenames.sort()
#    filenames=[i for i in filenames if i.count("_")==2]
    triples = [
            (snapshoturi,      po.snapshotID, "?snapshotid"),
            ]
    snapshotid = P.get(triples)
    if filenames:
        return PicklePublishing(snapshoturi, snapshotid, filenames)
#    return snapshotid, snapshoturi, filenames
