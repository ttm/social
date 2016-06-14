import percolation as P
from percolation.rdf import NS, a, po
from .log2rdf import LogPublishing
c = P.check


def publishAll(snapshoturis=None):
    """express irc logs as RDF for publishing"""
    if not snapshoturis:
        c("getting irc snapshots, implementation needs verification TTM")
        uridict = {}
        for snapshoturi in P.get(None, a, NS.po.IRCSnapshot, minimized=True):
            uridict[snapshoturi] = 0
            for rawFile in P.get(snapshoturi, NS.po.rawFile, strict=True, minimized=True):
                uridict[snapshoturi] += P.get(rawFile, NS.po.fileSize, minimized=True).toPython()
        snapshoturis.sort(key=lambda x: uridict[x])
    for snapshoturi in snapshoturis:
        triplification_class = publishAny(snapshoturi)
    return triplification_class


def publishAny(snapshoturi):
    triples = [
            (snapshoturi,      po.rawFile, "?fileurifoo"),
            ("?fileurifoo",    po.fileName, "?filename"),
            (snapshoturi,      po.snapshotID, "?snapshotid"),
            ]
    filename, snapshotid = P.get(triples)
#    filenames=[i for i in filenames if i.count("_")==2]
    return LogPublishing(snapshoturi, snapshotid, filename)
#    return snapshotid, snapshoturi, filenames


def log():
    """render irc log as rdf data for publishing"""
    pass
