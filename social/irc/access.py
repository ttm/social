from social import DATADIR, os
import percolation as P
from percolation.rdf import NS, a, po
c=P.check

def parseLegacyFiles(data_dir=DATADIR+"irc/"):
    """Parse legacy txt files with irc logs"""
    filenames=os.listdir(data_dir)
    filenames=[i for i in filenames if i!="ipython_log.py" and not i.endswith(".swp")]

    snapshots=set()
    for filename in filenames:
        snapshotid="irc-legacy-"+filename.replace("#","")
        snapshoturi=po.TwitterSnapshot+"#"+snapshotid
        expressed_classes=[po.Participant,po.IRCMessage]
        expressed_reference=filename.replace("#","").replace(".txt","").replace(".log","")
        name_humanized="IRC log of channel "+expressed_reference
        filesize=os.path.getsize(data_dir+filename)/10**6
        fileformat="txt"
        fileuri=po.File+"#Irc-log-"+filename.replace("#","")
        triples+=[
                 (snapshoturi,a,po.Snapshot),
                 (snapshoturi,a,po.IRCSnapshot),
                 (snapshoturi,po.snapshotID,snapshotid),
                 (snapshoturi, po.isEgo, False),
                 (snapshoturi, po.isGroup, True),
                 (snapshoturi, po.isFriendship, False),
                 (snapshoturi, po.isInteraction, True),
                 (snapshoturi, po.isPost, True),
                 (snapshoturi, po.humanizedName, name_humanized),
                 (snapshoturi, po.expressedReference, expressed_reference),
                 (snapshoturi, po.rawFile, fileuri),
                 (fileuri,     po.fileSize, filesize),
                 (fileuri,     po.fileName, filename),
                 (fileuri,     po.fileFormat, fileformat),
                 ]+[
                 (fileuri,    po.expressedClass, expressed_class) for expressed_class in expressed_classes
                 ]
        snapshots.add(snapshoturi)
    nfiles=len(filenames)
    nsnapshots=len(snapshots)
    P.context("social_irc","remove")
    platformuri=P.rdf.ic(po.Platform,"#IRC",context="social_irc")
    triples+=[
             (NS.social.Session,NS.social.nTwitterParsedFiles,nfiles),
             (NS.social.Session,NS.social.nTwitterSnapshots,nsnapshots),
             (platformuri, po.dataDir,data_dir),
             ]
    P.add(triples,context="social_irc")
    c("parsed {} irc logs files ({} snapshots) are in percolation graph and 'irc_twitter' context".format(nfiles,nsnapshots))
    c("percolation graph have {} triples ({} in social_irc context)".format(len(P.percolation_graph),len(P.context("social_irc"))))
    negos=P.query(r" SELECT (COUNT(?s) as ?cs) WHERE         { GRAPH <social_irc> { ?s po:isEgo true         } } ")
    ngroups=P.query(r" SELECT (COUNT(?s) as ?cs) WHERE       { GRAPH <social_irc> { ?s po:isGroup true       } } ")
    nfriendships=P.query(r" SELECT (COUNT(?s) as ?cs) WHERE  { GRAPH <social_irc> { ?s po:isFriendship true  } } ")
    ninteractions=P.query(r" SELECT (COUNT(?s) as ?cs) WHERE { GRAPH <social_irc> { ?s po:isInteraction true } } ")
    nposts=P.query(r" SELECT (COUNT(?s) as ?cs) WHERE        { GRAPH <social_irc> { ?s po:isPost true        } } ")
    totalsize=sum(P.query(r" SELECT ?size WHERE              { GRAPH <social_irc> { ?s po:fileSize ?size     } } "))
    c("""{} are ego snapshots, {} are group snapshots
{} have a friendship structures. {} have an interaction structures. {} have texts 
Total raw data size is {:.2f}MB""".format(negos,ngroups,nfriendships,ninteractions,nposts,totalsize))

    return snapshots




def getBuiltinLogs():
    # get logs in data/irc/* 
    pass
def getLiveLogs():
    # hold a list of online irc logs
    pass
