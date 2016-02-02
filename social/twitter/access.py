from social import DATADIR, os
import percolation as P
from percolation.rdf import NS, a, po
c=P.check

def parseLegacyFiles(data_dir=DATADIR+"twitter/"):
    """Parse legacy pickle files with Twitter tweets"""
    filenames=os.listdir(data_dir)

    platformuri=P.rdf.ic(po.Platform,"#Facebook",context="social_facebook")
    triples=[
            (platformuri, po.dataDir,data_dir),
            ]
    snapshots=set()
    for filename in filenames:
        snapshotid="twitter-legacy-"+filename.replace("_","")
        snapshoturi=po.TwitterSnapshot+"#"+snapshotid
        expressed_classes=[po.Participant,po.Tweet]
        expressed_reference=filename.replace("_","").replace(".pickle","")
        name_humanized="Twitter"+expressed_reference
        filesize=os.path.getsize(data_dir+filename)/10**6
        fileformat="pickle"
        fileuri=po.File+"#twitter-file-"+filename
        triples+=[
                 (snapshoturi,a,po.Snapshot),
                 (snapshoturi,a,po.TwitterSnapshot),
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
    triples+=[
             (NS.social.Session,NS.social.nTwitterParsedFiles,nfiles),
             (NS.social.Session,NS.social.nTwitterSnapshots,nsnapshots),
             ]
    P.context("social_twitter","remove")
    P.add(triples,context="social_twitter")
    c("parsed {} twitter files ({} snapshots) are in percolation graph and 'social_twitter' context".format(nfiles,nsnapshots))
    c("percolation graph have {} triples ({} in social_facebook context)".format(len(P.percolation_graph),len(P.context("social_twitter"))))
    negos=P.query(r" SELECT (COUNT(?s) as ?cs) WHERE         { GRAPH <social_twitter> { ?s po:isEgo true         } } ")
    ngroups=P.query(r" SELECT (COUNT(?s) as ?cs) WHERE       { GRAPH <social_twitter> { ?s po:isGroup true       } } ")
    nfriendships=P.query(r" SELECT (COUNT(?s) as ?cs) WHERE  { GRAPH <social_twitter> { ?s po:isFriendship true  } } ")
    ninteractions=P.query(r" SELECT (COUNT(?s) as ?cs) WHERE { GRAPH <social_twitter> { ?s po:isInteraction true } } ")
    nposts=P.query(r" SELECT (COUNT(?s) as ?cs) WHERE        { GRAPH <social_twitter> { ?s po:isPost true        } } ")
    totalsize=sum(P.query(r" SELECT ?size WHERE              { GRAPH <social_twitter> { ?s po:fileSize ?size     } } "))
    c("""{} are ego snapshots, {} are group snapshots
{} have a friendship network. {} have an interaction network. {} have post texts and reaction counts
Total raw data size is {:.2f}MB""".format(negos,ngroups,nfriendships,ninteractions,nposts,totalsize))

    return snapshots


def groupTwitterFileGroupsForPublishing(self,filegroups):
    filegroups_grouped=[]
    i=0
    agroup=[]
    asize=0
    for group in filegroups:
        size=0
        for afile in group:
            size+=os.path.getsize(afile)
        if size/10**9>.9: # if total size is bigger than 1GB, put it alone:
            filegroups_grouped.append([group])
        else:
            asize+=size
            agroup.append(group)
            if asize/10**9>1: # if > 1GB
                filegroups_grouped.append(agroup)
                agroup=[]
                asize=0
    return silegroups_grouped

def search():
    """get recent tweets through standard search API"""
    pass
def stream():
    """get currently sent tweets through standard stream API"""
    pass


def thirdParty():
    """get tweets through a third party API"""
    pass
