from social import DATADIR, os
import percolation as P
from percolation.rdf import NS, a, po
c=P.check

def parseLegacyFiles(data_dir=DATADIR+"irc/"):
    """Parse legacy txt files with irc logs"""
    filenames=os.listdir(data_dir)
    filenames=[i for i in filenames if i!="ipython_log.py" and not i.endswith(".swp")]

    platformuri=P.rdf.ic(po.Platform,"#IRC",context="social_facebook")
    triples=[
            (platformuri, po.dataDir,data_dir),
            ]
    snapshots=set()
    for filename in filenames:
        snapshotid="irc-legacy-"+filename.replace("_","")
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



def getBuiltinLogs():
    # get logs in data/irc/* 
    pass
def getLiveLogs():
    # hold a list of online irc logs
    pass
