import percolation as P
from percolation.rdf import NS, a
c=P.check
def parseBuiltinFiles(datadir="/../data/facebook/"):
    prefixes="ego_","avlab_","posavlab_" # these are all gdf ego networks
    """group files with appropriated metadata extracted from filename"""
    triples=[
            (NS.po.Facebook, NS.po.dataDir,datadir),
            ]
    files=os.listdir(datadir)
    for filename in files:
        fileformat=theFormat(filename)
        snapshotclass,snapshotid,snapshoturi=theSnapshotIDURI(filename)
        expressed_structure_uri=theStructure(filename) # group/ego and friendship/interaction
        note=theNote(filename) # for avlab and posavlab
        date_obtained=theDate(filename)
        name=theName(filename)
        fileuri=NS.po.File+"#"+snapshotid+"_f_"+filename
        triples=[]
        if note:
            triples+=[
                     (snapshoturi,NS.rdfs.comment,note),
                     ]
        triples+=[
                 (snapshoturi, a, snapshotclass),
                 (snapshoturi, NS.po.snapshotID, snapshotid),
                 (snapshoturi, NS.po.humanizedName, name),
                 (snapshoturi, NS.po.dateObtained, date_obtained),
                 (snapshoturi, NS.po.rawFile, fileuri),
                 (fileuri,    NS.po.filename, filename),
                 (fileuri,    NS.po.fileformat, fileformat),
                 (fileuri,    NS.po.expressedStructure, expressed_structure_uri),
                 ]
    P.add(triples,context="social_facebook")
    nfiles=len(files)
    nsnapshots=P.get()
    c("parsed facebook files in percolation graph and 'session' context")
def theName(filename):
    name=re.findall(r"(avlab_|posavlab_|ego_)*([a-zA-Z]*)\d*.gdf",cc)[0][1]
    name=" ".join(re.findall('[A-Z][^A-Z]*', name))
    return name
def theDate(filename):
    day,month,year=re.findall(r".*(\d\d)(\d\d)(\d\d\d\d).gdf",fname)[0]
    datetime=datetime.date(*[int(i) for i in (year,month,day)])
    return datetime
def theFormat(filename):
    if filename.endwith(".tab"):
        return "tab"
    elif filename.endwith(".gdf"):
        return "gdf"
    elif filename.endwith(".gml"):
        return "gml"
def theSnapshotIDURI(filename):
    if filename.endwith("_interaction.gdf"):
        snapshotid=filename.replace("_interaction.gdf",".gdf")
    else:
        snapshotid=filename
    snapshotid+="_fb" # to avoid having the same id between snapshots from diverse provenance
    if filename.endwith(".gml") or any(filename.startswith(i) for i in ("ego_","avlab_","posavlab_")):
        class_=NS.po.FacebookEgoFriendshipSnapshot
    else:
        class_=NS.po.FacebookGroupFriendshipInteractionSnapshot
    snapshoturi=class_+"#"+snapshotid
    return class_,snapshotid, snapshoturi
def theDate(filename):
    # parse datetime from filename
    pass
def theStructure(filename):
    if filename.endwith(".gml") or any(filename.startswith(i) for i in ("ego_","avlab_","posavlab_")):
        uri=NS.po.EgoFriendshipNetwork
    elif filename.endswith(".tab"):
        uri=NS.po.GroupIndividualPosts
    elif filename.endswith("_interaction.gdf"):
        uri=NS.po.GroupInteractionNetwork
    elif filename.endswith(".gdf"):
        uri=NS.po.GroupFriendshipNetwork
    return uri
def theNote(filename):
    if filename.startswith("avlab_"):
        return "snapshot acquired during AVLAB in Feb/21-3,25/2015"
    elif filename.startswith("posavlab_"):
        return "snapshot acquired after during AVLAB in Feb/21-3,25/2015"
class MeBot:
    """start browser bot with user credentials"""
    # use socialLegacy bot
    pass

