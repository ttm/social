import percolation as P, os, re, datetime
from percolation.rdf import NS, a
po=NS.po
from social import DATADIR
import social as S
c=P.check
def parseLegacyFiles(datadir=DATADIR+"facebook/"):
    """Parse legacy gdf, gml and tab files of facebook structures
    
    Synthax of facebook filenames is:
    <prefix><name><date><suffix><extension> where:

    <prefix> used are:
        *) avlab_ for files obtained with participants at AVLAB
        *) posavlab_ for files obtained from participants
        *) page_ for files about facebook pages
        *) ego_ for ego networks
    ommited for gml files and gdf group files.

    <name> is any string name associated with the user or group delimiting the structure in the file, e.g. FernandValfro.
    it gets split with spaces before uppercase letter chuncks for po:humanizedName: REM splits to REM. RFabbri to RFabbri.

    <date> daymonthyear in 2/2/4 digits, e.g. 20032014 for 20/March/2014.

    <suffix> is ommited for friendship .gml .gdf networks, .tab text and activity files.
    _interaction is used if interaction network.

    <extension> is either .gml for gml files, all are ego friendship network data
                          .gdf for gdf files with group and ego, interaction and friendship network data
                          .tab for tab files with post data, such as text

    These render snapshots of two classes:
    po:FacebookEgoFriendshipSnapshot from .gml files and gdf files with prefix avlab_ posavlab_ or ego_
    po:FacebookGroupFriendshipInteractionSnapshot from .gdf files without prefix with and without _interaction suffix and the .tab files. They form sets of files, all with friendship and interaction networks and some with a .tab file.

    ToDo:
       *) Implement parsing of page files.
       *) Implement parsing of new group files."""
    triples=[
            (NS.po.Facebook, NS.po.dataDir,datadir),
            ]
    filenames=os.listdir(datadir)
    # clean filenames: if they are equal except for extension, keep gml file
    snapshots=set()
    for filename in filenames:
        if filename.startswith("page_"):
            c("page data currently not supported. Jumping", filename)
            continue
        fileformat=theFormat(filename)
#        snapshotclass,snapshotid,snapshoturi=theSnapshotIDURI(filename)

        snapshotid=filename.replace("_interactions.gdf",".gdf").replace(".tab",".gdf")
        snapshotid+="_fb" # to avoid having the same id between snapshots from diverse provenance

        if filename.endswith(".gml") or any(filename.startswith(i) for i in ("ego_","avlab_","posavlab_")):
            snapshotclass=NS.po.FacebookEgoFriendshipSnapshot
        else: # group snapshot
            if filename.endswith("_interactions.gdf") or filename.endswith(".tab") or (filename.replace(".gdf","_interactions.gdf") in filenames): # with interaction
                snapshotclass=NS.po.FacebookGroupFriendshipInteractionSnapshot
            else:
                snapshotclass=NS.po.FacebookGroupFriendshipSnapshot

        snapshoturi=snapshotclass+"#"+snapshotid

        expressed_structure_uri=theStructure(filename) # group/ego and friendship/interaction
        date_obtained=theDate(filename)
        name=theName(filename)
        fileuri=NS.po.File+"#"+snapshotid+"_f_"+filename
        triples=[]
        note=theNote(filename) # for avlab and posavlab
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
        # get metadata for files
        metadata=theMetadata(filename)
        if metadata[0]:
            triples+=[(snapshoturi,po.numericID,metadata[0])]
        if metadata[1]:
            triples+=[(snapshoturi,po.stringID,metadata[1])]
        if len(metadata)==3:
            if not metadata[2]:
                c("group data without a publishing link: ",filename)
            triples+=[(snapshoturi,po.publishedURL,metadata[2])]
        snapshots.add(snapshoturi)
    # data about the overall data in percolation graph
    nfiles=len(filenames)
    nsnapshots=len(snapshots)
    triples+=[
             (NS.social.Session,NS.social.nFacebookParsedFiles,nfiles),
             (NS.social.Session,NS.social.nFacebookSnapshots,nsnapshots),
             ]
    P.context("social_facebook","remove")
    P.add(triples,context="social_facebook")
    c("parsed {} facebook files ({} snapshots) are in percolation graph and 'social_facebook' context".format(nfiles,nsnapshots))
    print("parsed {} facebook files ({} snapshots) are in percolation graph and 'social_facebook' context".format(nfiles,nsnapshots))
    return snapshots
def theMetadata(filename):
    metadata=S.legacy.facebook.files.files_dict[filename.replace("_interactions.gdf",".gdf").replace(".tab",".gdf")]
    return metadata

def theName(filename):
    name=re.findall(r"(avlab_|posavlab_|ego_)*([a-zA-Z]*)\d*[\b\.gdf\b|\b\.tab\b|\b\.gml\b]",filename)[0][1]
    pattern=r'([A-Z]{2,}(?=[A-Z]|$)|[A-Z][a-z]*)'
    name=" ".join(re.findall(pattern, name))
    return name
def theFormat(filename):
    if filename.endswith(".tab"):
        return "tab"
    elif filename.endswith(".gdf"):
        return "gdf"
    elif filename.endswith(".gml"):
        return "gml"
def theDate(filename):
    day,month,year=re.findall(r".*(\d\d)(\d\d)(\d\d\d\d)[\b_interactions\b]*[\b\.gdf\b|\b\.tab\b|\b\.gml\b]",filename)[0]
    datetime_=datetime.date(*[int(i) for i in (year,month,day)])
    return datetime_
def theStructure(filename):
    if filename.endswith(".gml") or any(filename.startswith(i) for i in ("ego_","avlab_","posavlab_")):
        uri=NS.po.EgoFriendshipNetwork
    elif filename.endswith(".tab"):
        uri=NS.po.GroupPosts
    elif filename.endswith("_interactions.gdf"):
        uri=NS.po.GroupInteractionNetwork
    elif filename.endswith(".gdf"):
        uri=NS.po.GroupFriendshipNetwork
    return uri
def theNote(filename):
    if filename.startswith("avlab_"):
        return "snapshot acquired during AVLAB in Feb/21-3,25/2014"
    elif filename.startswith("posavlab_"):
        return "snapshot acquired after during AVLAB in Feb/21-3,25/2014"
class MeBot:
    """start browser bot with user credentials"""
    # use socialLegacy bot
    pass

