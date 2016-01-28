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
    platformuri=P.rdf.ic(po.Platform,"#Facebook",context="social_facebook")
    triples=[
            (platformuri, po.dataDir,datadir),
            ]
    filenames=os.listdir(datadir)
    # clean filenames: if they are equal except for extension, keep gml file
    snapshots=set()
    regex=re.compile(r"^(avlab_|ego_|posavlab_|page_)*(.*?)(\d{8})(_interactions|_comments){0,1}\.(gdf|tab|gml)$")
    regex2=re.compile(r'([A-Z]{2,}(?=[A-Z]|$)|[A-Z][a-z]*)')
    for filename in filenames:
        prefix,name,date,sufix,format_=regex.findall(filename)[0]
        if prefix=="page_":
            c("page data currently not supported. Jumping", filename)
            continue
        if filename.endswith(".gml") or any(filename.startswith(i) for i in ("ego_","avlab_","posavlab_")):
            isego=True
            isgroup=False
            isfriendship=True
            isinteraction=False
            isposts=False
            expressed_structure="ego_friendship" # for the file
        else: # group snapshot
            isego=False
            isgroup=True
            ffilename=prefix+name+date+".gdf"
            ifilename=prefix+name+date+"_interactions.gdf"
            tfilename=prefix+name+date+".tab"
            isfriendship=ffilename in filenames
            isinteraction=ifilename in filenames
            isposts=tfilename in filenames
            if filename==ffilename:
                expressed_structure="group_friendship" # for the file
            elif filename==ifilename:
                expressed_structure="group_interaction" # for the file
            elif format_=="tab":
                expressed_structure="group_posts" # for the file
            else:
                raise NameError("filename structure not understood")
        filesize=os.path.getsize(datadir+filename)/(10**6) # size in megabytes
        snapshotid=filename.replace("_interactions.gdf",".gdf").replace(".tab",".gdf")+"_fb"
        snapshoturi=snapshotclass+"#"+snapshotid

        date_obtained=datetime.datetime(int(date[4:]),int(date[2:4]),int(date[:2]))
        name_humanized=" ".join(regex2.findall(name)[0])
        fileuri=NS.po.File+"#"+snapshotid+"-_file_-"+filename
        triples+=[
                 (snapshoturi, a, po.Snapshot),
                 (snapshoturi, NS.po.snapshotID, snapshotid),
                 (snapshoturi, NS.po.humanizedName, name_humanized),
                 (snapshoturi, NS.po.dateObtained, date_obtained),
                 (snapshoturi, NS.po.rawFile, fileuri),
                 (fileuri,    NS.po.fileSize, filesize),
                 (fileuri,    NS.po.fileName, filename),
                 (fileuri,    NS.po.fileFormat, format_),
                 (fileuri,    NS.po.expressedStructure, expressed_structure),
                 ]
        # get metadata for files
        metadata=S.legacy.facebook.files.files_dict[filename.replace("_interactions.gdf",".gdf").replace(".tab",".gdf")]
        if metadata[0]:
            triples+=[(snapshoturi,po.numericID,metadata[0])]
        if metadata[1]:
            triple+=[(snapshoturi,po.stringID,metadata[1])]
        if len(metadata)==3:
            if not metadata[2]:
                c("group data without a publishing link: ",filename)
            else:
                triples+=[(snapshoturi,po.publishedURL,metadata[2])]
        note=theNote(filename) # for avlab and posavlab
        if note:
            triples+=[
                     (snapshoturi,NS.rdfs.comment,note),
                     ]
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

def theNote(filename):
    if filename.startswith("avlab_"):
        return "snapshot acquired during AVLAB in Feb/21-3,25/2014"
    elif filename.startswith("posavlab_"):
        return "snapshot acquired after during AVLAB in Feb/21-3,25/2014"
class MeBot:
    """start browser bot with user credentials"""
    # use socialLegacy bot
    pass
