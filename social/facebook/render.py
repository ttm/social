from .gdf2rdf import GdfRdfPublishing
from .gml2rdf import GmlRdfPublishing
from percolation.rdf import NS, a
import percolation as P, social as S
c=P.check

social_facebook_inferred="social_facebook"
def gdfFile(filename):
    # gdf files can be tagged by the prefixes
    # and have associated interaction files
    pass
def gmlFile(filename):
    # gml files are always ego friendship networks 
    pass
def tabFile(filename):
    # tab files are associated to group data
    pass
def publish(filegroup):
    pass

def renderAny(snapshoturi):
    # get associated files
    # render with appropriate functions
    pass
def publishAny(snapshoturi):
    # publish to percolationdir
    # get friendship and interaction of the snapshoturi
    triples=[
            (snapshoturi, NS.po.rawFile, "?fileurifoo"),
            (snapshoturi,    NS.po.snapshotID, "?snapshotid"),
            ("?fileurifoo",    NS.po.expressedStructure, NS.po.FriendshipNetwork),
            ("?fileurifoo",    NS.po.fileFormat, "?fileformat"),
            ("?fileurifoo",    NS.po.fileName, "?filename"),
            ]
    fileformat,friendship_filename,snapshotid=P.get(triples,context=social_facebook_inferred)

    triples=[
            (snapshoturi, NS.po.rawFile, "?fileurifoo"),
            ("?fileurifoo",    NS.po.expressedStructure, NS.po.InteractionNetwork),
            ("?fileurifoo",    NS.po.fileName, "?filename"),
            ]
    interaction_filename=P.get(triples,context=social_facebook_inferred)

    triples=[
            (snapshoturi, NS.po.rawFile, "?fileurifoo"),
            ("?fileurifoo",    NS.po.expressedStructure, NS.po.GroupPosts),
            ("?fileurifoo",    NS.po.fileName, "?filename"),
            ]
    posts_filename=P.get(triples,context=social_facebook_inferred)
    c(fileformat)
    if "gdf" in fileformat:
        c("publish gdf", snapshoturi)
#        friendship_filename,interaction_filename=None,None
        return GdfRdfPublishing(snapshoturi,snapshotid,friendship_filename,interaction_filename,posts_filename)
    elif fileformat=="gml":
        c("publish gml", snapshoturi)
        GMLRDFPublishing(datadir,friendship_filename,snapshoturi,snapshotid)

#GDFTriplification(data_path="../data/fb/",filename_friendship="foo.gdf",filename_interaction="foo_interaction.gdf",
#    final_path="./fb/",scriptpath=None,numericid=None,stringid=None,fb_link=None,isego=None,umbrella_dir=None)
def publishAll(snapshoturis=None):
    triples=S.facebook.ontology.snapshots()
    P.add(triples,context="facebook_snapshots_ontology")
    P.rdf.inference.performRdfsInference("social_facebook","facebook_snapshots_ontology",social_facebook_inferred,False)
    if not snapshoturis:
        # get snapshots
        c("getting facebook snapshots, implementation needs verification TTM")
        # make inference from social_facebook with ontology
        # get all snapshots in new graph
        P.get((None,a,po.Snapshot),context=social_facebook_inferred)
    count=0
    #triples=[
    #        ("?s",a,NS.po.Snapshot),
    #        ("?s",NS.po.rawFile,"?rawfoo"),
    #        ("?rawfoo",NS.po.expressedStructure,NS.po.GroupPosts),
    #        ("?rawfoo",NS.po.fileSize,"?flesize"),
    #        ]
    ##snapshoturis=P.get(triples,modifier1=" ORDER BY DESC(?filesizefoo) ") # nao funciona
    #snapshoturis=P.get(triples)
    #snapshoturis.sort(key=lambda x: x[0])
    #snapshoturis=[i[1] for i in snapshoturis]
    uridict={}
    for snapshoturi in P.get(None,a,NS.po.Snapshot,minimized=True):
        uridict[snapshoturi]=0
        for rawFile in P.get(snapshoturi,NS.po.rawFile,strict=True,minimized=True):
            uridict[snapshoturi]+=P.get(rawFile,NS.po.fileSize,minimized=True).toPython()
    snapshoturis=list(uridict.keys())
    snapshoturis.sort(key=lambda x: uridict[x])
    c("snapuris:",snapshoturis)
    for snapshoturi in snapshoturis:
        triplification_class=publishAny(snapshoturi)
        count+=1
    return triplification_class

def botData(filename):
    pass
