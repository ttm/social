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
        GdfRdfPublishing(snapshoturi,snapshotid,friendship_filename,interaction_filename,posts_filename)
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
    for snapshoturi in snapshoturis:
        publishAny(snapshoturi)
        count+=1

def botData(filename):
    pass
