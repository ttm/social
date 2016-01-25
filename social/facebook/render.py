from .gdf2rdf import GDFTriplification
from .gml2rdf import GMLTriplification


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
            ("?fileurifoo",    NS.po.expressedStructure, po.FriendshipNetwork),
            ("?fileurifoo",    NS.po.fileFormat, "?fileformat"),
            ("?fileurifoo",    NS.po.fileName, "?filename"),
            ]
    fileformat,friendship_filename=P.get(triples,"social_facebook_inffered")

    triples=[
            (snapshoturi, NS.po.rawFile, "?fileurifoo"),
            ("?fileurifoo",    NS.po.expressedStructure, po.InteractionNetwork),
            ("?fileurifoo",    NS.po.fileName, "?filename"),
            ]
    interaction_filename=P.get(triples,"social_facebook_inffered")

    triples=[
            (snapshoturi, NS.po.rawFile, "?fileurifoo"),
            ("?fileurifoo",    NS.po.expressedStructure, po.GroupPosts),
            ("?fileurifoo",    NS.po.fileName, "?filename"),
            ]
    posts_filename=P.get(triples,"social_facebook_inffered")

    datadir=P.get((po.Facebook,po.dataDir,None),"social_facebook_inffered")

    if fileformat=="gdf":
        GDFRDFPublishing(datadir,friendship_filename,interaction_filename,posts_filename,snapshoturi,snapshotid)
    elif fileformat=="gml":
        GMLRDFPublishing(datadir,friendship_filename,snapshoturi,snapshotid)

#GDFTriplification(data_path="../data/fb/",filename_friendship="foo.gdf",filename_interaction="foo_interaction.gdf",
#    final_path="./fb/",scriptpath=None,numericid=None,stringid=None,fb_link=None,isego=None,umbrella_dir=None)
def publishAll(snapshoturis=None):
    if not snapshoturis:
        # get snapshots
        c("getting facebook snapshots, implementation needs verification TTM")
        triples=S.facebook.ontology.snapshots()
        P.add(triples,context="facebook_snapshots_ontolgy")
        # make inference from social_facebook with ontology
        P.infference("social_facebook","facebook_snapshots_ontology","social_facebook_inffered")
        # get all snapshots in new graph
        P.get((None,a,po.Snapshot),context="social_facebook_inffered")
    for snapshoturi in snapshoturis:
        publishAny(snapshoturi)


def botData(filename):
    pass
