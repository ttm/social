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
    for snapshoturi in snapshoturis[:7]:
        triplification_class=publishAny(snapshoturi)
        count+=1
    writePublishingReadme()
    return triplification_class
def writePublishingReadme(final_path="./fb/"):
    nfriendship= P.rdf.query("SELECT (COUNT(?s) as ?cs) WHERE { ?s a po:Snapshot . ?s facebook:isFriendship true . }")
    ninteraction=P.rdf.query("SELECT (COUNT(?s) as ?cs) WHERE { ?s a po:InteractionSnapshot }")
    nposts=      P.rdf.query("SELECT (COUNT(?s) as ?cs) WHERE { ?s a po:PostsSnapshot }")
    nego=        P.rdf.query("SELECT (COUNT(?s) as ?cs) WHERE { ?s a po:EgoSnapshot }")
    ngroup=      P.rdf.query("SELECT (COUNT(?s) as ?cs) WHERE { ?s a po:GroupSnapshot }")
    nfriendship_group=nfriendship-nego

    body="""::: Open Linked Social Data publication\n
This repository provides linked data for:
{} friendship snapshots (*Friendship.ttl and .rdf)
{} interaction snapshots (*Interaction.ttl and .rdf)
{} posts/texts snapshots (*Posts.ttl and .rdf)
{} ego snapshots (only *Friendship.ttl and .rdf)
{} group snapshots (can have *Friendship *Interaction *Posts.ttl .rdf)

*Meta.ttl and .rdf files are smaller and hold overall information
for discovery.

all interaction snapshots are group snapshots.
all posts snapshots are group snapshots.
all ego snapshots are friendship snapshots.
{} friendship snapshots are group snapshots.

The posts snapshots are not related to any participant.
Interactions and friendships yield relations
by the
facebook:Interaction#<snapshotid>-<userid1>-<userid2> and
facebook:Friendship#<snapshotid>-<userid1>-<userid2>
instances which relates
facebook:Participant#<snapshotid>-<userid1> and
facebook:Participant#<snapshotid>-<userid2>

Each directory of this repository have the name of the snapshot id
it provides data about.
""".format(
        nfrienship,
        ninteraction,
        nposts,
        nego,
        ngroup,
        nfriendship_group
        )

    # n participants
    nparticipants=P.get("SELECT (COUNT(?s) as ?cs) \
                    WHERE { ?s a po:Participant }")
    nparticipants_a0=P.get("SELECT (COUNT(?s) as ?cs) WHERE { \
                            ?s a facebook:Participant . \
                            ?s po:name ?namefoo . \
                    }")
    nparticipants_a=nparticipants-nparticipants_a0
    nparticipants_unique=P.get("SELECT (COUNT(?numeric_id) as ?total) WHERE { \
                            ?sfoo a facebook:Participant . \
                            ?sfoo po:numericID ?numeric_id . \
                    }")

    nfriendships=P.get("SELECT (COUNT(?s) as ?cs) \
                    WHERE { ?s a facebook:Friendship }")
    nfriendships_a=P.get("SELECT (COUNT(?s) as ?cs) \
                    WHERE { ?s a facebook:Friendship . \
                            ?s po:snapshot ?snapfoo . \
                            ?snapfoo po:friendshipsAnonymized true . \
                    }")

    ninteractions=P.get("SELECT (COUNT(?s) as ?cs) \
                        WHERE { ?s a facebook:Interaction }")

    ninteractions_a=P.get("SELECT (COUNT(?s) as ?cs) \
                    WHERE { ?s a facebook:Interaction . \
                            ?s po:snapshot ?snapfoo . \
                            ?snapfoo po:interactionsAnonymized true }")

    nposts=P.get("SELECT (COUNT(?s) as ?cs) WHERE { \
                            ?s a facebook:Post . \
                    }")

    ntokens=sum(P.get("SELECT ?val WHERE { \
            ?foosnapshot facebook:nTokensOverall ?val . \
                    }"))
    nlikes=sum(P.get("SELECT ?val WHERE { \
            ?foosnapshot facebook:nLikes ?val . \
                    }"))
    ncomments=sum(P.get("SELECT ?val WHERE { \
            ?foosnapshot facebook:nComments ?val . \
                    }"))
    nreactions=nlines+ncomments

    body+="""
Overview of core entities:
{} participants ({} anonymized; {} unique numericID)
{} friendships ({} anonymized)
{} interaction ({} anonymized)
{} posts with a total of {} tokens and {} reaction counts ({} comments+ {} likes)
:::""".format(nparticipants, nparticipants_a, nparticipants_unique,
        nfriendships, nfriendships_a,
        ninteractions, ninteractions_a,
        nposts, ntokens, nreactions, ncomments, nlikes)
    with open(final_path+"README","w") as f:
        f.write(body)

def botData(filename):
    pass
