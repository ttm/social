from .gdf2rdf import GdfRdfPublishing
from .gml2rdf import GmlRdfPublishing
from percolation.rdf import NS, a
import percolation as P
c = P.check
po = NS.po
social_facebook_inferred = "social_facebook"


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
    # publish to umbrelladir
    # get friendship and interaction of the snapshoturi
    triples = [
            (snapshoturi,      po.rawFile, "?fileurifoo"),
            (snapshoturi,      po.snapshotID, "?snapshotid"),
            ("?fileurifoo",    po.expressedClass, po.Friendship),
            ("?fileurifoo",    po.fileFormat, "?fileformat"),
            ("?fileurifoo",    po.fileName, "?filename"),
            ]
    fileformat, friendship_filename, snapshotid = P.get(triples)

    triples = [
            (snapshoturi, NS.po.rawFile, "?fileurifoo"),
            ("?fileurifoo",    po.expressedClass, po.Interaction),
            ("?fileurifoo",    NS.po.fileName, "?filename"),
            ]
    interaction_filename = P.get(triples, context=social_facebook_inferred)

    triples = [
            (snapshoturi, NS.po.rawFile, "?fileurifoo"),
            ("?fileurifoo",    po.expressedClass, po.Post),
            ("?fileurifoo",    NS.po.fileName, "?filename"),
            ]
    posts_filename = P.get(triples, context=social_facebook_inferred)
    c(fileformat)
    if "gdf" in fileformat:
        c("publish gdf", snapshoturi)
        return GdfRdfPublishing(snapshoturi, snapshotid, friendship_filename,
                                interaction_filename, posts_filename)
    elif fileformat == "gml":
        c("publish gml", snapshoturi)
        return GmlRdfPublishing(snapshoturi, snapshotid, friendship_filename)


def publishAll(snapshoturis=None):
    if not snapshoturis:
        c("getting facebook snapshots, implementation needs verification TTM")
        uridict = {}
        for snapshoturi in P.get(None, a, NS.po.FacebookSnapshot,
                                 minimized=True):
            uridict[snapshoturi] = 0
            for rawFile in P.get(snapshoturi, NS.po.rawFile, strict=True,
                                 minimized=True):
                uridict[snapshoturi] += P.get(rawFile, NS.po.fileSize,
                                              minimized=True).toPython()
        snapshoturis = [i for i in list(uridict.keys()) if i.endswith(".gml")]
        snapshoturis.sort(key=lambda x: uridict[x])
    c("snapuris:", snapshoturis)
    count = 0
    for snapshoturi in snapshoturis:
        triplification_class = publishAny(snapshoturi)
        count += 1
    return triplification_class


def writePublishingReadme(final_path="./fb/"):
    nfriendship = P.rdf.query("SELECT (COUNT(?s) as ?cs) WHERE { \
        ?s a po:Snapshot . ?s facebook:isFriendship true . }")
    ninteraction = P.rdf.query("SELECT (COUNT(?s) as ?cs) WHERE { \
                               ?s a po:InteractionSnapshot }")
    nposts = P.rdf.query("SELECT (COUNT(?s) as ?cs) WHERE { \
                         ?s a po:PostsSnapshot }")
    nego = P.rdf.query("SELECT (COUNT(?s) as ?cs) WHERE { \
                       ?s a po:EgoSnapshot }")
    ngroup = P.rdf.query("SELECT (COUNT(?s) as ?cs) WHERE { \
                         ?s a po:GroupSnapshot }")
    nfriendship_group = nfriendship-nego
    c("got snapshot counts")

    body = """::: Open Linked Social Data publication\n
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
        nfriendship,
        ninteraction,
        nposts,
        nego,
        ngroup,
        nfriendship_group
        )

    # n participants
    nparticipants = P.rdf.query("SELECT (COUNT(?s) as ?cs) \
                    WHERE { ?s a po:Participant }")
    nparticipants_a0 = P.rdf.query("SELECT (COUNT(?s) as ?cs) WHERE { \
                            ?s a facebook:Participant . \
                            ?s po:name ?namefoo . \
                    }")
    nparticipants_a = nparticipants-nparticipants_a0
    nparticipants_unique = P.rdf.query("SELECT (COUNT(?numeric_id) as ?total) WHERE { \
                            ?sfoo a facebook:Participant . \
                            ?sfoo po:numericID ?numeric_id . \
                    }")
    nfriendships = P.rdf.query("SELECT (COUNT(?s) as ?cs) \
                    WHERE { ?s a facebook:Friendship }")
    nfriendships_a = P.rdf.query("SELECT (COUNT(?s) as ?cs) \
                    WHERE { ?s a facebook:Friendship . \
                            ?s po:snapshot ?snapfoo . \
                            ?snapfoo po:friendshipsAnonymized true . \
                    }")
    ninteractions = P.rdf.query("SELECT (COUNT(?s) as ?cs) \
                        WHERE { ?s a facebook:Interaction }")

    ninteractions_a = P.rdf.query("SELECT (COUNT(?s) as ?cs) \
                    WHERE { ?s a facebook:Interaction . \
                            ?s po:snapshot ?snapfoo . \
                            ?snapfoo po:interactionsAnonymized true }")

    nposts = P.rdf.query("SELECT (COUNT(?s) as ?cs) WHERE { \
                            ?s a facebook:Post . \
                    }")

    ntokens = sum(P.rdf.query("SELECT ?val WHERE { \
            ?foosnapshot facebook:nTokensOverall ?val . \
                    }"))
    nlikes = sum(P.rdf.query("SELECT ?val WHERE { \
            ?foosnapshot facebook:nLikes ?val . \
                    }"))
    ncomments = sum(P.rdf.query("SELECT ?val WHERE { \
            ?foosnapshot facebook:nComments ?val . \
                    }"))
    nreactions = nlikes+ncomments
    c("got entities counts")

    body += """
Overview of core entities:
{} participants ({} anonymized; {} unique numericID)
{} friendships ({} anonymized)
{} interaction ({} anonymized)
{} posts with a total of {} tokens and {} reaction counts \
    ({} comments+ {} likes)
:::""".format(nparticipants, nparticipants_a, nparticipants_unique,
              nfriendships, nfriendships_a,
              ninteractions, ninteractions_a,
              nposts, ntokens, nreactions, ncomments, nlikes)
    with open(final_path+"README", "w") as f:
        f.write(body)


def botData(filename):
    pass
if __name__ == "__main__":
    from .access import parseLegacyFiles
    c("started access")
    ss = parseLegacyFiles()
    c("started rendering")
    publishAll(ss)
    c("finished publication of all facebook files")
