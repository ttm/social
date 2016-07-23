import os
import shutil
import networkx as x
import rdflib as r
import datetime
import percolation as P
import social as S
from percolation.rdf import NS
from .read import trans
po = NS.po
c = P.check


class GmlRdfPublishing:
    """Produce a linked data publication tree from a GML file

    expressing a facebook ego friendship network.

    OUTPUTS:
    =======
    the tree in the directory final_path."""

    def __init__(self, snapshoturi, snapshotid, filename_friendships="foo.gml",
                 data_path="../data/facebook/",
                 final_path="./facebook_snapshots/",
                 umbrella_dir="facebook_snapshots/"):
        self.friendship_graph = "social_facebook_friendships"
        self.meta_graph = "social_facebook_meta"
        self.social_graph = "social_facebook"
        P.context(self.friendship_graph, "remove")
        P.context(self.meta_graph, "remove")
        self.snapshotid = snapshotid
        self.snapshoturi = snapshoturi
        self.online_prefix = "https://raw.githubusercontent.com/\
            OpenLinkedSocialData/{}master/{}/".format(umbrella_dir,
                                                      self.snapshotid)
        participant_uri = P.rdf.ic(po.FacebookSnapshot, self.snapshotid,
                self.friendship_graph)
        self.isego = True
        self.isgroup = False
        self.isfriendship = True
        self.isinteraction = False
        self.hastext = False
        self.friendships_anonymized = True
        # friendship_network=x.read_gml(data_path+filename_friendships)
        with open(data_path+filename_friendships) as f:
            lines = f.readlines()
        friendship_network = x.readwrite.gml.parse_gml_lines(lines, "id", None)
        locals_ = locals().copy()
        for i in locals_:
            if i != "self":
                exec("self.{}={}".format(i, i))
        self.rdfFriendshipNetwork(friendship_network)
        self.makeMetadata()
        self.writeAllFB()

    def writeAllFB(self):
        c("started rendering of the snapshot publication. snapshotID:",
          self.snapshotid)
        self.final_path_ = "{}{}/".format(self.final_path, self.snapshotid)
        if not os.path.isdir(self.final_path_):
            os.mkdir(self.final_path_)
        g = P.context(self.friendship_graph)
        g.namespace_manager.bind("po", po)
        g.serialize(self.final_path_+self.snapshotid+"Friendship.ttl", "turtle")
        c("ttl")
        g.serialize(self.final_path_+self.snapshotid+"Friendship.rdf", "xml")
        c("serialized friendships")
        # get filesize and ntriples
        filesizerdf = os.path.getsize(self.final_path_+self.snapshotid +
                                      "Friendship.rdf")/(10**6)
        filesizettl = os.path.getsize(self.final_path_+self.snapshotid +
                                      "Friendship.ttl")/(10**6)
        ntriples = len(g)
        triples = [
                 (self.snapshoturi, po.friendshipXMLFileSizeMB, filesizerdf),
                 (self.snapshoturi, po.friendshipTTLFileSizeMB, filesizettl),
                 (self.snapshoturi, po.nFriendshipTriples, ntriples),
                 ]
        P.add(triples, context=self.meta_graph)
        g = P.context(self.meta_graph)
        ntriples = len(g)
        triples.append(
                 (self.snapshoturi, po.nMetaTriples, ntriples+1),
        )
        g.serialize(self.final_path_+self.snapshotid+"Meta.ttl", "turtle")
        c("ttl")
        g.serialize(self.final_path_+self.snapshotid+"Meta.rdf", "xml")
        c("serialized meta")

        if not os.path.isdir(self.final_path_+"scripts"):
            os.mkdir(self.final_path_+"scripts")
        shutil.copy(S.PACKAGEDIR+"/../tests/triplify.py",
                    self.final_path_+"scripts/triplify.py")
        # copia do base data
        if not os.path.isdir(self.final_path_+"base"):
            os.mkdir(self.final_path_+"base")
        shutil.copy(self.data_path+self.filename_friendships,
                    self.final_path_+"base/")

        originals = "base/{}".format(self.filename_friendships)
        tfriendship = """\n\n{nf} individuals with metadata {fvars}
and {nfs} friendships constitute the friendship network in the RDF/XML file:
{frdf} \in the Turtle file: \n{fttl}
(anonymized {fan}).""".format(
                        nf=self.nfriends, fvars=str(self.friendsvars),
                        nfs=self.nfriendships,
                        frdf=self.frdf, fttl=self.fttl,
                        fan=self.friendships_anonymized,
                    )
        datetime_string = P.get(self.snapshoturi, po.dateObtained, None,
                                context=self.social_graph)[2]

        with open(self.final_path_+"README", "w") as f:
            f.write("""::: Open Linked Social Data publication
\nThis repository is a RDF data expression of the facebook
snapshot {snapid} collected around {date}.{tfriendship}
\nMetadata for discovery in the RDF/XML file:
{mrdf} \nor in the Turtle file:\n{mttl}
\nOriginal file(s):
{origs}
\nEgo network: {ise}
Group network: {isg}
Friendship network: {isf}
Interaction network: {isi}
Has text/posts: {ist}
\nAll files should be available at the git repository:
{ava}
\n{desc}

The script that rendered this data publication is on the script/ \
                    directory.\n:::""".format(
                snapid=self.snapshotid, date=datetime_string,
                tfriendship=tfriendship,
                mrdf=self.mrdf,
                mttl=self.mttl,
                origs=originals,
                ise=self.isego,
                isg=self.isgroup,
                isf=self.isfriendship,
                isi=self.isinteraction,
                ist=self.hastext,
                ava=self.online_prefix,
                desc=self.desc
                ))

    def rdfFriendshipNetwork(self, friendship_network):
        for node_ in friendship_network.nodes(data=True):
            node = node_[1]
            assert len(node) == 5 or (("RicardoFabbri18022013" in
                                       self.snapshotid) and (len(node) == 4))
            assert isinstance(node_[0], int)
            assert isinstance(node["agerank"], int)
            assert isinstance(node["wallcount"], int)
            assert isinstance(node["label"], str) and len(node["label"])
            assert ("RicardoFabbri18022013" in self.snapshotid) or \
                isinstance(node["locale"], str)
            assert isinstance(node["sex"], str)
        for edge in friendship_network.edges(data=True):
            assert isinstance(edge[0], int)
            assert isinstance(edge[1], int)
            assert isinstance(edge[2], dict) and len(edge[2]) == 0
        self.friendsvars = ["name", "ageRank", "wallCount", "sex"]
        if ("RicardoFabbri18022013" not in self.snapshotid):
            self.friendsvars.append("locale")
        c("create uris for each partcipant, \
          with po:Participant#snapshoturi-localid")
        count = 0
        for node_ in friendship_network.nodes(data=True):
            node = node_[1]
            localid = str(node_[0])
            participant_uri = P.rdf.ic(po.Participant, self.snapshotid +
                                       "-"+localid, self.friendship_graph,
                                       self.snapshoturi)
            triples = [(participant_uri, eval('po.'+trans[i]), node[i])
                       for i in node]
            if localid == '38':
                c(node,triples)
            P.rdf.add(triples, context=self.friendship_graph)
            count += 1
            if count % 300 == 0:
                c("participants:", count)
        count = 0
        for localid1_, localid2_ in friendship_network.edges():
            localid1, localid2 = str(localid1_), str(localid2_)
            friendship_uri = P.rdf.ic(po.Friendship,
                                      self.snapshotid+"-"+localid1+"-"+localid2,
                                      self.friendship_graph, self.snapshoturi)
            uids = [r.URIRef(po.Participant+"#{}-{}".format(
                self.snapshotid, i)) for i in (localid1, localid2)]
            P.rdf.triplesScaffolding(friendship_uri, [po.member]*2,
                                     uids, self.friendship_graph)
            count += 1
            if count % 1000 == 0:
                c("friendships:", count)
        self.nfriends = friendship_network.number_of_nodes()
        self.nfriendships = friendship_network.number_of_edges()

    def makeMetadata(self):
        triples = P.get(self.snapshoturi, None, None, self.social_graph)
        # for rawfile in P.get(self.snapshoturi, po.rawFile, None,
        #                      self.social_graph, strict=True, minimized=True):
        #     triples.extend(P.get(rawfile, None, None, self.social_graph))
        P.add(triples, context=self.meta_graph)

        self.ffile = "base/"+self.filename_friendships
        self.frdf = self.snapshotid+"Friendship.rdf"
        self.fttl = self.snapshotid+"Friendship.ttl"
        triples = [
                # (self.snapshoturi, po.onlineOriginalFriendshipFile,
                #  self.online_prefix+self.ffile),
                # (self.snapshoturi, po.originalFriendshipFileName, self.ffile),
                # (self.snapshoturi, po.onlineFriendshipXMLFile,
                #  self.online_prefix+self.frdf),
                # (self.snapshoturi, po.onlineFriendshipTTLFile,
                #  self.online_prefix+self.fttl),
                # (self.snapshoturi, po.friendshipXMLFileName, self.frdf),
                # (self.snapshoturi, po.friendshipTTLFileName, self.fttl),
                # (self.snapshoturi, po.numberOfFriends,              self.nfriends),
                # (self.snapshoturi, po.numberOfFriendships,          self.nfriendships),
                (self.snapshoturi, po.friendshipsAnonymized, self.friendships_anonymized),
                ]
        P.add(triples, context=self.meta_graph)
        # P.rdf.triplesScaffolding(self.snapshoturi,
        #                          [po.frienshipParticipantAttribute] *
        #                          len(self.friendsvars),
        #                          self.friendsvars, context=self.meta_graph)
        self.mrdf = self.snapshotid+"Meta.rdf"
        self.mttl = self.snapshotid+"Meta.ttl"
        self.desc = "facebook network with snapshotID: {}\nsnapshotURI: {} \n\
            isEgo: {}. isGroup: {}.".format(self.snapshotid, self.snapshoturi,
                                            self.isego, self.isgroup)
        self.desc += "\nisFriendship: {}".format(self.isfriendship)
        # self.desc += "; numberOfFriends: {}; numberOfFrienships: {}."\
        #     .format(self.nfriends, self.nfriendships)
        self.desc += "\nisInteraction: {}".format(self.isinteraction)
        self.desc += "\nisPost: {} (hasText)".format(self.hastext)
        triples = [
                (self.snapshoturi, po.triplifiedIn, datetime.datetime.now()),
                # (self.snapshoturi, po.triplifiedBy, "scripts/"),
                # (self.snapshoturi, po.donatedBy, self.snapshotid[:-4]),
                # (self.snapshoturi, po.availableAt, self.online_prefix),
                # (self.snapshoturi, po.onlineMetaXMLFile,
                #  self.online_prefix+self.mrdf),
                # (self.snapshoturi, po.onlineMetaTTLFile,
                #  self.online_prefix+self.mttl),
                # (self.snapshoturi, po.metaXMLFileName,   self.mrdf),
                # (self.snapshoturi, po.metaTTLFileName,   self.mttl),
                (self.snapshoturi, po.acquiredThrough,   "Netvizz"),
                (self.snapshoturi, po.socialProtocol, "Facebook"),
                # (self.snapshoturi, po.socialProtocolTag, "Facebook"),
                # (self.snapshoturi, po.socialProtocol,
                #  P.rdf.ic(po.Platform, "Facebook", self.meta_graph,
                #           self.snapshoturi)),
                (self.snapshoturi, po.comment,         self.desc),
                ]
        P.add(triples, self.meta_graph)
