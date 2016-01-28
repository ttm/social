import percolation as P, datetime, os, shutil
from percolation.rdf import NS, a
po=NS.po
c=P.check
class GmlRdfPublishing:
    """Produce a linked data publication tree from a GML file expressing a facebook ego friendship network.

    OUTPUTS:
    =======
    the tree in the directory final_path."""

    def __init__(self,snapshoturi,snapshotid,filename_friendships="foo.gml",\
            data_path="../data/facebook/",final_path="./fb/",umbrella_dir="facebook_networks/"):
        self.friendship_graph="social_facebook_friendships"
        self.meta_graph="social_facebook_meta"
        self.social_graph="social_facebook"
        P.context(self.friendship_graph,"remove")
        P.context(self.meta_graph,"remove")
        self.snapshotid=snapshotid
        self.snapshoturi=snapshoturi
        self.online_prefix="https://raw.githubusercontent.com/OpenLinkedSocialData/{}master/{}/".format(umbrella_dir,self.snapshotid)
        self.isego=True
        self.isgroup=False
        self.isfriendship=True
        self.isinteraction=False
        self.hastext=False
        self.friendships_anonymized=True
        self.nfriends=self.nfriendships=self.ninteracted=self.ninteractions=self.nposts=0

        friendship_network=x.read_gml(data_path+filename_friendships)
        locals_=locals().copy()
        for i in locals_:
            if i !="self":
                if isinstance(locals_[i],str):
                    exec("self.{}='{}'".format(i,locals_[i]))
                else:
                    exec("self.{}={}".format(i,locals_[i]))
        self.rdfFriendshipNetwork(friendship_network)
        self.makeMetadata()
        self.writeAllFB()
    def writeAllFB(self):
        c("started rendering of the snapshot publication. snapshotID:",self.snapshotid)
        self.final_path_="{}{}/".format(self.final_path,self.snapshotid)
        if not os.path.isdir(self.final_path_):
            os.mkdir(self.final_path_)
        #fnet,inet,mnet
        g=P.context(self.friendship_graph)
        g.namespace_manager.bind("po",po)
        g.serialize(self.final_path_+self.snapshotid+"Friendship.ttl","turtle"); c("ttl")
        g.serialize(self.final_path_+self.snapshotid+"Friendship.rdf","xml")
        c("serialized friendships")
        # get filesize and ntriples
        filesizerdf=os.path.getsize(self.final_path_+self.snapshotid+"Friendship.rdf")/(10**6)
        filesizettl=os.path.getsize(self.final_path_+self.snapshotid+"Friendship.ttl")/(10**6)
        ntriples=len(g)
        triples=[
                 (self.snapshoturi,po.friendshipXMLFileSizeMB,filesizerdf),
                 (self.snapshoturi,po.friendshipTTLFileSizeMB,filesizettl),
                 (self.snapshoturi,po.nFriendshipTriples,ntriples),
                 ]
        P.add(triples,context=self.meta_graph)
        g=P.context(self.meta_graph)
        ntriples=len(g)
        triples+=[
                 (self.snapshoturi,po.nMetaTriples,ntriples+1)      ,
                 ]
        P.add(triples,context=self.meta_graph)
        if not os.path.isdir(self.final_path_+"scripts"):
            os.mkdir(self.final_path_+"scripts")
        shutil.copy(S.PACKAGEDIR+"/../tests/triplify.py",self.final_path_+"scripts/triplify.py")
        # copia do base data
        if not os.path.isdir(self.final_path_+"base"):
            os.mkdir(self.final_path_+"base")
        shutil.copy(self.data_path+self.filename_friendships,self.final_path_+"base/")

        originals="base/{}".format(self.filename_friendships)
        tfriendship="""\n\n{nf} individuals with metadata {fvars}
and {nfsiendships constitute the friendship network in the RDF/XML file:
{frdf} \in the Turtle file: \n{fttl}
(anonymi {fan}).""".format(
                        nf=self.nfriends,fvars=str(self.friendsvars),
                        nfs=self.nfriendships,
                        frdf=self.frdf,fttl=self.fttl,
                        fan=self.friendships_anonymized,
                    )
        datetime_string=P.get(r.URIRef(self.snapshoturi),po.dateObtained,None,context="social_facebook")[2]

        with open(self.final_path_+"README","w") as f:
            f.write("""::: Open Linked Social Data publication
\nThis repository is a RDF data expression of the facebook
snapshot {snapid} collected around {date}.{tfriendship}{tinteraction}{tposts}
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

The script that rendered this data publication is on the script/ directory.\n:::""".format(
                snapid=self.snapshotid,date=datetime_string,
                        tfriendship=tfriendship,
                        tinteraction=tinteraction,
                        tposts=tposts,
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
    def rdfFriendshipNetwork(self,friendship_network):
        c("test variables to be the expected")
        for node in friendship_network.nodes():
            assert len(node)==6
            assert isinstance(node["id"],int)
            assert isinstance(node["agerank"],int)
            assert isinstance(node["wallcount"],int)
            assert isinstance(node["label"],str) and len(node["label"])
            assert isinstance(node["locale"],str) and len(node["locale"])
            assert isinstance(node["sex"],str) and len(node["sex"])
        for edge in friendship_network.edges(data=True):
            assert isinstance(edge[0],int)
            assert isinstance(edge[1],int)
            assert isinstance(edge[2],dict) and len(edge[2])==0
        c("create uris for each partcipant, with po:Participant#snapshoturi-localid")
        for node in friendship_network.nodes():
            localid=node["id"]
            agerank=node["agerank"]
            wallcount=node["wallcount"]
            name=node["label"]
            locale=node["locale"]
            sex=node["sex"]
            participant_uri=P.rdf.ic(po.Participant,self.snapshotid+"-"+localid,self.friendship_graph,self.snapshoturi)
            triples=[
                    (participant_uri,po.ageRank,node["agerank"]),
                    (participant_uri,po.wallCount,node["wallcount"]),
                    (participant_uri,po.name,node["label"]),
                    (participant_uri,po.locale,node["locale"]),
                    (participant_uri,po.sex,node["sex"]),
                    ]
            P.rdf.add(triples,context=self.friendship_graph)
        for localid1,localid2 in friendship_network.edges():
            friendship_uri=P.rdf.ic(po.Friendship,self.snapshotid+"-"+localid1+"-"+localid2,self.friendship_graph,self.snapshoturi)
            participant_uri1=po.Participant,self.snapshotid+"-"+localid1
            participant_uri2=po.Participant,self.snapshotid+"-"+localid2
            triples=[
                    (friendship_uri,po.member,participant_uri1),
                    (friendship_uri,po.member,participant_uri2),
                    ]
            P.rdf.add(triples,context=self.friendship_graph)
        self.nfriends=friendship_network.number_of_nodes()
        self.nfriendships=friendship_network.number_of_edges()
    def makeMetadata(self,friendship_network):
        triples=P.get(self.snapshoturi,None,None,"social_facebook")
        for rawfile in P.get(self.snapshoturi,po.rawFile,None,"social_facebook",strict=True,minimized=True):
            triples+=P.get(rawfile,None,None,"social_facebook")
        P.add(triples,context=self.meta_graph)

        self.ffile="base/"+self.filename_friendships
        self.frdf=self.snapshotid+"Friendship.rdf"
        self.fttl=self.snapshotid+"Friendship.ttl"
        triples=[
                (self.snapshoturi, po.onlineOriginalFriendshipFile,self.online_prefix+self.ffile),
                (self.snapshoturi, po.originalFriendshipFileName,self.ffile),
                (self.snapshoturi, po.onlineFriendshipXMLFile,self.online_prefix+self.frdf),
                (self.snapshoturi, po.onlineFriendshipTTLFile,self.online_prefix+self.fttl),
                (self.snapshoturi, po.friendshipXMLFileName,       self.frdf),
                (self.snapshoturi, po.friendshipTTLFileName,       self.fttl),
                (self.snapshoturi, po.nFriends,              self.nfriends),
                (self.snapshoturi, po.nFriendships,          self.nfriendships),
                (self.snapshoturi, po.friendshipsAnonymized ,self.friendships_anonymized),
                ]
        P.add(triples,context=self.meta_graph)
        P.rdf.triplesScaffolding(self.snapshoturi,
                [po.frienshipParticipantAttribute]*len(self.friendsvars),
                self.friendsvars,context=self.meta_graph)

        self.mrdf=self.snapshotid+"Meta.rdf"
        self.mttl=self.snapshotid+"Meta.ttl"

        self.desc="facebook network with snapshotID: {}\nsnapshotURI: {} \nisEgo: {}. isGroup: {}.".format(
                                                self.snapshotid,self.snapshoturi,self.isego,self.isgroup,)
        self.desc+="\nisFriendship: {}".format(self.isfriendship)
        self.desc+="; nFriends: {}; nFrienships: {}.".format(self.nfriends,self.nfriendships,)
        self.desc+="\nisInteraction: {}".format(self.isinteraction)
        self.desc+="\nisPost: {} (alias hasText: {})".format(self.hastext,self.hastext)
        triples=[
                (self.snapshoturi, po.triplifiedIn,      datetime.datetime.now()),
                (self.snapshoturi, po.triplifiedBy,      "scripts/"),
                (self.snapshoturi, po.donatedBy,         self.snapshotid[:-4]),
                (self.snapshoturi, po.availableAt,       self.online_prefix),
                (self.snapshoturi, po.onlineMetaXMLFile, self.online_prefix+self.mrdf),
                (self.snapshoturi, po.onlineMetaTTLFile, self.online_prefix+self.mttl),
                (self.snapshoturi, po.metaXMLFileName,   self.mrdf),
                (self.snapshoturi, po.metaTTLFileName,   self.mttl),
                (self.snapshoturi, po.acquiredThrough,   "Netvizz"),
                (self.snapshoturi, po.socialProtocolTag, "Facebook"),
                (self.snapshoturi, po.socialProtocol,    P.rdf.ic(po.Platform,"Facebook",self.meta_graph,self.snapshoturi)),
                (self.snapshoturi, NS.rdfs.comment,         self.desc),
                ]
        P.add(triples,self.meta_graph)
        #[NS.facebook.frienshipParticipantAttribute]*len(self.friendsvars)


        # write about the friendship network
        # write about the snapshot
        # and about files
        pass




