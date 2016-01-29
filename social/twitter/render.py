def publishAll(snapshoturis=None):
    """express tweets as RDF for publishing"""
    if not snapshoturis:
        c("getting facebook snapshots, implementation needs verification TTM")
        uridict={}
        for snapshoturi in P.get(None,a,NS.po.TwitterSnapshot,minimized=True):
            uridict[snapshoturi]=0
            for rawFile in P.get(snapshoturi,NS.po.rawFile,strict=True,minimized=True):
                uridict[snapshoturi]+=P.get(rawFile,NS.po.fileSize,minimized=True).toPython()
        snapshoturis=[i for i in list(uridict.keys()) if i.endswith(".gml")]
        snapshoturis.sort(key=lambda x: uridict[x])
    for snapshoturi in snapshoturis:
        triplification_class=publishAny(snapshoturi)
        count+=1
    #writePublishingReadme()
    return triplification_class

def publishAny(snapshoturi):
    # publish to umbrelladir
    triples=[
            (snapshoturi,      po.rawFile, "?fileurifoo"),
            ("?fileurifoo",    po.fileName, "?filename"),
            ]
    filenames=P.get(triples)
    triples=[
            (snapshoturi,      po.rawFile, "?fileurifoo"),
            (snapshoturi,      po.snapshotID, "?snapshotid"),
            ]
    snapshotid=P.get(triples)
    return PicklePublishing(snapshoturi,snapshotid,filename)

class PicklePublishing:
    def __init__(self,snapshoturi,snapshotid,filename="foo.pickle",\
            data_path="../data/twitter/",final_path="./twitter_snapshots/",umbrella_dir="twitter_snapshots/"):
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

        #friendship_network=x.read_gml(data_path+filename_friendships)
        with open(data_path+filename_friendships) as f:
            lines=f.readlines()
        friendship_network=x.readwrite.gml.parse_gml_lines(lines,"id",None)
        locals_=locals().copy()
        for i in locals_:
            if i !="self":
                exec("self.{}={}".format(i,i))
        self.rdfFriendshipNetwork(friendship_network)
        self.makeMetadata()
        self.writeAllFB()

