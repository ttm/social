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
    filenames.sort()
    triples=[
            (snapshoturi,      po.rawFile, "?fileurifoo"),
            (snapshoturi,      po.snapshotID, "?snapshotid"),
            ]
    snapshotid=P.get(triples)
    return PicklePublishing(snapshoturi,snapshotid,filenames)

class PicklePublishing:
    def __init__(self,snapshoturi,snapshotid,filename="foo.pickle",\
            data_path="../data/twitter/",final_path="./twitter_snapshots/",umbrella_dir="twitter_snapshots/"):
        if len(filenames)==2:
            pickle_filename1=filenames[0]
            pickle_filename2=filenames[1]
        elif filenames[0].count("_")==0:
            pickle_filename1=filenames[0]
            pickle_filename2=""
        elif filenames[0].count("_")==1:
            pickle_filename1=""
            pickle_filename2=filenames[0]
        online_prefix="https://raw.githubusercontent.com/OpenLinkedSocialData/{}master/{}/".format(umbrella_dir,self.snapshotid)
        isego=True
        isgroup=False
        isfriendship=True
        isinteraction=False
        hastext=False
        friendships_anonymized=True
        tweets_graph="social_tweets"
        meta_graph="social_twitter_meta"
        social_graph="social_twitter"
        P.context(self.tweets_graph,"remove")
        P.context(self.meta_graph,"remove")
        locals_=locals().copy()
        for i in locals_:
            if i !="self":
                exec("self.{}={}".format(i,i))
        self.rdfTweets()
        self.makeMetadata()
        self.writeAllTW()
    def rdfTweets(self):
        tweets=[]
        if self.pickle_filename1:
            tweets+=P.utils.pRead2( self.data_path+self.fname)[0]
        if self.pickle_filename2:
            tweets,fopen=P.utils.pRead3(data_path+fname__,tweets,5000) # limit chuck to 5k tweets
        

        with open(data_path+filename) as f:
            lines=f.readlines()
        friendship_network=x.readwrite.gml.parse_gml_lines(lines,"id",None)
        locals_=locals().copy()
        for i in locals_:
            if i !="self":
                exec("self.{}={}".format(i,i))
        self.rdfFriendshipNetwork(friendship_network)
        self.makeMetadata()
        self.writeAllFB()

