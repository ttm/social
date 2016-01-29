import percolation as P
from percolation.rdf import NS, a, po
c=P.check
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
    def __init__(self,snapshoturi,snapshotid,filenames=("foo.pickle",),\
            data_path="../data/twitter/",final_path="./twitter_snapshots/",umbrella_dir="twitter_snapshots/"):
        if len(filenames)==2:
            pickle_filename1=filenames[0]
            pickle_filename2=filenames[1]
        elif filenames[0].count("_")==1:
            pickle_filename1=filenames[0]
            pickle_filename2=""
        elif filenames[0].count("_")==2:
            pickle_filename1=""
            pickle_filename2=filenames[0]
        online_prefix="https://raw.githubusercontent.com/OpenLinkedSocialData/{}master/{}/".format(umbrella_dir,snapshotid)
        isego=True
        isgroup=False
        isfriendship=True
        isinteraction=False
        hastext=False
        friendships_anonymized=True
        tweet_graph="social_tweets"
        meta_graph="social_twitter_meta"
        social_graph="social_twitter"
        P.context(tweet_graph,"remove")
        P.context(meta_graph,"remove")
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
            tweets+=P.utils.pickleReadFile( self.data_path+self.pickle_filename1)[0]
        if self.pickle_filename2:
            tweets,fopen=P.utils.pickleReadChunks(data_path+self.pickle_filename2,tweets,5000) # limit chuck to 5k tweets
        chunck_count=0
        while tweets:
            c("rendering tweets, chunk:",chunck_count,"ntweets:",len(tweets))
            for tweet in tweets:
                tweetid=tweet["id_str"]
                tweeturi=P.rdf.ic(po.Tweet,tweetid,self.tweet_graph,self.snapshoturi)
                triples=[
                        (tweeturi,po.stringID,tweetid),
                        (tweeturi,po.message,tweet["text"]),
                        (tweeturi,po.retweetCount,tweet["retweet_count"]),
                        ]
                tweets=[]
                if self.pickle_filename2:
                    tweets,fopen=P.utils.pRead3(None,tweets,fopen)
                chunck_count+=1
    def makeMetadata(self):
        pass
    def writeAllTW(self):
        pass
