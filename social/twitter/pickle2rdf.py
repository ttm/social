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
        final_path_="{}{}/".format(final_path,snapshotid)
        for i in locals_:
            if i !="self":
                exec("self.{}={}".format(i,i))
        self.rdfTweets()
        self.makeMetadata()
        self.writeAllTW()
    def rdfTweets(self):
        tweets=[]
        if self.pickle_filename1:
            tweets+=pickleReadFile( self.data_path+self.pickle_filename1)[0]
        if self.pickle_filename2:
            tweets,fopen=pickleReadChunks(data_path+self.pickle_filename2,tweets,5000) # limit chuck to 5k tweets
        chunck_count=0
        self.tweets=tweets
        while tweets:
            c("rendering tweets, chunk:",chunck_count,"ntweets:",len(tweets))
            for tweet in tweets:
                tweeturi,triples=tweetTriples(tweet)
                if "retweeted_status" in dir(tweet):
                    tweeturi0,triples0=tweetTriples(tweet)
                    triples+=triples0
                    triples+=[(tweeturi,po.retweetOf,tweeturi0)]
                P.add(triples,context=self.tweet_graph)
                c("end of chunk:",chuck_count)
                self.writeTweets(chunk_count)
                c("chunck has been written")
                chunck_count+=1
                if self.pickle_filename2:
                    tweets,fopen=P.utils.pickleReadChuck(None,tweets,fopen)
    def writeTweets(self,chunk_count):
        filename=self.snapshotid+"Tweet{:05d}".format(ccount)
        filename_=self.final_path_+filename
        g=P.context(self.tweet_graph)
        g.namespace_manager.bind("po",po)
        g.serialize(filename+".ttl","turtle"); c("ttl")
        g.serialize(filename+".rdf","xml")


def tweetTriples(tweet):
    tweetid_=tweet["id_str"]
    userid_=tweet["user"]["id_str"]
    userid=self.snapshotid+"-"+userid_
    tweetid=userid_+"-"+tweetid_
    tweeturi=P.rdf.ic(po.Tweet,tweetid,self.tweet_graph,self.snapshoturi)
    useruri=P.rdf.ic(po.Tweet,userid,self.tweet_graph,self.snapshoturi)
    triples=[
            (tweeturi,po.stringID,tweetid),
            (tweeturi,po.createdAt,dateutil.parser.parse(tweet["created_at"])),
            (tweeturi,po.message,tweet["text"]),
            (tweeturi,po.retweetCount,tweet["retweet_count"]),
            (tweeturi,po.language,tweet["lang"]),
            (tweeturi,po.author,useruri),
            (useruri,po.stringID,tweet["user"]["screen_name"]),
            (useruri,po.numericID,tweet["user"]["id_str"]),
            (useruri,po.favouritesCount,tweet["user"]["favourites_count"]),
            (useruri,po.followersCount,tweet["user"]["followers_count"]),
            (useruri,po.followersCount,tweet["user"]["friends_count"]),
            (useruri,po.language,tweet["user"]["lang"]),
            (useruri,po.listedCount,tweet["user"]["listed_count"]),
            (useruri,po.name,tweet["user"]["name"]),
            (useruri,po.statusesCount,tweet["user"]["statuses_count"]),
            (useruri,po.createdAt,dateutil.parser.parse(tweet["user"]["created_at"])),
            (useruri,po.utcOffset,tweet["user"]["utc_offset"]),
            ]
    return triples
    def makeMetadata(self):
        pass
    def writeAllTW(self):
        pass

def twitterReadPickle(filename):
    """pickle read for the Dumper class"""
    objs=[]
    with open(filename,"rb") as f:
        while 1:
            try:
                objs.append(pickle.load(f))
            except EOFError:
                break
    return objs
def twitterReadPickleChunck(filename=None,tweets=[],fopen=None,ntweets=5000):
    """Read ntweets from filename or fopen and add them to tweets list"""
    if not fopen:
        f=open(filename,"rb")
    else:
        f=fopen
    #while len(tweets)<9900:
    while len(tweets)<ntweets:
        try:
            tweets+=pickle.load(f)
        except EOFError:
            break
    return tweets,f
