import percolation as P, pickle, dateutil, nltk as k
from percolation.rdf import NS, a, po
c=P.check
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
        isego=False
        isgroup=True
        isfriendship=False
        isinteraction=True
        hastext=True
        interactions_anonymized=False
        tweet_graph="social_tweets"
        meta_graph="social_twitter_meta"
        social_graph="social_twitter"
        P.context(tweet_graph,"remove")
        P.context(meta_graph,"remove")
        final_path_="{}{}/".format(final_path,snapshotid)
        tweet_rdf=[]
        tweet_ttl=[]
        nparticipants=0
        nretweets=0
        ntweets=0
        nreplies=0
        anonymous_user_count=0
        anonymous_tweet_count=0
        dates1=[]
        dates2=[]
        locals_=locals().copy()
        for i in locals_:
            if i !="self":
                exec("self.{}={}".format(i,i))
        self.rdfTweets()
        self.makeMetadata()
        self.writeAllTW()
    def makeMetadata(self):
        triples=P.get(self.snapshoturi,None,None,self.social_graph)
        for rawfile in P.get(self.snapshoturi,po.rawFile,None,self.social_graph,strict=True,minimized=True):
            triples+=P.get(rawfile,None,None,self.social_graph)
        P.add(triples,context=self.meta_graph)
        triples=[
                (self.snapshoturi, po.nParticipants,           self.nparticipants),
                (self.snapshoturi, po.nTweets,                 self.ntweets),
                (self.snapshoturi, po.nReplies,              self.nreplies),
                (self.snapshoturi, po.nRetweets,               self.nretweets),
                ]
        P.add(triples,context=self.meta_graph)
        P.rdf.triplesScaffolding(self.snapshoturi,
                [po.tweetParticipantAttribute]*len(self.participantvars),
                self.participantvars,context=self.meta_graph)
        P.rdf.triplesScaffolding(self.snapshoturi,
                [po.tweetXMLFilename]*len(self.tweet_rdf)+[po.tweetTTLFilename]*len(self.tweet_ttl),
                self.tweet_rdf+self.tweet_ttl,context=self.meta_graph)
        P.rdf.triplesScaffolding(self.snapshoturi,
                [po.onlineTweetXMLFile]*len(self.tweet_rdf)+[po.onlineTweetTTLFile]*len(self.tweet_ttl),
                [self.online_prefix+i for i in self.tweet_rdf+self.tweet_ttl],context=self.meta_graph)
        self.ffile=["base/"+i for i in self.filenames]
        P.rdf.triplesScaffolding(self.snapshoturi,
                [po.originalTweetFileName]*len(self.ffile),
                self.ffile,context=self.meta_graph)
        P.rdf.triplesScaffolding(self.snapshoturi,
                [po.onlineOriginalTweetFile]*len(self.ffile),
                [self.online_prefix+i for i in self.ffile],context=self.meta_graph)

        self.mrdf=self.snapshotid+"Meta.rdf"
        self.mttl=self.snapshotid+"Meta.ttl"
        self.desc="facebook network with snapshotID: {}\nsnapshotURI: {} \nisEgo: {}. isGroup: {}.".format(
                                                self.snapshotid,self.snapshoturi,self.isego,self.isgroup,)
        self.desc+="\nisFriendship: {}".format(self.isfriendship)
        self.desc+="\nisInteraction: {}".format(self.isinteraction)
        self.desc+="; nParticipants: {}; nInteractions: {} (responses+retweets).".format(self.nparticipants,self.nresponses+self.nretweets,)
        self.desc+="\nisPost: {} (alias hasText: {})".format(self.hastext,self.hastext)
        self.desc+="\n nTweets: {}; nResponses: {}; nRetweets: {}.".format(self.nfriends,self.nfriendships,)
        self.desc+="\n nTokens: {}; mTokens: {}; dTokens: {};"
        seld.desc+="\n nChars: {}; mChars: {}; dChars: {}.".format(self.nfriends,self.nfriendships,)
        self.desc+="\n mTokens: {}; nChars: {}; nLinks: {}.".format(self.nfriends,self.nfriendships,)
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
    def writeAllTW(self):
        pass
    def rdfTweets(self):
        tweets=[]
        if self.pickle_filename1:
            tweets+=readPickleTweetFile( self.data_path+self.pickle_filename1)[0]
        if self.pickle_filename2:
            tweets,fopen=readPickleTweetChunk(data_path+self.pickle_filename2,tweets,None,5000) # limit chuck to 5k tweets
        chunk_count=0
        self.tweets=tweets # for probing only, remove to release memory
        while tweets:
            c("rendering tweets, chunk:",chunk_count,"ntweets:",len(tweets))
            for tweet in tweets:
                tweeturi,triples=self.tweetTriples(tweet)
                if "retweeted_status" in dir(tweet):
                    tweeturi0,triples0=self.tweetTriples(tweet)
                    triples+=triples0
                    triples+=[(tweeturi,po.retweetOf,tweeturi0)]
                    if self.ntweets%100==0:
                        c("rendered",self.ntweets,"tweets")
                P.add(triples,context=self.tweet_graph)
            c("end of chunk:",chunk_count, "ntriples:",len(triples))
            self.writeTweets(chunk_count)
            c("chunk has been written")
            chunk_count+=1
            if self.pickle_filename2:
                tweets,fopen=readPickleTweetChuck(None,None,fopen,5000)
            else:
                tweets=[]
    def writeTweets(self,chunk_count):
        filename=self.snapshotid+"Tweet{:05d}".format(chunk_count)
        filename_=self.final_path_+filename
        g=P.context(self.tweet_graph)
        g.namespace_manager.bind("po",po)
        tttl=filename+".ttl"
        trdf=filename+".rdf"
        g.serialize(tttl,"turtle"); c("ttl")
        g.serialize(trdf+".rdf","xml")
        self.tweet_ttl+=[tttl]
        self.tweet_rdf+=[trdf]
    def tweetTriples(self,tweet):
        tweetid_=tweet["id_str"]
        # if tweetid_ in triples
        userid_=tweet["user"]["id_str"]
        userid=self.snapshotid+"-"+userid_
        useruri=P.rdf.ic(po.Participant,userid,self.tweet_graph,self.snapshoturi)
        tweetid=userid_+"-"+tweetid_
        tweeturi=P.rdf.ic(po.Tweet,tweetid,self.tweet_graph,self.snapshoturi)
        triples=[]
        if tweet["in_reply_to_user_id_str"] or tweet["in_reply_to_status_id_str"]:
            self.nreplies+=1                
            if tweet["in_reply_to_status_id_str"]:
                userid_reply=self.snapshotid+"-"+tweet["in_reply_to_user_id_str"]
                useruri_reply=P.rdf.ic(po.Participant,userid_reply,self.tweet_graph,self.snapshoturi)
                if not P.get(useruri_reply,po.numericID,None,context=self.tweet_graph): # new user
                    self.nparticipants+=1
                    triples+=[(useruri_reply,po.numericID,userid_reply)]
            else:
                userid_reply=self.snapshotid+"-anonymous-"+str(self.anonymous_user_count)
                useruri_reply=P.rdf.ic(po.Participant,userid_reply,self.tweet_graph,self.snapshoturi)
                self.anonymous_user_count+=1
                triples+=[(useruri_reply,po.anonymous,True)]
            if tweet["in_reply_to_status_id_str"]:
                tweetid_reply=userid_reply+"-"+tweet["in_reply_to_status_id_str"]
                tweeturi_reply=P.rdf.ic(po.Tweet,tweetid_reply,self.tweet_graph,self.snapshoturi)
                if not P.get(tweeturi_reply,po.numericID,None,context=self.tweet_graph): # new message
                    self.ntweets+=1
                    triples+=[(tweeturi_reply,po.numericID,tweetid_reply)]
            else:
                tweetid_reply=self.snapshotid+"-anonymous-"+str(self.anonymous_tweet_count)
                tweeturi_reply=P.rdf.ic(po.Tweet,tweetid_reply,self.tweet_graph,self.snapshoturi)
                self.anonymous_tweet_count+=1
                triples+=[(tweeturi_reply,po.anonymous,True)]
            triples+=[
                     (tweeturi,po.inReplyToUser,useruri_reply),
                     (tweeturi,po.inReplyToStatus,tweeturi_reply),
                     ]
        elif tweet["in_reply_to_user_id_str"] and not tweet["in_reply_to_status_id_str"]:
            self.nreplies+=1                
            raise ValueError("reply have no status id")
        elif not tweet["in_reply_to_user_id_str"] and tweet["in_reply_to_status_id_str"]:
            raise ValueError("reply have no user id")
        query=[
              ("?uri",a,po.Tweet),
              ("?uri",po.stringID,tweetid_)
              ]
        tweet_known=P.get(query,context=self.tweet_graph)
        query=[
              ("?uri",a,po.Participant),
              ("?uri",po.numericID,tweet["user"]["id_str"])
              ]
        participant_known=P.get(query,context=self.tweet_graph)
        if not tweet_known:
            self.ntweets+=1
        if not participant_known:
            self.nparticipants+=1

        self.mcharsposts=n.mean(nchars_all)
        self.dcharsposts=n.std(  nchars_all)
        self.totalchars=n.sum(   nchars_all)
        self.mtokensposts=n.mean(ntokens_all)
        self.dtokensposts=n.std( ntokens_all)
        self.totaltokens=n.sum(  ntokens_all)
        tweet_text=tweet["text"]
        nchars=len(tweet_text)
        ntokens=len(k.tokenize.wordpunct_tokenize(tweet_text))
        nchars_all+=[nchars]
        ntokens_all+=[ntokens]
        hashtags=[]
        links=[]
        user_mentions=[]
        media=[]
        #symbols?
        triples+=[
                 (tweeturi,po.nChars,nchars),
                 (tweeturi,po.nTokens,ntokens),
                 (tweeturi,po.stringID,tweetid),
                 (tweeturi,po.createdAt,dateutil.parser.parse(tweet["created_at"])),
                 (tweeturi,po.message,tweet["text"]),
                 (tweeturi,po.retweetCount,tweet["retweet_count"]),
                 (tweeturi,po.language,tweet["lang"]),
                 (tweeturi,po.author,useruri),
                 (tweeturi,po.author,useruri),
                 (useruri,po.stringID,tweet["user"]["screen_name"]),
                 (useruri,po.numericID,tweet["user"]["id_str"]),
                 (useruri,po.favouritesCount,tweet["user"]["favourites_count"]),
                 (useruri,po.followersCount,tweet["user"]["followers_count"]),
                 (useruri,po.friendsCount,tweet["user"]["friends_count"]),
                 (useruri,po.language,tweet["user"]["lang"]),
                 (useruri,po.listedCount,tweet["user"]["listed_count"]),
                 (useruri,po.name,tweet["user"]["name"]),
                 (useruri,po.statusesCount,tweet["user"]["statuses_count"]),
                 (useruri,po.createdAt,dateutil.parser.parse(tweet["user"]["created_at"])),
                 (useruri,po.utcOffset,tweet["user"]["utc_offset"]),
                 ]
        triples=[triple for triple in triples if triple[2]]
        self.participantvars=["stringID","numericID","favouritesCount","followersCount","friendsCount",\
                "language","listedCount","name","statusesCount","createdAt","utfOffset"]
        return tweeturi,triples

def readPickleTweetFile(filename):
    """pickle read for the Dumper class"""
    objs=[]
    with open(filename,"rb") as f:
        while 1:
            try:
                objs.append(pickle.load(f))
            except EOFError:
                break
    return objs
def readPickleTweetChunck(filename=None,tweets=[],fopen=None,ntweets=5000):
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
