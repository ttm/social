import percolation as P, social as S, numpy as n, pickle, dateutil, nltk as k, os, datetime, shutil, rdflib as r
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

        participantvars=["stringID","numericID","favouritesCount","followersCount","friendsCount",\
                "language","listedCount","name","statusesCount","createdAt","utfOffset"]
        self.tweetvars=["author","nChars","nTokens","stringID","createdAt","message","retweetCount","language","inReplyToTweet","retweetOf"]
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
        online_prefix="https://raw.githubusercontent.com/OpenLinkedSocialData/{}master/{}/".format(umbrella_dir,snapshotid)
        dates=[]; tweet_rdf=[]; tweet_ttl=[]; nchars_all=[]; ntokens_all=[]
        ntriples=nhashtags=nmedia=nlinks=nuser_mentions=nparticipants=nretweets=ntweets=nreplies=anonymous_user_count=anonymous_tweet_count=0
        locals_=locals().copy(); del locals_["self"]
        for i in locals_:
            exec("self.{}={}".format(i,i))
        self.rdfTweets()
        self.makeMetadata()
        self.writeAllTW()
    def makeMetadata(self):
        triples=P.get(self.snapshoturi,None,None,self.social_graph)
        for rawfile in P.get(self.snapshoturi,po.rawFile,None,self.social_graph,strict=True,minimized=True):
            triples+=P.get(rawfile,None,None,self.social_graph)
        self.totalchars=sum(self.nchars_all)
        self.mcharstweets=n.mean(self.nchars_all)
        self.dcharstweets=n.std(self.nchars_all)
        self.totaltokens=sum(self.ntokens_all)
        self.mtokenstweets=n.mean(self.ntokens_all)
        self.dtokenstweets=n.std(self.ntokens_all)
        P.add(triples,context=self.meta_graph)
        triples=[
                (self.snapshoturi, po.nParticipants,           self.nparticipants),
                (self.snapshoturi, po.nTweets,                 self.ntweets),
                (self.snapshoturi, po.nReplies,              self.nreplies),
                (self.snapshoturi, po.nRetweets,               self.nretweets),
                (self.snapshoturi, po.nCharsOverall, self.totalchars),
                (self.snapshoturi, po.mCharsOverall, self.mcharstweets),
                (self.snapshoturi, po.dCharsOverall, self.dcharstweets),
                (self.snapshoturi, po.nTokensOverall, self.totaltokens),
                (self.snapshoturi, po.mTokensOverall, self.mtokenstweets),
                (self.snapshoturi, po.dTokensOverall, self.dtokenstweets),
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
        self.desc="twitter dataset with snapshotID: {}\nsnapshotURI: {} \nisEgo: {}. isGroup: {}.".format(
                                                self.snapshotid,self.snapshoturi,self.isego,self.isgroup,)
        self.desc+="\nisFriendship: {}".format(self.isfriendship)
        self.desc+="\nisInteraction: {}".format(self.isinteraction)
        self.desc+="; nParticipants: {}; nInteractions: {} (responses+retweets+user mentions).".format(self.nparticipants,self.nreplies+self.nretweets+self.nuser_mentions,)
        self.desc+="\nisPost: {} (alias hasText: {})".format(self.hastext,self.hastext)
        self.desc+="\n nTweets: {};".format(self.ntweets)
        self.desc+="nReplies: {}; nRetweets: {}; nUserMentions: {}.".format(self.nreplies,self.nretweets,self.nuser_mentions)
        self.desc+="\n nTokens: {}; mTokens: {}; dTokens: {};".format(self.totaltokens,self.mtokenstweets,self.dtokenstweets)
        self.desc+="\n nChars: {}; mChars: {}; dChars: {}.".format(self.totalchars,self.mcharstweets,self.dcharstweets)
        self.desc+="\n mHashtahs: {}; nMedia: {}; nLinks: {}.".format(self.nhashtags,self.nmedia,self.nlinks)
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
                (self.snapshoturi, po.nTriples,         self.ntriples),
                (self.snapshoturi, NS.rdfs.comment,         self.desc),
                ]
        P.add(triples,self.meta_graph)
    def writeAllTW(self):
        # write meta and readme with self.desc, finished.
        g=P.context(self.meta_graph)
        ntriples=len(g)
        triples=[
                 (self.snapshoturi,po.nMetaTriples,ntriples)      ,
                 ]
        P.add(triples,context=self.meta_graph)
        g.namespace_manager.bind("po",po)
        g.serialize(self.final_path_+self.snapshotid+"Meta.ttl","turtle"); c("ttl")
        g.serialize(self.final_path_+self.snapshotid+"Meta.rdf","xml")
        c("serialized meta")
        # copia o script que gera este codigo
        if not os.path.isdir(self.final_path_+"scripts"):
            os.mkdir(self.final_path_+"scripts")
        shutil.copy(S.PACKAGEDIR+"/../tests/triplify.py",self.final_path_+"scripts/triplify.py")
        # copia do base data
        tinteraction="""\n\n{} individuals with metadata {}
and {} interactions (retweets: {}, replies: {}, user_mentions: {}) 
constitute the interaction 
network in the RDF/XML file:
{}
and the Turtle file:
{}
(anonymized: {}).""".format( self.nparticipants,str(self.participantvars),
                    self.nretweets+self.nreplies+self.nuser_mentions,self.nretweets,self.nreplies,self.nuser_mentions,
                    self.tweet_rdf,
                    self.tweet_ttl,
                    self.interactions_anonymized)
        for filename in self.filenames:
            shutil.copy(self.data_path+filename,self.final_path_+"base/")
        tposts="""\n\nThe dataset consists ofd {} tweets with metadata {}
{:.3f} characters in average (std: {:.3f}) and total chars in snapshot: {}
{:.3f} tokens in average (std: {:.3f}) and total tokens in snapshot: {}""".format(
                        str(self.tweetvars),
                        self.ntweets,self.mcharstweets,self.dcharstweets,self.totalchars,
                        self.mtokenstweets,self.dtokenstweets,self.totaltokens,
                        )
        date1=min(self.dates)
        date2=max(self.dates)
        with open(self.final_path_+"README","w") as f:
            f.write("""::: Open Linked Social Data publication
\nThis repository is a RDF data expression of the twitter
snapshot {snapid} with tweets from {date1} to {date2}.{tinteraction}{tposts}
\nMetadata for discovery in the RDF/XML file:
{mrdf} \nor in the Turtle file:\n{mttl}
\nOriginal file(s):
\nEgo network: {ise}
Group network: {isg}
Friendship network: {isf}
Interaction network: {isi}
Has text/posts: {ist}
\nAll files should be available at the git repository:
{ava}
\n{desc}

The script that rendered this data publication is on the script/ directory.\n:::""".format(
                snapid=self.snapshotid,date1=date1.isoformat(),date2=date2.isoformat(),
                        tinteraction=tinteraction,
                        tposts=tposts,
                        mrdf=self.mrdf,
                        mttl=self.mttl,
                        ise=self.isego,
                        isg=self.isgroup,
                        isf=self.isfriendship,
                        isi=self.isinteraction,
                        ist=self.hastext,
                        ava=self.online_prefix,
                        desc=self.desc
                        ))


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
                self.ntriples+=len(triples)
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
        if not os.path.isdir(self.final_path):
            os.mkdir(self.final_path)
        if not os.path.isdir(self.final_path_):
            os.mkdir(self.final_path_)
        filename=self.snapshotid+"Tweet{:05d}".format(chunk_count)
        filename_=self.final_path_+filename
        g=P.context(self.tweet_graph)
        g.namespace_manager.bind("po",po)
        tttl=filename_+".ttl"
        trdf=filename_+".rdf"
        g.serialize(tttl,"turtle"); c("ttl")
        g.serialize(trdf+".rdf","xml")
        self.tweet_ttl+=[tttl]
        self.tweet_rdf+=[trdf]

    def tweetTriples(self,tweet):
        triples=[]
        userid, useruri, triples_=self.userTriples(tweet)
        triples+=triples_
        tweetid,tweeturi,triples_=self.messageTriples(userid,useruri,tweet)
        triples+=triples_
        triples+=self.replyTriples(tweet,tweeturi)
        triples+=self.entityTriples(tweet,tweeturi)
        self.countNew(tweetid,userid)
        return tweeturi,triples

    def userTriples(self,tweet):
        if not tweet["user"]["id_str"]:
            raise ValueError("user id is None???")
        userid_=tweet["user"]["id_str"]
        userid=self.snapshotid+"-"+userid_
        useruri=P.rdf.ic(po.Participant,userid,self.tweet_graph,self.snapshoturi)
        triples=[
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
        return userid, useruri, triples

    def messageTriples(self,userid,useruri,tweet):
        tweetid_=tweet["id_str"]
        # if tweetid_ in triples
        tweetid=userid+"-"+tweetid_
        tweeturi=P.rdf.ic(po.Tweet,tweetid,self.tweet_graph,self.snapshoturi)
        tweet_text=tweet["text"]
        nchars=len(tweet_text)
        ntokens=len(k.tokenize.wordpunct_tokenize(tweet_text))
        self.nchars_all+=[nchars]
        self.ntokens_all+=[ntokens]
        date=dateutil.parser.parse(tweet["created_at"])
        self.dates+=[date]
        triples=[
                 (tweeturi,po.author,useruri),
                (tweeturi,po.nChars,nchars),
                (tweeturi,po.nTokens,ntokens),
                (tweeturi,po.stringID,tweetid),
                (tweeturi,po.createdAt,date),
                (tweeturi,po.message,tweet["text"]),
                (tweeturi,po.retweetCount,tweet["retweet_count"]),
                (tweeturi,po.language,tweet["lang"]),
                ]
        return tweetid,tweeturi,triples

    def replyTriples(self,tweet,tweeturi):
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
                tweetid_reply=self.snapshotid+"-noidmsg-"+str(self.anonymous_tweet_count)
                tweeturi_reply=P.rdf.ic(po.Tweet,tweetid_reply,self.tweet_graph,self.snapshoturi)
                self.anonymous_tweet_count+=1
                triples+=[(tweeturi_reply,po.noid,True)]
            triples+=[
                     (tweeturi,po.inReplyToTweet,tweeturi_reply),
                     (tweeturi_reply,po.author,useruri_reply),
                     ]
        return triples
    def entityTriples(self,tweet,tweeturi):
        hashtags=[]
        links=[]
        user_mentions=[]
        media=[]
        #symbols?
        return []
    def countNew(self,tweetid,userid):
        query=[
              ("?uri",a,po.Tweet),
              ("?uri",po.stringID,tweetid)
              ]
        tweet_known=P.get(query,context=self.tweet_graph)
        query=[
              ("?uri",a,po.Participant),
              ("?uri",po.numericID,userid)
              ]
        participant_known=P.get(query,context=self.tweet_graph)
        if not tweet_known:
            self.ntweets+=1
        if not participant_known:
            self.nparticipants+=1


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
