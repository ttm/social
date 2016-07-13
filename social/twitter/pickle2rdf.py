import os
import shutil
import pickle
import dateutil
import datetime
import numpy as n
# import nltk as k
import percolation as P
import social as S
from percolation.rdf import NS, a, po, c


class PicklePublishing:
    def __init__(self, snapshoturi, snapshotid, filenames=("foo.pickle", ),
                 data_path="../data/twitter/",
                 final_path="./twitter_snapshots/",
                 umbrella_dir="twitter_snapshots/"):
        if len(filenames) == 2:
            pickle_filename1 = filenames[0]
            pickle_filename2 = filenames[1]
        elif filenames[0].count("_") == 1:
            pickle_filename1 = filenames[0]
            pickle_filename2 = ""
        elif filenames[0].count("_") == 2:
            pickle_filename1 = ""
            pickle_filename2 = filenames[0]
        else:
            raise ValueError("Filenames not understood")
        participantvars = ["stringID", "numericID", "screenName",
                           "favouritesCount", "followersCount", "friendsCount",
                           "language", "listedCount", "name", "statusesCount",
                           "createdAt", "utfOffset", "snapshot"]
        participantvars.sort()
        tweetvars = ["author", "nChars", "nTokens", "stringID", "createdAt",
                     "message", "retweetCount", "language", "inReplyToTweet",
                     "retweetOf", "expandedURL", "hashtag", "snapshot",
                     "stringID", "userMention", "media"]
        isego = False
        isgroup = True
        isfriendship = False
        isinteraction = True
        hastext = True
        interactions_anonymized = False

        tweet_graph = "social_tweets0"
        meta_graph = "social_twitter_meta"
        social_graph = "social_twitter"
        P.context(tweet_graph, "remove")
        P.context(meta_graph, "remove")

        final_path_ = "{}{}/".format(final_path, snapshotid)
        online_prefix = ("https://raw.githubusercontent.com/OpenLinkedSocialData/{}master/{}/".format(umbrella_dir, snapshotid))
        dates = []
        size_rdf = []
        size_ttl = []
        tweet_rdf = []
        tweet_ttl = []
        nchars_all = []
        ntokens_all = []
        ntriples = nhashtags = nmedia = nlinks = nuser_mentions = \
            nparticipants = nretweets = ntweets = nreplies = \
            anonymous_user_count = anonymous_tweet_count = 0
        locals_ = locals().copy()
        del locals_["self"]
        for i in locals_:
            exec("self.{}={}".format(i, i))
        self.rdfTweets()
        self.makeMetadata()
        self.writeAllTW()

    def makeMetadata(self):
        triples = P.get(self.snapshoturi, None, None, self.social_graph)
        for rawfile in P.get(self.snapshoturi, po.rawFile, None,
                             self.social_graph, strict=True, minimized=True):
            triples.extend(P.get(rawfile, None, None, self.social_graph))
        self.totalchars = sum(self.nchars_all)
        self.mcharstweets = n.mean(self.nchars_all)
        self.dcharstweets = n.std(self.nchars_all)
        self.totaltokens = sum(self.ntokens_all)
        self.mtokenstweets = n.mean(self.ntokens_all)
        self.dtokenstweets = n.std(self.ntokens_all)
        # P.add(triples, context=self.meta_graph)
        triples.extend((
                (self.snapshoturi, po.nParticipants, self.nparticipants),
                (self.snapshoturi, po.nTweets, self.ntweets),
                (self.snapshoturi, po.nReplies, self.nreplies),
                (self.snapshoturi, po.nRetweets, self.nretweets),
                (self.snapshoturi, po.nCharsOverall, self.totalchars),
                (self.snapshoturi, po.mCharsOverall, self.mcharstweets),
                (self.snapshoturi, po.dCharsOverall, self.dcharstweets),
                (self.snapshoturi, po.nTokensOverall, self.totaltokens),
                (self.snapshoturi, po.mTokensOverall, self.mtokenstweets),
                (self.snapshoturi, po.dTokensOverall, self.dtokenstweets),
        ))
        P.add(triples, context=self.meta_graph)
        P.rdf.triplesScaffolding(
            self.snapshoturi,
            [po.tweetParticipantAttribute]*len(self.participantvars),
            self.participantvars, context=self.meta_graph
        )
        P.rdf.triplesScaffolding(
            self.snapshoturi,
            [po.tweetXMLFilename]*len(self.tweet_rdf) +
            [po.tweetTTLFilename]*len(self.tweet_ttl),
            self.tweet_rdf+self.tweet_ttl, context=self.meta_graph)
        P.rdf.triplesScaffolding(
            self.snapshoturi,
            [po.onlineTweetXMLFile]*len(self.tweet_rdf) +
            [po.onlineTweetTTLFile]*len(self.tweet_ttl),
            [self.online_prefix+i for i in self.tweet_rdf+self.tweet_ttl],
            context=self.meta_graph)

        self.mrdf = self.snapshotid+"Meta.rdf"
        self.mttl = self.snapshotid+"Meta.ttl"
        self.desc = ("twitter dataset with snapshotID: {}\nsnapshotURI: "
                     "{} \nisEgo: {}. isGroup: {}.").format(
                         self.snapshotid, self.snapshoturi, self.isego,
                         self.isgroup, )
        self.desc += "\nisFriendship: {}; ".format(self.isfriendship)
        self.desc += "isInteraction: {}.".format(self.isinteraction)
        self.desc += ("\nnParticipants: {}; nInteractions: {} "
                      "(replies+retweets+user mentions).").format(
                          self.nparticipants, self.nreplies+self.nretweets +
                          self.nuser_mentions,)
        self.desc += "\nisPost: {} (alias hasText: {})".format(
            self.hastext, self.hastext)
        self.desc += "\nnTweets: {}; ".format(self.ntweets)
        self.desc += "nReplies: {}; nRetweets: {}; nUserMentions: {}.".format(
            self.nreplies, self.nretweets, self.nuser_mentions)
        self.desc += "\nnTokens: {}; mTokens: {}; dTokens: {};".format(
            self.totaltokens, self.mtokenstweets, self.dtokenstweets)
        self.desc += "\nnChars: {}; mChars: {}; dChars: {}.".format(
            self.totalchars, self.mcharstweets, self.dcharstweets)
        self.desc += "\nnHashtags: {}; nMedia: {}; nLinks: {}.".format(
            self.nhashtags, self.nmedia, self.nlinks)
        triples.extend((
                (self.snapshoturi, po.triplifiedIn, datetime.datetime.now()),
                (self.snapshoturi, po.triplifiedBy, "scripts/"),
                (self.snapshoturi, po.donatedBy, self.snapshotid[:-4]),
                (self.snapshoturi, po.availableAt, self.online_prefix),
                (self.snapshoturi, po.onlineMetaXMLFile, self.online_prefix+self.mrdf),
                (self.snapshoturi, po.onlineMetaTTLFile, self.online_prefix+self.mttl),
                (self.snapshoturi, po.metaXMLFileName, self.mrdf),
                (self.snapshoturi, po.metaTTLFileName, self.mttl),
                (self.snapshoturi, po.totalXMLFileSizeMB, sum(self.size_rdf)),
                (self.snapshoturi, po.totalTTLFileSizeMB, sum(self.size_ttl)),
                (self.snapshoturi, po.acquiredThrough, "Twitter APIs"),
                (self.snapshoturi, po.socialProtocolTag, "Twitter"),
                (self.snapshoturi, po.socialProtocol, P.rdf.ic(po.Platform,
                                                               "Twitter",
                                                               self.meta_graph,
                                                               self.snapshoturi)),
                (self.snapshoturi, po.nTriples, self.ntriples),
                (self.snapshoturi, NS.rdfs.comment, self.desc),
        ))
        P.add(triples, self.meta_graph)

    def writeAllTW(self):
        # write meta and readme with self.desc, then all is finished.
        g = P.context(self.meta_graph)
        ntriples = len(g)
        triples = [
                 (self.snapshoturi, po.nMetaTriples, ntriples),
                 ]
        P.add(triples, context=self.meta_graph)
        g.namespace_manager.bind("po", po)
        g.serialize(self.final_path_+self.snapshotid+"Meta.ttl", "turtle")
        c("ttl")
        g.serialize(self.final_path_+self.snapshotid+"Meta.rdf", "xml")
        c("serialized meta")
        # copia o script que gera este codigo
        if not os.path.isdir(self.final_path_+"scripts"):
            os.mkdir(self.final_path_+"scripts")
        shutil.copy(S.PACKAGEDIR+"/../tests/triplify.py",
                    self.final_path_+"scripts/triplify.py")
        # copia do base data
        tinteraction = """\n\n{} individuals with metadata {}
and {} interactions (retweets: {}, replies: {}, user_mentions: {})
constitute the interaction
network in the RDF/XML file(s):
{}
and the Turtle file(s):
{}
(anonymized: {}).""".format(self.nparticipants, str(self.participantvars),
                            self.nretweets+self.nreplies+self.nuser_mentions,
                            self.nretweets, self.nreplies, self.nuser_mentions,
                            self.tweet_rdf,
                            self.tweet_ttl,
                            self.interactions_anonymized)
        tposts = """\n\nThe dataset consists of {} tweets with metadata {}
{:.3f} characters in average (std: {:.3f}) and total chars in snapshot: {}
{:.3f} tokens in average (std: {:.3f}) and total tokens in snapshot: {}""".format(
                        self.ntweets, str(self.tweetvars),
                        self.mcharstweets, self.dcharstweets, self.totalchars,
                        self.mtokenstweets, self.dtokenstweets, self.totaltokens,
                        )
        self.dates = [i.isoformat() for i in self.dates]
        date1 = 0  # min(self.dates)
        date2 = 0  # max(self.dates)
        with open(self.final_path_+"README", "w") as f:
            f.write("""::: Open Linked Social Data publication
\nThis repository is a RDF data expression of the twitter
snapshot {snapid} with tweets from {date1} to {date2}
(total of {ntrip} triples).{tinteraction}{tposts}
\nMetadata for discovery in the RDF/XML file:
{mrdf} \nor in the Turtle file:\n{mttl}
\nEgo network: {ise}
Group network: {isg}
Friendship network: {isf}
Interaction network: {isi}
Has text/posts: {ist}
\nAll files should be available at the git repository:
{ava}
\n{desc}

The script that rendered this data publication is on the script/ directory.
:::""".format(snapid=self.snapshotid, date1=date1, date2=date2, ntrip=self.ntriples,
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

    def rdfTweets(self):
        tweets = []
        if self.pickle_filename1:
            tweets += readPickleTweetFile(
                self.data_path+self.pickle_filename1)[0]
        if self.pickle_filename2:
            # limit chuck to 10k tweets
            tweets, fopen = readPickleTweetChunk(
                self.data_path+self.pickle_filename2, tweets, None, 10000)
        chunk_count = 0
        # self.tweets = tweets  # for debugging only, remove to release memory
        while tweets:
            c("rendering tweets, chunk:", chunk_count, "ntweets:",
              len(tweets), "snapshotid", self.snapshotid)
            count = 0

            for tweet in tweets:
                tweeturi, triples = self.tweetTriples(tweet)
                if "retweeted_status" in tweet.keys():
                    # self.nretweets += 1
                    tweeturi0, triples0 = self.tweetTriples(tweet['retweeted_status'])
                    triples.extend(triples0)
                    triples.append((tweeturi, po.retweetOf, tweeturi0))
                self.ntriples += len(triples)
                P.add(triples, context=self.tweet_graph)
                count += 1
                if count % 1000 == 0:
                    c("triplified", count, "tweets")
            c("end of chunk:", chunk_count, "ntriples:", self.ntriples)
            self.writeTweets(chunk_count)
            c("chunk has been written")
            chunk_count += 1
            # if chunk_count == 2:
            #     break
            if self.pickle_filename2:
                tweets, fopen = readPickleTweetChunk(None, [], fopen, 10000)
            else:
                tweets = []
        # for i in range(chunk_count):  # free memory
        #     P.context(self.tweet_graph[:-1]+str(i), "remove")

    def writeTweets(self, chunk_count):
        if not os.path.isdir(self.final_path):
            os.mkdir(self.final_path)
        if not os.path.isdir(self.final_path_):
            os.mkdir(self.final_path_)
        filename = self.snapshotid+"Tweet{:05d}".format(chunk_count)
        g = P.context(self.tweet_graph)
        g.namespace_manager.bind("po", po)
        tttl = filename+".ttl"
        trdf = filename+".rdf"
        g.serialize(self.final_path_+tttl, "turtle")
        c("ttl")
        g.serialize(self.final_path_+trdf, "xml")
        filesizettl = os.path.getsize(self.final_path_+tttl)/(10**6)
        filesizerdf = os.path.getsize(self.final_path_+trdf)/(10**6)
        self.tweet_ttl += [tttl]
        self.size_ttl += [filesizettl]
        self.tweet_rdf += [trdf]
        self.size_rdf += [filesizerdf]
        # self.tweet_graph = self.tweet_graph[:-1]+str(chunk_count+1)
        P.context(self.tweet_graph, 'remove')

    def tweetTriples(self, tweet):
        userid, useruri, triples = self.userTriples(tweet)
        tweetid, tweeturi, triples_ = self.messageTriples(userid, useruri, tweet)
        triples.extend(triples_)
        triples.extend(self.replyTriples(tweet, tweeturi))
        triples.extend(self.entityTriples(tweet, tweeturi))
        # self.countNew(tweetid, userid)
        return tweeturi, triples

    def userTriples(self, tweet):
        assert tweet["user"]["id_str"], "user id is None???"
        userid_ = tweet["user"]["id_str"]
        userid = self.snapshotid+"-"+userid_
        useruri = P.rdf.ic(po.Participant, userid, self.tweet_graph,
                           self.snapshoturi)
        triples = [
                 (useruri, po.stringID, userid),
                 (useruri, po.screenName, tweet["user"]["screen_name"]),
                 (useruri, po.numericID, tweet["user"]["id_str"]),
                 (useruri, po.favouritesCount, tweet["user"]["favourites_count"]),
                 (useruri, po.followersCount, tweet["user"]["followers_count"]),
                 (useruri, po.friendsCount, tweet["user"]["friends_count"]),
                 (useruri, po.language, tweet["user"]["lang"]),
                 (useruri, po.listedCount, tweet["user"]["listed_count"]),
                 (useruri, po.name, tweet["user"]["name"]),
                 (useruri, po.statusesCount, tweet["user"]["statuses_count"]),
                 (useruri, po.createdAt, dateutil.parser.parse(tweet["user"]["created_at"])),
                 (useruri, po.utcOffset, tweet["user"]["utc_offset"]),
                 ]
        return userid, useruri, triples

    def messageTriples(self, userid, useruri, tweet):
        tweetid_ = tweet["id_str"]
        # if tweetid_ in triples
        tweetid = userid+"-"+tweetid_
        tweeturi = P.rdf.ic(po.Tweet, tweetid, self.tweet_graph, self.snapshoturi)
        # tweet_text = tweet["text"]
        nchars = len(tweet['text'])
#        ntokens = len(k.tokenize.wordpunct_tokenize(tweet['text']))
        # self.nchars_all += [nchars]
        # self.ntokens_all += [ntokens]
        date = dateutil.parser.parse(tweet["created_at"])
        # self.dates += [date]
        triples = [
                (tweeturi, po.author, useruri),
                (tweeturi, po.nChars, nchars),
#                (tweeturi, po.nTokens, ntokens),
                (tweeturi, po.stringID, tweetid),
                (tweeturi, po.createdAt, date),
                (tweeturi, po.message, tweet["text"]),
                (tweeturi, po.retweetCount, tweet["retweet_count"]),
                (tweeturi, po.language, tweet["lang"]),
                ]
        return tweetid, tweeturi, triples

    def replyTriples(self, tweet, tweeturi):
        triples = []
        if tweet["in_reply_to_user_id_str"] or tweet["in_reply_to_status_id_str"]:
            # self.nreplies += 1
            if tweet["in_reply_to_status_id_str"]:
                userid_reply = self.snapshotid+"-"+tweet["in_reply_to_user_id_str"]
                useruri_reply = P.rdf.ic(po.Participant, userid_reply,
                                         self.tweet_graph, self.snapshoturi)
                # if not P.get(useruri_reply, po.numericID, None):  # new user
                #     self.nparticipants += 1
                #     triples += [(useruri_reply, po.numericID, userid_reply)]
                triples.append((useruri_reply, po.numericID, userid_reply))
            else:
                userid_reply = self.snapshotid+"-anonymous-"+str(
                    self.anonymous_user_count)
                useruri_reply = P.rdf.ic(po.Participant, userid_reply,
                                         self.tweet_graph, self.snapshoturi)
                self.anonymous_user_count += 1
                triples += [(useruri_reply, po.anonymous, True)]
            if tweet["in_reply_to_status_id_str"]:
                tweetid_reply = userid_reply+"-"+tweet["in_reply_to_status_id_str"]
                tweeturi_reply = P.rdf.ic(po.Tweet, tweetid_reply,
                                          self.tweet_graph, self.snapshoturi)
                # if not P.get(tweeturi_reply, po.numericID, None):  # new message
                #     self.ntweets += 1
                #     triples += [(tweeturi_reply, po.numericID, tweetid_reply)]
                triples.append((tweeturi_reply, po.numericID, tweetid_reply))
            else:
                tweetid_reply = self.snapshotid+"-noidmsg-"+str(self.anonymous_tweet_count)
                tweeturi_reply = P.rdf.ic(po.Tweet, tweetid_reply,
                                          self.tweet_graph, self.snapshoturi)
                self.anonymous_tweet_count += 1
                triples.append((tweeturi_reply, po.noid, True))
            triples.extend((
                     (tweeturi, po.inReplyToTweet, tweeturi_reply),
                     (tweeturi_reply, po.author, useruri_reply),
            ))
        return triples

    def entityTriples(self, tweet, tweeturi):
        triples = []
        for hashtag_ in tweet["entities"]["hashtags"]:
            self.nhashtags += 1
            hashtag = hashtag_["text"]
            triples.append(
                    (tweeturi, po.hashtag, hashtag),
            )
        for user_mention in tweet["entities"]["user_mentions"]:
            # self.nuser_mentions += 1
            # userid_mention_ = user_mention["id_str"]
            # name_mention = user_mention["name"]
            # screen_name_mention = user_mention["screen_name"]
            if user_mention["id_str"]:
                userid_mention = self.snapshotid+"-"+user_mention['id_str']
                useruri_mention = P.rdf.ic(po.Participant, userid_mention,
                                       self.tweet_graph, self.snapshoturi)
            else:
                userid_mention = self.snapshotid+"-anonymous-"+str(
                    self.anonymous_user_count)
                useruri_mention = P.rdf.ic(po.Participant, userid_mention,
                                         self.tweet_graph, self.snapshoturi)
                self.anonymous_user_count += 1
                triples += [(useruri_mention, po.anonymous, True)]
            triples.extend((
                    (tweeturi, po.userMention, useruri_mention),
                    (useruri_mention, po.name, user_mention['name']),
                    (useruri_mention, po.screenName, user_mention['screen_name']),
                    (useruri_mention, po.stringID, userid_mention),
                    (useruri_mention, po.numericID, user_mention['id_str'])
            ))
            # if not P.get(useruri_mention, po.numericID, None):  # new user
            #     self.nparticipants += 1
            #     triples.append((useruri_mention, po.numericID, userid_mention))
        # links = []
        for link in tweet["entities"]["urls"]:
            # self.nlinks += 1
            # url = link["url"]
            triples.append((tweeturi, po.expandedURL, link["expanded_url"]))
        if "media" in tweet["entities"].keys():
            for media in tweet["entities"]["media"]:
                self.nmedia += 1
                mediaid = self.snapshoturi+"-"+str(self.nmedia)
                mediauri = P.rdf.ic(po.Media, mediaid, self.tweet_graph,
                                    self.snapshoturi)
                triples += [
                        (tweeturi, po.media, mediauri),
                        (mediauri, po.type, media["type"]),
                        (mediauri, po.expandedURL, media["expanded_url"]),
                        ]
        # symbols? TTM
        return triples

    def countNew(self, tweetid, userid):
        query = [
              ("?uri", a, po.Tweet),
              ("?uri", po.stringID, tweetid)
              ]
        tweet_known = P.get(query)
        query = [
              ("?uri", a, po.Participant),
              ("?uri", po.numericID, userid)
              ]
        participant_known = P.get(query)
        if not tweet_known:
            self.ntweets += 1
        if not participant_known:
            self.nparticipants += 1


def readPickleTweetFile(filename):
    """pickle read for the Dumper class"""
    objs = []
    with open(filename, "rb") as f:
        while 1:
            try:
                objs.append(pickle.load(f))
            except EOFError:
                break
    return objs


def readPickleTweetChunk(filename=None, tweets=[], fopen=None, ntweets=5000):
    """Read ntweets from filename or fopen and add them to tweets list"""
    if not fopen:
        f = open(filename, "rb")
    else:
        f = fopen
    while len(tweets) < ntweets:
        try:
            tweets += pickle.load(f)
        except EOFError:
            break
    return tweets, f
