import os
import shutil
# import pickle
import re
import string
import random
import codecs
from urllib.parse import quote
# import dateutil
import datetime
# import rdflib as r
import nltk as k
import numpy as n
import percolation as P
import social as S
from percolation.rdf import NS, a, po, c


class LogPublishing:
    def __init__(self, snapshoturi, snapshotid, filename="foo.txt",
                 data_path="../data/irc/", final_path="./irc_snapshots/",
                 umbrella_dir="irc_snapshots/"):
        c(snapshoturi, snapshotid, filename)
        isego = False
        isgroup = True
        isfriendship = False
        isinteraction = True
        hastext = True
        interactions_anonymized = False
        irc_graph = "social_log"
        meta_graph = "social_irc_meta"
        social_graph = "social_irc"
        P.context(irc_graph, "remove")
        P.context(meta_graph, "remove")
        final_path_ = "{}{}/".format(final_path, snapshotid)
        online_prefix = "https://raw.githubusercontent.com/OpenLinkedSocialData/{}master/{}/".format(umbrella_dir, snapshotid)
        naamessages = nurls = ndirect = nmention = 0
        dates = []
        nchars_all = []
        ntokens_all = []
        nsentences_all = []
        participantvars = ["nick"]
        messagevars = ["author", "createdAt", "mentions", "directedTo",
                       "systemMessage", "text", "cleanMessageText",
                       "nChars", "nTokens", "nSentences", "url", "emptyMessage"]
        messagevars.sort()
        locals_ = locals().copy()
        del locals_["self"]
        for i in locals_:
            exec("self.{}={}".format(i, i))
        self.rdfLog()
        self.makeMetadata()
        self.writeAllIRC()

    def rdfLog(self):
        try:
            with codecs.open(self.data_path+self.filename, "rb", "iso-8859-1") as f:
                logtext = textFix(f.read())
            c('opened log {} as iso-8859-1'.format(self.snapshotid))
        except OSError:
            with open(self.data_path+self.filename, "r") as f:
                logtext = textFix(f.read())
            c('opened log {} as utf8'.format(self.snapshotid))
        # msgregex=r"\[(\d{2}):(\d{2}):(\d{2})\] \* ([^ ?]*)[ ]*(.*)" # DELETE ???
        # rmessage= r"\[(\d{2}):(\d{2}):(\d{2})\] \<(.*?)\>[ ]*(.*)" # message
        # lista arquivos no dir
        # rdate = r"(\d{4})(\d{2})(\d{2})"  # date
        # system message:
        rsysmsg = r"(\d{4})\-(\d{2})\-(\d{2})T(\d{2}):(\d{2}):(\d{2})  \*\*\* (\S+) (.*)"
        # user message:
        rmsg = r"(\d{4})\-(\d{2})\-(\d{2})T(\d{2}):(\d{2}):(\d{2})  \<(.*?)\> (.*)"
        rurl = 'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
        messages = re.findall(rmsg, logtext)
        system_messages = re.findall(rsysmsg, logtext)
        self.NICKS = set([Q(i[-2]) for i in messages]+[Q(i[-2]) for i in system_messages])
        triples = []
        for nick in self.NICKS:
            useruri = P.rdf.ic(po.Participant, "{}-{}".format(self.snapshotid, nick), self.irc_graph, self.snapshoturi)
            triples.append(
                    (useruri, po.nick, nick),
            )
        messageids = set()
        msgcount = 0
        c("starting translation of log with", len(messages)+len(system_messages), "messages")
        for message in messages:
            year, month, day, hour, minute, second, nick, text = message
            nick = Q(nick)
            datetime_ = datetime.datetime(*[int(i) for i in (year, month, day, hour, minute, second)])
            self.dates += [datetime_]
            timestamp = datetime_.isoformat()
            messageid = "{}-{}-{}".format(self.snapshotid, nick, timestamp)
            while messageid in messageids:
                messageid += '_r_%05x' % random.randrange(16**5)
            messageids.add(messageid)
            messageuri = P.rdf.ic(po.IRCMessage, messageid, self.irc_graph, self.snapshoturi)
            tokens = k.word_tokenize(text)
            tokens = [i for i in tokens if i not in set(string.punctuation)]
            direct_nicks = []  # for directed messages at
            mention_nicks = []  # for mentioned fellows
            direct = 1
            for token in tokens:
                if token not in self.NICKS:
                    direct = 0
                else:
                    if direct:
                        direct_nicks.append(token)
                    else:
                        mention_nicks.append(token)
            for nick in direct_nicks:
                useruri2 = po.Participant+"#{}-{}".format(self.snapshotid, nick)
                triples.append((messageuri, po.directedTo, useruri2))
            if direct_nicks:
                self.ndirect += 1
                text_ = text[text.index(direct_nicks[-1])+len(direct_nicks[-1])+1:].lstrip()
            else:
                text_ = text
            for nick in mention_nicks:
                useruri2 = po.Participant+"#{}-{}".format(self.snapshotid, nick)
                triples.append((messageuri, po.mentions, useruri2))
            self.nmention += len(mention_nicks)
            useruri = po.Participant+"#{}-{}".format(self.snapshotid, nick)
            triples.extend((
                     (messageuri, po.author, useruri),
                     (messageuri, po.systemMessage, False),
                     (messageuri, po.createdAt, datetime_),
            ))
            if text:
                triples.append((messageuri, po.text, text))
            if text_:
                nchars = len(text_)
                ntokens = len(k.word_tokenize(text_))
                nsentences = len(k.sent_tokenize(text_))
                triples += [
                         (messageuri, po.cleanText, text_),
                         # (messageuri, po.nChars, nchars),
                         # (messageuri, po.nTokens, ntokens),
                         # (messageuri, po.nSentences, nsentences),
                         ]
                urls = re.findall(rurl, text_)
                for url in urls:
                    triples += [
                             (messageuri, po.hasUrl, url),
                             ]
                self.nchars_all += [nchars]
                self.ntokens_all += [ntokens]
                self.nsentences_all += [nsentences]
                self.nurls += len(urls)
            else:
                triples += [
                         (messageuri, po.emptyMessage, True),
                         ]
            if text.startswith(";aa ") or text.startswith("lalenia, aa ") or text.startswith("lalenia: aa "):
                self.naamessages += 1
                # triples.append((messageuri, a, po.AAIRCMessage))
                triples.append((messageuri, po.aaMessage, True))
            msgcount += 1
            if msgcount % 1000 == 0:
                c("finished user message", msgcount)
        msgcount = 0
        for message in system_messages:
            year, month, day, hour, minute, second, nick, text = message
            nick = Q(nick)
            useruri = po.Participant+"#{}-{}".format(self.snapshotid, nick)
            datetime_ = datetime.datetime(*[int(i) for i in (year, month, day, hour, minute, second)])
            self.dates += [datetime_]
            timestamp = datetime_.isoformat()
            messageid = "{}-{}".format(self.snapshotid, timestamp)
            while messageid in messageids:
                messageid += '_r_%05x' % random.randrange(16**5)
            messageids.add(messageid)
            messageuri = P.rdf.ic(po.IRCMessage, messageid, self.irc_graph, self.snapshoturi)
            triples += [
                     (messageuri, po.impliedUser, useruri),
                     (messageuri, po.createdAt, datetime_),
                     (messageuri, po.systemMessage, True)
                     ]
            if text:
                triples += [
                         (messageuri, po.text, text)
                         ]
            msgcount += 1
            if msgcount % 1000 == 0:
                c("Total system messages:", msgcount)
        self.messageids = messageids
        if not os.path.isdir(self.final_path):
            os.mkdir(self.final_path)
        if not os.path.isdir(self.final_path_):
            os.mkdir(self.final_path_)
        g = P.context(self.irc_graph)
        triples_ = [tr for tr in g]
        triples.extend(triples_)
        self.log_xml, self.size_xml, self.log_ttl, self.size_ttl = P.rdf.writeByChunks(
            self.final_path_+self.snapshotid+"Log", ntriples=100000, triples=triples, bind=[('po', po)])
        # write self.irc_graph with message and participant instances
        # linked to snapshot
        # g = P.context(self.irc_graph)
        # g.namespace_manager.bind("po", po)
        # tttl = self.snapshotid+"Log"+".ttl"
        # trdf = self.snapshotid+"Log"+".rdf"
        # g.serialize(self.final_path_+tttl, "turtle")
        # c("ttl")
        # g.serialize(self.final_path_+trdf, "xml")
        # c("rdf")

    def makeMetadata(self):
        # triples = P.get(self.snapshoturi, None, None, self.social_graph)
        # for rawfile in P.get(self.snapshoturi, po.rawFile, None, self.social_graph, strict=True, minimized=True):
        #     triples += P.get(rawfile, None, None, self.social_graph)
        # self.totalchars = sum(self.nchars_all)
        # self.mcharsmessages = n.mean(self.nchars_all)
        # self.dcharsmessages = n.std(self.nchars_all)
        # self.totaltokens = sum(self.ntokens_all)
        # self.mtokensmessages = n.mean(self.ntokens_all)
        # self.dtokensmessages = n.std(self.ntokens_all)
        # self.totalsentences = sum(self.nsentences_all)
        # self.msentencesmessages = n.mean(self.nsentences_all)
        # self.dsentencesmessages = n.std(self.nsentences_all)
        # self.nparticipants = len(self.NICKS)
        # self.nmessages = len(self.messageids)
        # self.ntriples = len(P.context(self.irc_graph))
        # triples = [
                # (self.snapshoturi, po.numberOfParticipants,           self.nparticipants),
                # (self.snapshoturi, po.numberOfMessages,                 self.nmessages),
                # (self.snapshoturi, po.numberOfDirectMessages,              self.ndirect),
                # (self.snapshoturi, po.numberOfUserMentions,              self.nmention),
                # (self.snapshoturi, po.numberOfChars, self.totalchars),
                # (self.snapshoturi, po.meanChars, self.mcharsmessages),
                # (self.snapshoturi, po.deviationChars, self.dcharsmessages),
                # (self.snapshoturi, po.numberOfTokens, self.totaltokens),
                # (self.snapshoturi, po.meanTokens, self.mtokensmessages),
                # (self.snapshoturi, po.deviationTokens, self.dtokensmessages),
                # (self.snapshoturi, po.numberOfSentences, self.totalsentences),
                # (self.snapshoturi, po.meanSentences, self.msentencesmessages),
                # (self.snapshoturi, po.deviationSentences, self.dsentencesmessages),
        #        ]
        # P.add(triples, context=self.meta_graph)
        # P.rdf.triplesScaffolding(
        #     self.snapshoturi,
        #     [po.ircParticipantAttribute]*len(self.participantvars),
        #     self.participantvars, context=self.meta_graph
        # )
        # P.rdf.triplesScaffolding(
        #     self.snapshoturi,
        #     [po.logXMLFilename]*len(self.log_xml)+[po.logTTLFilename]*len(self.log_ttl),
        #     self.log_xml+self.log_ttl, context=self.meta_graph
        # )
        # P.rdf.triplesScaffolding(
        #     self.snapshoturi,
        #     [po.onlineLogXMLFile]*len(self.log_xml)+[po.onlineLogTTLFile]*len(self.log_ttl),
        #     [self.online_prefix+i for i in self.log_xml+self.log_ttl], context=self.meta_graph
        # )

        self.mrdf = self.snapshotid+"Meta.rdf"
        self.mttl = self.snapshotid+"Meta.ttl"
        self.desc = "irc dataset with snapshotID: {}\nsnapshotURI: {} \nisEgo: {}. isGroup: {}.".format(
                                                self.snapshotid, self.snapshoturi, self.isego, self.isgroup, )
        self.desc += "\nisFriendship: {}; ".format(self.isfriendship)
        self.desc += "isInteraction: {}.".format(self.isinteraction)
        # self.desc += "\nnParticipants: {}; nInteractions: {} (directed messages+user mentions).".format(
        #     self.nparticipants, self.ndirect+self.nmention)
        self.desc += "\nisPost: {} (alias hasText: {})".format(self.hastext, self.hastext)
        # self.desc += "\nnumberOfMessages: {}; ".format(self.nmessages)
        # self.desc += "nDirectedMessages: {}; numberOfUserMentions: {};".format(self.ndirect, self.nmention)
        # self.desc += "\nnumberOfChars: {}; meanChars: {}; deviationChars: {}.".format(
        #     self.totalchars, self.mcharsmessages, self.dcharsmessages)
        # self.desc += "\nnumberOfTokens: {}; meanTokens: {}; deviationTokens: {};"
        #     self.totaltokens, self.mtokensmessages, self.dtokensmessages)
        # self.desc += "\nnSentencesOverall: {}; meanSentences: {}; deviationSentences: {};".format(
        #     self.totalsentences, self.msentencesmessages, self.dsentencesmessages)
        # self.desc += "\nnumberOfURLs: {}; numberOfAAMessages {}.".format(self.nurls, self.naamessages)
        triples = [
                (self.snapshoturi, po.triplifiedIn,      datetime.datetime.now()),
                 (self.snapshoturi, a, po.Snapshot),
                 (self.snapshoturi, po.snapshotID, self.snapshotid),
                 (self.snapshoturi, po.isEgo, False),
                 (self.snapshoturi, po.isGroup, True),
                 (self.snapshoturi, po.isFriendship, False),
                 (self.snapshoturi, po.isInteraction, True),
                 (self.snapshoturi, po.isPost, True),
                 (self.snapshoturi, po.channel, '#'+self.snapshotid.replace('irc-legacy-', '')),
                # (self.snapshoturi, po.triplifiedBy,      "scripts/"),
                # (self.snapshoturi, po.donatedBy,         self.snapshotid[:-4]),
                # (self.snapshoturi, po.availableAt,       self.online_prefix),
                # (self.snapshoturi, po.onlineMetaXMLFile, self.online_prefix+self.mrdf),
                # (self.snapshoturi, po.onlineMetaTTLFile, self.online_prefix+self.mttl),
                # (self.snapshoturi, po.metaXMLFileName,   self.mrdf),
                # (self.snapshoturi, po.metaTTLFileName,   self.mttl),
                # (self.snapshoturi, po.totalXMLFileSizeMB, sum(self.size_xml)),
                # (self.snapshoturi, po.totalTTLFileSizeMB, sum(self.size_ttl)),
                (self.snapshoturi, po.acquiredThrough,   "channel text log"),
                (self.snapshoturi, po.socialProtocol, "IRC"),
                # (self.snapshoturi, po.socialProtocolTag, "IRC"),
                # (self.snapshoturi, po.socialProtocol,    P.rdf.ic( po.Platform, "IRC", self.meta_graph, self.snapshoturi)),
                # (self.snapshoturi, po.numberOfTriples,         self.ntriples),
                (self.snapshoturi, po.comment,         self.desc),
                ]
        P.add(triples, self.meta_graph)

    def writeAllIRC(self):
        # g = P.context(self.meta_graph)
        # ntriples = len(g)
        # triples = [
        #          (self.snapshoturi, po.nMetaTriples, ntriples+1),
        #          ]
        # P.add(triples, context=self.meta_graph)
        g = P.context(self.meta_graph)
        g.namespace_manager.bind("po", po)
        g.serialize(self.final_path_+self.snapshotid+"Meta.ttl", "turtle")
        c("ttl")
        g.serialize(self.final_path_+self.snapshotid+"Meta.rdf", "xml")
        c("serialized meta")
#         if not os.path.isdir(self.final_path_+"scripts"):
#             os.mkdir(self.final_path_+"scripts")
#         shutil.copy(S.PACKAGEDIR+"/../tests/triplify.py", self.final_path_+"scripts/triplify.py")
#         tinteraction = """\n\n{} individuals with metadata {}
# and {} interactions (direct messages: {}, user mentions: {})
# constitute the interaction
# structure in the RDF/XML file(s):
# {}
# and the Turtle file(s):
# {}
# (anonymized: "nicks inteface").""".format(
#             self.nparticipants, str(self.participantvars),
#             self.ndirect+self.nmention, self.ndirect, self.nmention,
#             self.log_xml,
#             self.log_ttl)
#         tposts = """\n\nThe dataset consists of {} irc messages with metadata {}
# {:.3f} characters in average (std: {:.3f}) and total chars in snapshot: {}
# {:.3f} tokens in average (std: {:.3f}) and total tokens in snapshot: {}
# {:.3f} sentences in average (std: {:.3f}) and total sentences in snapshot: {}""".format(
#                         self.nmessages, str(self.messagevars),
#                         self.mcharsmessages, self.dcharsmessages, self.totalchars,
#                         self.mtokensmessages, self.dtokensmessages, self.totaltokens,
#                         self.msentencesmessages, self.dsentencesmessages, self.totalsentences,
#                         )
#         self.dates = [i.isoformat() for i in self.dates]
#         date1 = min(self.dates)
#         date2 = max(self.dates)
#         with open(self.final_path_+"README", "w") as f:
#             f.write("""::: Open Linked Social Data publication
# \nThis repository is a RDF data expression of the IRC
# snapshot {snapid} with tweets from {date1} to {date2}
# (total of {ntrip} triples).{tinteraction}{tposts}
# \nMetadata for discovery in the RDF/XML file:
# {mrdf} \nor in the Turtle file:\n{mttl}
# \nEgo network: {ise}
# Group network: {isg}
# Friendship network: {isf}
# Interaction network: {isi}
# Has text/posts: {ist}
# \nAll files should be available at the git repository:
# {ava}
# \n{desc}
#
# The script that rendered this data publication is on the script/ directory.\n:::""".format(
#                 snapid=self.snapshotid, date1=date1, date2=date2, ntrip=self.ntriples,
#                 tinteraction=tinteraction,
#                 tposts=tposts,
#                 mrdf=self.log_xml,
#                 mttl=self.log_ttl,
#                 ise=self.isego,
#                 isg=self.isgroup,
#                 isf=self.isfriendship,
#                 isi=self.isinteraction,
#                 ist=self.hastext,
#                 ava=self.online_prefix,
#                 desc=self.desc
#                 ))


strange = ("Ã\x89", "Ã¡", "Ã ", "Ã¢", "Ã£", "Ã¤", "Ã©", "Ã¨", "Ãª", "Ã«",
           "Ã­", "Ã¬", "Ã®", "Ã¯", "Ã³", "Ã²", "Ã´", "Ãµ", "Ã¶", "Ãº", "Ã¹",
           "Ã»", "Ã¼", "Ã§", "Ã", "Ã€", "Ã‚", "Ãƒ", "Ã„", "Ã‰", "Ãˆ",
           "ÃŠ", "Ã‹", "Ã", "ÃŒ", "ÃŽ", "Ã", "Ã“", "Ã’", "Ã”", "Ã•",
           "Ã–", "Ãš", "Ã™", "Ã›", "Ãœ", "Ã‡", "Ã")
correct = ("É", "á",  "à",  "â",  "ã",  "ä",  "é",  "è",  "ê",  "ë",  "í",
           "ì",  "î",  "ï",  "ó",  "ò",  "ô",  "õ",  "ö",  "ú",  "ù",  "û",
           "ü",  "ç",  "Á",  "À",  "Â",  "Ã",  "Ä",  "É",  "È",  "Ê",  "Ë",
           "Í",  "Ì",  "Î",  "Ï",  "Ó",  "Ò",  "Ô",  "Õ",  "Ö",  "Ú",  "Ù",
           "Û",  "Ü",  "Ç", "Ú")


def textFix(string):
    # https://berseck.wordpress.com/2010/09/28/transformar-utf-8-para-acentos-iso-com-php/
    for st, co in zip(strange, correct):
        string = string.replace(st, co)
    # also try .encode("latin1").decode("utf8")
    return string


def Q(string):
    return quote(string).replace("%", "")
