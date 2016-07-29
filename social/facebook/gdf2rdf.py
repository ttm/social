import os
import shutil
import rdflib as r
import nltk as k
import numpy as n
import datetime
import dateutil.parser
import percolation as P
import social as S
from percolation.rdf import NS, a
from .read import readGDF, trans
c = P.check
po = NS.po


class GdfRdfPublishing:
    """Produce a linked data publication tree from GDF and TAB files

    expressing either ego or group structure.

    OUTPUTS:
    =======
    the tree in the directory final_path."""
    provenance_prefix = 'facebook-legacy-'

    def __init__(self, snapshoturi, snapshotid, filename_friendships=None,
                 filename_interactions=None, filename_posts=None,
                 data_path="../data/facebook/",
                 final_path="./facebook_snapshots/",
                 umbrella_dir="facebook_snapshots/"):
        self.friendship_graph = "social_facebook_friendships"
        self.interaction_graph = "social_facebook_interactions"
        self.meta_graph = "social_facebook_meta"
        self.posts_graph = "social_facebook_posts"
        self.social_graph = "social_facebook"
        P.context(self.friendship_graph, "remove")
        P.context(self.interaction_graph, "remove")
        P.context(self.meta_graph, "remove")
        P.context(self.posts_graph, "remove")
        self.snapshotid = snapshotid
        self.snapshoturi = snapshoturi
        self.online_prefix = "https://raw.githubusercontent.com/Open\
            LinkedSocialData/{}master/{}/".format(umbrella_dir, self.snapshotid)
        self.isfriendship = bool(filename_friendships)
        self.isinteraction = bool(filename_interactions)
        self.hastext = bool(filename_posts)
        self.nfriends = self.nfriendships = self.ninteracted = \
            self.ninteractions = self.nposts = 0
        if self.isfriendship:
            # return networkx graph
            fnet = readGDF(data_path+filename_friendships)
            # writes to self.friendship_graph
            fnet_ = self.rdfFriendshipNetwork(fnet)
        if self.isinteraction:
            inet = readGDF(data_path+filename_interactions)  # to networkx
            self.rdfInteractionNetwork(inet)  # to self.interaction_graph
        else:
            self.groupid2 = 0
        if self.hastext:
            self.rdfGroupPosts(data_path+filename_posts)  # to self.posts_graph
        self.observation_count = 0
        locals_ = locals().copy()
        for i in locals_:
            if i != "self":
                if isinstance(locals_[i], str):
                    exec("self.{}='{}'".format(i, locals_[i]))
                else:
                    exec("self.{}={}".format(i, locals_[i]))
        self.makeMetadata()  # rdflib graph with metadata
        self.writeAllFB()  # write linked data tree

    def rdfGroupPosts(self, filename_posts_):
        data = [i.split("\t") for i in open(filename_posts_,
                                            "r").read().split("\n")[:-1]]
        tvars = data[0]
        standard_vars = ['id', 'type', 'message', 'created_time',
                         'comments', 'likes', 'commentsandlikes']
        assert len(tvars) == \
            sum([i == j for i, j in zip(tvars, standard_vars)]),\
            "the tab file format was not understood"
        data = data[1:]
        triples = []
        self.nposts = 0
        nchars_all = []
        ntokens_all = []
        for post in data:
            ind = P.rdf.ic(po.Post, post[0], self.posts_graph, self.snapshoturi)
            ptext = post[2].replace("_", "\n")
            nchars = len(ptext)
            nchars_all.append(nchars)
            ntokens = len(k.tokenize.wordpunct_tokenize(ptext))
            ntokens_all.append(ntokens)
            triples.extend((
                        (ind, po.snapshot, self.snapshoturi),
                        (ind, po.postID, post[0]),
                        (ind, po.postType, post[1]),
                        (ind, po.text, ptext),
                        (ind, po.createdAt, dateutil.parser.parse(post[3])),
                        (ind, po.numberOfComments, int(post[4])),
                        (ind, po.numberOfLikes, int(post[5])),
                        # (ind, po.nChars, nchars),
                        # (ind, po.nTokens, ntokens),
            ))
            if self.nposts % 200 == 0:
                c("posts: ", self.nposts)
            self.nposts += 1
        self.postsvars = ["postID", "postType", "postText", "createdAt",
                          "nComments", "nLikes", "nChars", "nTokens"]
        self.mcharsposts = n.mean(nchars_all)
        self.dcharsposts = n.std(nchars_all)
        self.totalchars = n.sum(nchars_all)
        self.mtokensposts = n.mean(ntokens_all)
        self.dtokensposts = n.std(ntokens_all)
        self.totaltokens = n.sum(ntokens_all)
        P.add(triples, context=self.posts_graph)

    def writeAllFB(self):
        c("started rendering of snapshotID:", self.snapshotid)
        self.final_path_ = "{}{}/".format(self.final_path, self.snapshotid)
        if not os.path.isdir(self.final_path_):
            os.mkdir(self.final_path_)
        # triples = []
        if self.isfriendship:
            g = P.context(self.friendship_graph)
            g.namespace_manager.bind("po", po)
            g.serialize(self.final_path_+self.snapshotid+"Friendship.ttl",
                        "turtle")
            c("ttl")
            g.serialize(self.final_path_+self.snapshotid+"Friendship.rdf",
                        "xml")
            c("xml; serialized friendships")
            # get filesize and ntriples
            # filesizerdf = os.path.getsize(
            #     self.final_path_+self.snapshotid+"Friendship.rdf")/(10**6)
            # filesizettl = os.path.getsize(
            #     self.final_path_+self.snapshotid+"Friendship.ttl")/(10**6)
            # ntriples = len(g)
            # triples.extend((
            #         (self.snapshoturi, po.friendshipXMLFileSizeMB, filesizerdf),
            #         (self.snapshoturi, po.friendshipTTLFileSizeMB, filesizettl),
            #         (self.snapshoturi, po.nFriendshipTriples, ntriples),
            # ))
        if self.isinteraction:
            g = P.context(self.interaction_graph)
            g.namespace_manager.bind("po", po)
            g.serialize(
                self.final_path_+self.snapshotid+"Interaction.ttl", "turtle")
            c("ttl")
            g.serialize(
                self.final_path_+self.snapshotid+"Interaction.rdf", "xml")
            c("serialized interaction")
            # filesizerdf = os.path.getsize(
            #     self.final_path_+self.snapshotid+"Interaction.rdf")/(10**6)
            # filesizettl = os.path.getsize(
            #     self.final_path_+self.snapshotid+"Interaction.ttl")/(10**6)
            # ntriples = len(g)
            # triples.extend((
            #     (self.snapshoturi, po.interactionXMLFileSizeMB, filesizerdf),
            #     (self.snapshoturi, po.interactionTTLFileSizeMB, filesizettl),
            #     (self.snapshoturi, po.nInteractionTriples, ntriples),
            # ))
        if self.hastext:
            g = P.context(self.posts_graph)
            g.namespace_manager.bind("po", po)
            g.serialize(
                self.final_path_+self.snapshotid+"Posts.ttl", "turtle")
            c("ttl")
            g.serialize(self.final_path_+self.snapshotid+"Posts.rdf", "xml")
            c("serialized posts")
            # filesizerdf = os.path.getsize(
            #     self.final_path_+self.snapshotid+"Posts.rdf")/(10**6)
            # filesizettl = os.path.getsize(
            #     self.final_path_+self.snapshotid+"Posts.ttl")/(10**6)
            # ntriples = len(g)
            # triples.extend((
            #     (self.snapshoturi, po.postsXMLFileSizeMB, filesizerdf),
            #     (self.snapshoturi, po.postsTTLFileSizeMB, filesizettl),
            #     (self.snapshoturi, po.nPostsTriples, ntriples),
            # ))
        g = P.context(self.meta_graph)
        # ntriples = len(g)
        # triples.extend((
        #     (self.snapshoturi, po.nMetaTriples, ntriples),
        # ))
        # P.add(triples, context=self.meta_graph)
        g.namespace_manager.bind("po", po)
        g.serialize(self.final_path_+self.snapshotid+"Meta.ttl", "turtle")
        c("ttl")
        g.serialize(self.final_path_+self.snapshotid+"Meta.rdf", "xml")
        c("serialized meta")
        # copy script that generates the linked data
        if not os.path.isdir(self.final_path_+"scripts"):
            os.mkdir(self.final_path_+"scripts")
        shutil.copy(S.PACKAGEDIR+"/../tests/triplify.py",
                    self.final_path_+"scripts/triplify.py")
        # copy base data
        if not os.path.isdir(self.final_path_+"base"):
            os.mkdir(self.final_path_+"base")
        originals = ""
        if self.isfriendship:
            shutil.copy(self.data_path+self.filename_friendships,
                        self.final_path_+"base/")
            originals += "base/{}".format(self.filename_friendships)
            tfriendship = """\n\n{nf} individuals with metadata {fvars}
and {nfs} friendships constitute the friendship network in the RDF/XML file:
{frdf} \nor in the Turtle file: \n{fttl}
(anonymized: {fan}).""".format(
                        nf=self.nfriends, fvars=str(self.friendsvars),
                        nfs=self.nfriendships,
                        frdf=self.frdf, fttl=self.fttl,
                        fan=self.friendships_anonymized,
                            )
        else:
            tfriendship = ""
        if self.isinteraction:
            shutil.copy(self.data_path+self.filename_interactions,
                        self.final_path_+"base/")
            tinteraction = """\n\n{} individuals with metadata {}
and {} interactions with metadata {} constitute the interaction
network in the RDF/XML file:
{}
or in the Turtle file:
{}
(anonymized: {}).""".format(self.ninteracted,
                            str(self.varsfriendsinteraction),
                            self.ninteractions, str(self.interactionsvars),
                            self.irdf,
                            self.ittl,
                            self.interactions_anonymized)
            originals += "\nbase/{}".format(self.filename_interactions)
        else:
            tinteraction = ""
        if self.hastext:
            shutil.copy(self.data_path+self.filename_posts,
                        self.final_path_+"base/")
            tposts = """\n\n{} posts with {:.3f} characters in average \
                (std: {:.3f}) and total chars in snapshot: {}
{:.3f} tokens in average (std: {:.3f}) and total tokens in snapshot: {}
posts data in the RDF/XML file:
{}
or in the Turtle file:
{}""".format(self.nposts, self.mcharsposts, self.dcharsposts, self.totalchars,
             self.mtokensposts, self.dtokensposts, self.totaltokens,
             self.prdf,
             self.pttl)
            originals += "\nbase/{}".format(self.filename_posts)
        else:
            tposts = ""
        datetime_string = P.get(r.URIRef(self.snapshoturi), po.dateObtained, None, context=self.social_graph)[2]
        with open(self.final_path_+"README", "w") as f:
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

The script that rendered this data publication is on the script/ \
                    directory.\n:::""".format(
                        snapid=self.snapshotid,
                        date=datetime_string,
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

    def makeMetadata(self):
        if self.isfriendship and self.groupid and self.groupid2 and \
                (self.groupid != self.groupid2):
            raise ValueError("Group IDS are different")
        # triples = P.get(self.snapshoturi, None, None, self.social_graph)
        # for rawfile in P.get(self.snapshoturi, po.rawFile, None,
        #                      self.social_graph, strict=True, minimized=True):
        #     triples.extend(P.get(rawfile, None, None, self.social_graph))
        # P.add(triples, context=self.meta_graph)
        foo = {"uris": [], "vals": []}
        if self.isfriendship:
            foo["uris"].extend([
                    # po.onlineOriginalFriendshipFile,
                    # po.originalFriendshipFileName,
                    # po.onlineFriendshipXMLFile,
                    # po.onlineFriendshipTTLFile,
                    # po.friendshipXMLFileName,
                    # po.friendshipTTLFileName,
                    # po.numberOfFriends,
                    # po.numberOfFriendships,
                    po.friendshipsAnonymized
                    ] #  + [po.frienshipParticipantAttribute]*len(self.friendsvars)
            )
            self.ffile = "base/"+self.filename_friendships
            self.frdf = self.snapshotid+"Friendship.rdf"
            self.fttl = self.snapshotid+"Friendship.ttl"
            foo["vals"].extend([
                    # self.online_prefix+self.ffile,
                    # self.ffile,
                    # self.online_prefix+self.frdf,
                    # self.online_prefix+self.fttl,
                    # self.frdf,
                    # self.fttl,
                    # self.nfriends,
                    # self.nfriendships,
                    self.friendships_anonymized
                    ] #  +list(self.friendsvars)
            )

        if self.isinteraction:
            foo["uris"].extend([
                        # po.onlineOriginalInteractionFile,
                        # po.originalInteractionFileName,
                        # po.onlineInteractionXMLFile,
                        # po.onlineInteractionTTLFile,
                        # po.interactionXMLFileName,
                        # po.interactionTTLFileName,
                        # po.numberOfInteractedParticipants,
                        # po.numberOfInteractions,
                        po.interactionsAnonymized
                        ])
            #  + [po.interactionParticipantAttribute]*len(
            #      self.interactionsvars)
            self.ifile = "base/"+self.filename_interactions
            self.irdf = irdf = self.snapshotid+"Interaction.rdf"
            self.ittl = ittl = self.snapshotid+"Interaction.ttl"
            foo["vals"].extend([
                    # self.ifile,
                    # self.online_prefix+self.ifile,
                    # self.online_prefix+irdf,
                    # self.online_prefix+ittl,
                    # irdf,
                    # ittl,
                    # self.ninteractions,
                    # self.ninteracted,
                    self.interactions_anonymized,
                    ] #  +list(self.interactionsvars)
            )
        if self.hastext:
            foo["uris"].extend([
                        # po.onlineOriginalPostsFile,
                        # po.originalPostsFileName,
                        # po.onlinePostsXMLFile,
                        # po.onlinePostsTTLFile,
                        # po.postsXMLFileName,
                        # po.postsTTLFileName,
                        # po.numberOfPosts,
                        # po.numberOfChars,
                        # po.meanChars,
                        # po.deviationChars,
                        # po.numberOfTokens,
                        # po.meanTokens,
                        # po.deviationTokens,
                        ] #  + [po.postAttribute]*len(self.postsvars)
            )
            self.pfile = "base/"+self.filename_posts
            self.prdf = self.snapshotid+"Post.rdf"
            self.pttl = self.snapshotid+"Post.ttl"
            foo["vals"].extend([
                    # self.online_prefix+self.pfile,
                    # self.pfile,
                    # self.online_prefix+self.prdf,
                    # self.online_prefix+self.pttl,
                    # self.prdf,
                    # self.pttl,
                    # self.nposts,
                    # int(self.totalchars),
                    # self.mcharsposts,
                    # self.dcharsposts,
                    # int(self.totaltokens),
                    # self.mtokensposts,
                    # self.dtokensposts,
                    ] #  +list(self.postsvars)
            )

        foo["uris"].extend([
                    a,
                    po.snapshotID,
                    po.isGroup,
                    po.isEgo,
                    po.isFriendship,
                    po.isInteraction,
                    # po.hasText,
                    po.isPost,
                    po.dateObtained,
                    po.name,
                    ]
        )
        # self.isego = bool(P.get(r.URIRef(self.snapshoturi), a, po.EgoSnapshot))
        # self.isgroup = bool(P.get(r.URIRef(self.snapshoturi), a, po.GroupSnapshot))
        self.isego = P.get(r.URIRef(self.snapshoturi), po.isEgo)[2].toPython()
        self.isgroup = P.get(r.URIRef(self.snapshoturi), po.isGroup)[2].toPython()
        date_obtained = P.get(r.URIRef(self.snapshoturi), po.dateObtained)[2].toPython()
        assert isinstance(date_obtained, datetime.date)
        name = P.get(r.URIRef(self.snapshoturi), po.name, None, context=self.social_graph)[2]
        foo["vals"].extend([po.Snapshot, self.snapshotid,
                        self.isgroup, self.isego, self.isfriendship,
                        self.isinteraction, self.hastext, date_obtained, name]) #  , self.hastext])

        numericID = P.get(r.URIRef(self.snapshoturi), po.numericID, None, context=self.social_graph)
        if numericID:
            foo['uris'].append(po.numericID)
            foo['vals'].append(numericID[2])
        stringID = P.get(r.URIRef(self.snapshoturi), po.stringID, None, context=self.social_graph)
        if stringID:
            foo['uris'].append(po.stringID)
            foo['vals'].append(stringID[2])
        url = P.get(r.URIRef(self.snapshoturi), po.url, None, context=self.social_graph)
        if url:
            foo['uris'].append(po.url)
            foo['vals'].append(url[2])
        self.mrdf = self.snapshotid+"Meta.rdf"
        self.mttl = self.snapshotid+"Meta.ttl"

        self.desc = "facebook network with snapshotID: {}\nsnapshotURI: \
            {} \nisEgo: {}. isGroup: {}.".format(
                self.snapshotid, self.snapshoturi, self.isego, self.isgroup)
        self.desc += "\nisFriendship: {}".format(self.isfriendship)
        # if self.isfriendship:
        #     self.desc += "; numberOfFriends: {}; numberOfFrienships: {}.".format(
        #         self.nfriends, self.nfriendships)
        self.desc += "\nisInteraction: {}".format(self.isinteraction)
        # if self.isinteraction:
        #     self.desc += "; numberOfInteracted: {}; numberOfInteractions: {}.".format(
        #         self.ninteracted, self.ninteractions)
        self.desc += "\nisPost: {} (has text)".format(
            self.hastext)
        # if self.hastext:
        #     self.desc += ";\nmeanChars: {}; deviationChars: {}; \
        #         totalChars: {}; \nmeanTokens: {}; \
        #         deviationTokens: {}; totalTokens: {}".format(
        #             self.nposts,
        #             self.mcharsposts, self.dcharsposts, self.totalchars,
        #             self.mtokensposts, self.dtokensposts, self.totaltokens,
        #             )
        P.rdf.triplesScaffolding(self.snapshoturi, [
                        po.triplifiedIn,
                        # po.triplifiedBy,
                        # po.donatedBy,
                        # po.availableAt,
                        # po.onlineMetaXMLFile,
                        # po.onlineMetaTTLFile,
                        # po.metaXMLFileName,
                        # po.metaTTLFileName,
                        po.acquiredThrough,
                        po.socialProtocol,
                        # po.socialProtocolTag,
                        # po.socialProtocol,
                        po.comment,
                        ]+foo["uris"],
                        [
                        datetime.datetime.now(),
                        # "scripts/",
                        # self.snapshotid[:-4],
                        # self.online_prefix,
                        # self.online_prefix+self.mrdf,
                        # self.online_prefix+self.mttl,
                        # self.mrdf,
                        # self.mttl,
                        "Netvizz",
                        "Facebook",
                        # "Facebook",
                        # P.rdf.ic(po.Platform, "Facebook", self.meta_graph, self.snapshoturi),
                        self.desc,
                        ]+foo["vals"],
                        self.meta_graph)

    def rdfFriendshipNetwork(self, fnet):
        if sum([("user" in i) for i in fnet["individuals"]["label"]]) == \
                len(fnet["individuals"]["label"]):
            # fake names and local ids
            self.friendships_anonymized = True
        else:
            self.friendships_anonymized = False
        tkeys = list(fnet["individuals"].keys())
        if "groupid" in tkeys:
            self.groupid = fnet["individuals"]["groupid"][0]
            tkeys.remove("groupid")
        else:
            self.groupid = None
        if self.friendships_anonymized:
            self.friendsvars = [trans[i] for i in tkeys if
                                i not in ('label', 'name')]
        else:
            self.friendsvars = [trans[i] for i in tkeys]
        insert = {"uris": [], "vals": []}
        # values for each participant are in the same order as insert['uris']
        for tkey in tkeys:
            insert["uris"].append(eval("po."+trans[tkey]))
            insert["vals"].append(fnet["individuals"][tkey])
        self.nfriends = len(insert["vals"][0])
        iname = tkeys.index("name")
        ilabel = tkeys.index("label")
        for vals_ in zip(*insert["vals"]):
            if self.friendships_anonymized:
                if vals_[ilabel] and ("user" not in vals_[ilabel]):
                    raise ValueError("Anonymized networks should have no \
                                     informative name. Found: "+vals_[ilabel])
                name_ = "{}-{}".format(self.self.snapshotid, vals_[iname])
                insert_uris_ = [el for i, el in enumerate(insert['uris']) if
                                i not in (ilabel, iname)]
                vals_ = [el for i, el in enumerate(vals_) if
                         (i not in (ilabel, iname))]
            else:
                name_ = "{}-{}".format(self.provenance_prefix, vals_[iname])
                insert_uris_ = [el for i, el in enumerate(insert['uris']) if i != iname]
                vals_ = [el for i, el in enumerate(vals_) if i != iname]
                uri = insert['uris'][iname]
                numericID = vals_[iname]
                P.add([(ind, uri, numericID)], self.friendship_graph)
            ind = P.rdf.ic(po.Participant, name_, self.friendship_graph,
                           self.snapshoturi)
            name__ = '{}-{}'.format(self.snapshotid, self.observation_count)
            self.observation_count += 1
            obs = P.rdf.ic(po.Observation, name__, self.friendship_graph,
                           self.snapshoturi)
            P.add([(ind, po.observation, obs)], self.friendship_graph)
            P.rdf.triplesScaffolding(obs, insert_uris_, vals_,
                                     self.friendship_graph)
        c("participants written")
        friendships_ = [fnet["relations"][i] for i in ("node1", "node2")]
        i = 0
        for uid1, uid2 in zip(*friendships_):
            uids_ = [uid1, uid2]
            uids_.sort()
            if self.friendships_anonymized:
                flabel = "{}-{}-{}".format(self.snapshotid, *uids_)
                uids = [r.URIRef(po.Participant+"#{}-{}".format(
                    self.snapshotid, i)) for i in (uid1, uid2)]
            else:
                flabel = "{}-{}-{}".format(self.provenance_prefix, *uids_)
                uids = [r.URIRef(po.Participant+"#{}-{}".format(
                    self.provenance_prefix, i)) for i in (uid1, uid2)]
            friendship_uri = P.rdf.ic(po.Friendship, flabel,
                                      self.friendship_graph, self.snapshoturi)
            P.rdf.triplesScaffolding(friendship_uri, [po.member]*2,
                                     uids, self.friendship_graph)
            i += 1
            if (i % 1000) == 0:
                c("friendships", i)
        self.nfriendships = len(friendships_[0])
        c("friendships written")

    def rdfInteractionNetwork(self, fnet):
        if sum([("user" in i) for i in fnet["individuals"]["label"]]) == \
                len(fnet["individuals"]["label"]):
            # fake names and local ids
            self.interactions_anonymized = True
        else:
            self.interactions_anonymized = False
        tkeys = list(fnet["individuals"].keys())
        if "groupid" in tkeys:
            self.groupid2 = fnet["individuals"]["groupid"][0]
            tkeys.remove("groupid")
        else:
            self.groupid2 = None
        if self.interactions_anonymized:
            self.varsfriendsinteraction = [trans[i] for i in tkeys
                                           if i not in ('label', 'name')]
        else:
            self.varsfriendsinteraction = [trans[i] for i in tkeys]
        insert = {"uris": [], "vals": []}
        for tkey in tkeys:
            insert["uris"].append(eval("po."+trans[tkey]))
            insert["vals"].append(fnet["individuals"][tkey])
        self.ninteracted = len(insert["vals"][0])
        iname = tkeys.index("name")
        ilabel = tkeys.index("label")
        for vals_ in zip(*insert["vals"]):
            if self.interactions_anonymized:
                name_ = "{}-{}".format(self.snapshotid, vals_[iname])
                insert_uris_ = [el for i, el in enumerate(insert['uris']) if
                                i not in (ilabel, iname)]
                vals__ = [el for i, el in enumerate(vals_) if
                          i not in (ilabel, iname)]
            else:
                name_ = "{}-{}".format(self.provenance_prefix, vals_[iname])
                insert_uris_ = [el for i, el in enumerate(insert['uris']) if i != iname]
                vals__ = [el for i, el in enumerate(vals_) if i != iname]
                uri = insert['uris'][iname]
                numericID = vals_[iname]
                P.add([(ind, uri, numericID)], self.interaction_graph)
            ind = P.rdf.ic(po.Participant, name_, self.interaction_graph,
                           self.snapshoturi)
            name__ = '{}-{}'.format(self.snapshotid, self.observation_count)
            self.observation_count += 1
            obs = P.rdf.ic(po.Observation, name__, self.interaction_graph,
                           self.snapshoturi)
            P.add([(ind, po.observation, obs)], self.interaction_graph)
            if vals__:
                P.rdf.triplesScaffolding(obs, insert_uris_, vals__,
                                         self.interaction_graph)
            else:
                c("anonymous participant without attributes (besides local id). \
                  snapshotid:", self.snapshotid, "values:", vals_)

        c("participant written")
        self.interactionsvarsfoo = ["node1", "node2", "weight"]
        interactions_ = [fnet["relations"][i] for i in self.interactionsvarsfoo]
        self.ninteractions = len(interactions_[0])
        self.interactionsvars = ["iFrom", "iTo", "weight"]
        i = 0
        for uid1, uid2, weight in zip(*interactions_):
            weight_ = int(weight)
            assert weight_-weight == 0, \
                "float weights in fb interaction networks?"
            if self.interactions_anonymized:
                iid = "{}-{}-{}".format(self.snapshotid, uid1, uid2)
                uids = [r.URIRef(po.Participant+"#{}-{}".format(self.snapshotid, i))
                        for i in (uid1, uid2)]
            else:
                iid = "{}-{}-{}".format(self.provenance_prefix, uid1, uid2)
                uids = [r.URIRef(po.Participant+"#{}-{}".format(self.provenance_prefix, i))
                        for i in (uid1, uid2)]
            ind = P.rdf.ic(po.Interaction, iid, self.interaction_graph,
                           self.snapshoturi)
            P.rdf.triplesScaffolding(ind, [po.interactionFrom,
                                           po.interactionTo],
                                     uids, self.interaction_graph)
            name__ = '{}-{}'.format(self.snapshotid, self.observation_count)
            self.observation_count += 1
            obs = P.rdf.ic(po.Observation, name__, self.interaction_graph,
                           self.snapshoturi)
            P.add([(ind, po.observation, obs), (obs, po.weight, weight_],
                self.interaction_graph)
            if (i % 1000) == 0:
                c("interactions: ", i)
            i += 1
        c("escritas interações")
