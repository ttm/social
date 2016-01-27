import percolation as P, social as S, rdflib as r, builtins as B, re, datetime, dateutil.parser, os, shutil, numpy as n, nltk as k
from percolation.rdf import NS, a
from .read import readGDF
c=P.check

class GdfRdfPublishing:
    """Produce a linked data publication tree from a standard GDF file.

    INPUTS:
    ======
    => the data directory path
    => the file name (filename_friendship) of the friendship network
    => the file name (filename_interaction) of the interaction network
    => the final path (final_path) for the tree of files to be created
    => a path to the script that is initializing this class (scriptpath)
    => the numeric id (numericid) of the facebook user or group of the network(s)
    => the string id (stringid) of the facebook user or group of the network(s)
    => the facebook link (fb_link) of the user or group
    => the network is from a user (ego==True) or a group (ego==False)
    => a umbrella directory (umbrella_dir) on which more of data is being published

    OUTPUTS:
    =======
    the tree in the directory fpath."""

    def __init__(self,snapshoturi,snapshotid,filename_friendships="foo.gdf",\
            filename_interactions=None,filename_posts=None,\
            data_path="../data/facebook/",final_path="./fb/",umbrella_dir="facebook_networks/"):

        self.friendship_graph="social_facebook_friendships"
        self.interaction_graph="social_facebook_interactions"
        self.meta_graph="social_facebook_meta"
        self.posts_graph="social_facebook_posts"
        self.social_graph="social_facebook"
        P.context(self.friendship_graph,"remove")
        P.context(self.interaction_graph,"remove")
        P.context(self.meta_graph,"remove")
        P.context(self.posts_graph,"remove")
        self.snapshotid=snapshotid
        self.snapshoturi=snapshoturi
        self.online_prefix="https://raw.githubusercontent.com/OpenLinkedSocialData/{}master/{}/".format(umbrella_dir,self.snapshotid)
        self.isfriendship= bool(filename_friendships)
        self.isinteraction=bool(filename_interactions)
        self.hastext=bool(filename_posts)
        self.nfriends=self.nfriendships=self.ninteracted=self.ninteractions=self.nposts=0
        if self.isfriendship:
            fnet=readGDF(data_path+filename_friendships)     # return networkx graph
            fnet_=self.rdfGDFFriendshipNetwork(fnet)   # writes to self.friendship_graph
        if self.isinteraction:
            inet=readGDF(data_path+filename_interactions)    # return networkx graph
            self.rdfInteractionNetwork(inet)      # writes to self.interaction_graph
        else:
            self.groupid2=0
        if self.hastext:
            self.rdfGroupPosts(data_path+filename_posts)      # writes to self.posts_graph

        locals_=locals().copy()
        for i in locals_:
            if i !="self":
                if isinstance(locals_[i],str):
                    exec("self.{}='{}'".format(i,locals_[i]))
                else:
                    exec("self.{}={}".format(i,locals_[i]))
        meta=self.makeMetadata()     # return rdflib graph with metadata about the structure
        self.writeAllFB()  # write linked data tree

    def rdfGroupPosts(self,filename_posts_):
        data=[i.split("\t") for i in open(filename_posts_,"r").read().split("\n")[:-1]]
        tvars=data[0]
        standard_vars=['id','type','message','created_time','comments','likes','commentsandlikes']
        if len(tvars)!=sum([i==j for i,j in zip(tvars,standard_vars)]):
            raise ValueError("the tab file format was not understood")
        data=data[1:]
        triples=[]
        self.nposts=0
        nchars_all=[]
        ntokens_all=[]
        for post in data:
            ind=P.rdf.ic(NS.facebook.Post,post[0],self.posts_graph,self.snapshoturi)
            ptext=post[2].replace("_","\n")
            nchars=len(ptext)
            nchars_all+=[nchars]
            ntokens=len(k.tokenize.wordpunct_tokenize(ptext))
            ntokens_all+=[ntokens]
            triples+=[
                     (ind,NS.po.snapshot,self.snapshoturi),
                     (ind,NS.facebook.postID,post[0]),
                     (ind,NS.facebook.postType,post[1]),
                     (ind,NS.facebook.postText,ptext),
                     (ind,NS.facebook.createdAt,dateutil.parser.parse(post[3])),
                     (ind,NS.facebook.nComments,int(post[4])),
                     (ind,NS.facebook.nLikes,int(post[5])),
                     (ind,NS.facebook.nChars,nchars),
                     (ind,NS.facebook.nTokens,ntokens),
                     ]
            if self.nposts%200==0:
                c("posts: ",self.nposts)
            self.nposts+=1
        self.postsvars=["postID","postType","postText","createdAt","nComments","nLikes","nChars","nTokens"]
        self.mcharsposts=n.mean(nchars_all)
        self.dcharsposts=n.std(  nchars_all)
        self.totalchars=n.sum(   nchars_all)
        self.mtokensposts=n.mean(ntokens_all)
        self.dtokensposts=n.std( ntokens_all)
        self.totaltokens=n.sum(  ntokens_all)
        #triples+=[ # went to meta file
        #         (self.snapshoturi,NS.po.mCharsPosts,self.mcharsposts),
        #         (self.snapshoturi,NS.po.dCharsPosts,self.dcharsposts),
        #         (self.snapshoturi,NS.po.totalCharsPosts,self.totalchars),

        #         (self.snapshoturi,NS.po.mTokensPosts,self.mtokensposts),
        #         (self.snapshoturi,NS.po.dTokensPosts,self.dtokensposts),
        #         (self.snapshoturi,NS.po.totalTokensPosts,self.totaltokens),
        #         ]
        P.add(triples,context=self.posts_graph)

    def writeAllFB(self):
        c("started rendering of the snapshot publication. snapshotID:",self.snapshotid)
        self.final_path_="{}{}/".format(self.final_path,self.snapshotid)
        if not os.path.isdir(self.final_path_):
            os.mkdir(self.final_path_)
        #fnet,inet,mnet
        triples=[]
        if self.isfriendship:
            g=P.context(self.friendship_graph)
            g.namespace_manager.bind("po",NS.po)
            g.namespace_manager.bind("facebook",NS.facebook)
            g.serialize(self.final_path_+self.snapshotid+"Friendship.ttl","turtle"); c("ttl")
            g.serialize(self.final_path_+self.snapshotid+"Friendship.rdf","xml")
            c("serialized friendships")
            # get filesize and ntriples
            filesizerdf=os.path.getsize(self.final_path_+self.snapshotid+"Friendship.rdf")/(10**6)
            filesizettl=os.path.getsize(self.final_path_+self.snapshotid+"Friendship.ttl")/(10**6)
            ntriples=len(g)
            triples+=[
                     (self.snapshoturi,NS.po.friendshipXMLFileSizeMB,filesizerdf),
                     (self.snapshoturi,NS.po.friendshipTTLFileSizeMB,filesizettl),
                     (self.snapshoturi,NS.po.nFriendshipTriples,ntriples),
                     ]
        if self.isinteraction:
            g=P.context(self.interaction_graph)
            g.namespace_manager.bind("po",NS.po)
            g.namespace_manager.bind("facebook",NS.facebook)
            g.serialize(self.final_path_+self.snapshotid+"Interaction.ttl","turtle"); c("ttl")
            g.serialize(self.final_path_+self.snapshotid+"Interaction.rdf","xml")
            c("serialized interaction")
            filesizerdf=os.path.getsize(self.final_path_+self.snapshotid+"Interaction.rdf")/(10**6)
            filesizettl=os.path.getsize(self.final_path_+self.snapshotid+"Interaction.ttl")/(10**6)
            ntriples=len(g)
            triples+=[
                     (self.snapshoturi,NS.po.interactionXMLFileSizeMB,filesizerdf),
                     (self.snapshoturi,NS.po.interactionTTLFileSizeMB,filesizettl),
                     (self.snapshoturi,NS.po.nInteractionTriples,ntriples),
                     ]
        if self.hastext:
            g=P.context(self.posts_graph)
            g.namespace_manager.bind("po",NS.po)
            g.namespace_manager.bind("facebook",NS.facebook)
            g.serialize(self.final_path_+self.snapshotid+"Posts.ttl","turtle"); c("ttl")
            g.serialize(self.final_path_+self.snapshotid+"Posts.rdf","xml")
            c("serialized posts")
            filesizerdf=os.path.getsize(self.final_path_+self.snapshotid+"Posts.rdf")/(10**6)
            filesizettl=os.path.getsize(self.final_path_+self.snapshotid+"Posts.ttl")/(10**6)
            ntriples=len(g)
            triples+=[
                     (self.snapshoturi,NS.po.postsXMLFileSizeMB,filesizerdf),
                     (self.snapshoturi,NS.po.postsTTLFileSizeMB,filesizettl),
                     (self.snapshoturi,NS.po.nPostsTriples,ntriples)      ,
                     ]
        g=P.context(self.meta_graph)
        ntriples=len(g)
        triples+=[
                 (self.snapshoturi,NS.po.nMetaTriples,ntriples)      ,
                 ]
        P.add(triples,context=self.meta_graph)
        g.namespace_manager.bind("po",NS.po)
        g.namespace_manager.bind("facebook",NS.facebook)
        g.serialize(self.final_path_+self.snapshotid+"Meta.ttl","turtle"); c("ttl")
        g.serialize(self.final_path_+self.snapshotid+"Meta.rdf","xml")
        c("serialized meta")
        # copia o script que gera este codigo
        if not os.path.isdir(self.final_path_+"scripts"):
            os.mkdir(self.final_path_+"scripts")
        shutil.copy(S.PACKAGEDIR+"/../tests/triplify.py",self.final_path_+"scripts/triplify.py")
        # copia do base data
        if not os.path.isdir(self.final_path_+"base"):
            os.mkdir(self.final_path_+"base")
        originals=""
        if self.isfriendship:
            shutil.copy(self.data_path+self.filename_friendships,self.final_path_+"base/")
            originals+="base/{}".format(self.filename_friendships)
            tfriendship="""\n\n{nf} individuals with metadata {fvars}
and {nfs} friendships constitute the friendship network in the RDF/XML file:
{frdf} \nor in the Turtle file: \n{fttl}
(anonymized: {fan}).""".format(
                            nf=self.nfriends,fvars=str(self.friendsvars),
                            nfs=self.nfriendships,
                            frdf=self.frdf,fttl=self.fttl,
                            fan=self.friendships_anonymized,
                        )
        else:
            tfriendship=""
        if self.isinteraction:
            shutil.copy(self.data_path+self.filename_interactions,self.final_path_+"base/")
            tinteraction="""\n\n{} individuals with metadata {}
and {} interactions with metadata {} constitute the interaction 
network in the RDF/XML file:
{}
or in the Turtle file:
{}
(anonymized: {}).""".format( self.ninteracted,str(self.varsfriendsinteraction),
                        self.ninteractions,str(self.interactionsvars),
                        self.irdf,
                        self.ittl,
                        self.interactions_anonymized)
            originals+="\nbase/{}".format(self.filename_interactions)
        else:
            tinteraction=""
        if self.hastext:
            shutil.copy(self.data_path+self.filename_posts,self.final_path_+"base/")
            tposts="""\n\n{} posts with {:.3f} characters in average (std: {:.3f}) and total chars in snapshot: {}
{:.3f} tokens in average (std: {:.3f}) and total tokens in snapshot: {}
posts data in the RDF/XML file:
{}
or in the Turtle file:
{}""".format( self.nposts,self.mcharsposts,self.dcharsposts,self.totalchars,
                        self.mtokensposts,self.dtokensposts,self.totaltokens,
                        self.prdf,
                        self.pttl)
            originals+="\nbase/{}".format(self.filename_posts)
        else:
            tposts=""


#        P.rdf.writeAll(mnet,aname+"Meta",fpath_,1)
        # faz um README
        datetime_string=P.get(r.URIRef(self.snapshoturi),NS.po.dateObtained,None,context="social_facebook")[2]
#        if not os.path.isdir(self.final_path+"base"):
#            os.mkdir(self.final_path+"base")
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

    def makeMetadata(self):
        if self.isfriendship and self.groupid and self.groupid2 and (self.groupid!=self.groupid2):
            raise ValueError("Group IDS are different")
        # put all triples from social_facebook to self.meta_graph
        #g1=P.context("social_facebook")
        #g2=P.context(self.meta_graph)
        #for subject, predicate, object_ in g1.triples((self.snapshoturi))
        triples=P.get(self.snapshoturi,None,None,"social_facebook")
        for rawfile in P.get(self.snapshoturi,NS.po.rawFile,None,"social_facebook",strict=True,minimized=True):
            triples+=P.get(rawfile,None,None,"social_facebook")
        P.add(triples,context=self.meta_graph)
        foo={"uris":[],"vals":[]}
        if self.isfriendship:
            foo["uris"]+=[
                         NS.facebook.onlineOriginalFriendshipFile,
                         NS.facebook.originalFriendshipFileName,
                         NS.po.onlineFriendshipXMLFile,
                         NS.po.onlineFriendshipTTLFile,
                         NS.po.friendshipXMLFileName,
                         NS.po.friendshipTTLFileName,
                         ]+\
                         [
                         NS.facebook.nFriends,
                         NS.facebook.nFriendships,
                         NS.facebook.friendshipsAnonymized 
                         ]+\
                         [NS.facebook.frienshipParticipantAttribute]*len(self.friendsvars)
            self.ffile=ffile="base/"+self.filename_friendships
            self.frdf=self.snapshotid+"Friendship.rdf"
            self.fttl=self.snapshotid+"Friendship.ttl"
            foo["vals"]+=[
                         self.online_prefix+ffile,
                         ffile,
                         self.online_prefix+self.frdf,
                         self.online_prefix+self.fttl,
                         self.frdf,
                         self.fttl,
                         self.nfriends,
                         self.nfriendships,
                         self.friendships_anonymized
                         ]+list(self.friendsvars)

        if self.isinteraction:
            foo["uris"]+=[
                         NS.facebook.onlineOriginalInteractionFile,
                         NS.facebook.originalInteractionFileName,
                         NS.po.onlineInteractionXMLFile,
                         NS.po.onlineInteractionTTLFile,
                         NS.po.interactionXMLFileName,
                         NS.po.interactionTTLFileName,
                         ]+\
                         [
                         NS.facebook.nInteracted,
                         NS.facebook.nInteractions,
                         NS.facebook.interactionsAnonymized 
                         ]+\
                         [NS.facebook.interactionParticipantAttribute]*len(self.interactionsvars)
            self.ifile="base/"+self.snapshotid
            self.irdf=irdf=self.snapshotid+"Interaction.rdf"
            self.ittl=ittl=self.snapshotid+"Interaction.ttl"
            foo["vals"]+=[
                          self.ifile,
                          self.online_prefix+self.ifile,
                          self.online_prefix+irdf,
                          self.online_prefix+ittl,
                          irdf,
                          ittl,
                          self.ninteractions,
                          self.ninteracted,
                          self.interactions_anonymized,
                          ]+list(self.interactionsvars)
        if self.hastext:
            foo["uris"]+=[
                         NS.facebook.onlineOriginalPostsFile,
                         NS.facebook.originalPostsFileName,
                         NS.po.onlinePostsXMLFile,
                         NS.po.onlinePostsTTLFile,
                         NS.po.postsXMLFileName,
                         NS.po.postsTTLFileName,
                         ]+\
                         [
                         NS.facebook.nPosts,
                         NS.facebook.nCharsOverall,
                         NS.facebook.mCharsPostsOverall,
                         NS.facebook.dCharsPostsOverall,
                         NS.facebook.nTokensOverall,
                         NS.facebook.mTokensPostsOverall,
                         NS.facebook.dTokensPostsOverall,
                         ]+\
                         [NS.facebook.postAttribute]*len(self.postsvars)
            self.pfile="base/"+self.filename_posts
            self.prdf=self.snapshotid+"Post.rdf"
            self.pttl=self.snapshotid+"Post.ttl"
            foo["vals"]+=[
                          self.online_prefix+self.pfile,
                          self.pfile,
                          self.online_prefix+self.prdf,
                          self.online_prefix+self.pttl,
                          self.prdf,
                          self.pttl,
                          self.nposts,
                          int(self.totalchars),
                          self.mcharsposts,
                          self.dcharsposts,
                          int(self.totaltokens),
                          self.mtokensposts,
                          self.dtokensposts,
                          ]+list(self.postsvars)
        foo["uris"]+=[
                     NS.facebook.isGroup,
                     NS.facebook.isEgo,
                     NS.facebook.isFriendship,
                     NS.facebook.isInteraction,
                     NS.facebook.hasText,
                     NS.facebook.isPost,
                     ]
        self.isego=  bool(P.get(r.URIRef(self.snapshoturi),a,NS.po.EgoSnapshot  ))
        self.isgroup=bool(P.get(r.URIRef(self.snapshoturi),a,NS.po.GroupSnapshot))
        foo["vals"]+=[self.isgroup,self.isego,self.isfriendship,self.isinteraction,self.hastext,self.hastext]

        self.mrdf=self.snapshotid+"Meta.rdf"
        self.mttl=self.snapshotid+"Meta.ttl"

        self.desc="facebook network with snapshotID: {}\nsnapshotURI: {} \nisEgo: {}. isGroup: {}.".format(
                                                self.snapshotid,self.snapshoturi,self.isego,self.isgroup,)
        self.desc+="\nisFriendship: {}".format(self.isfriendship)
        if self.isfriendship:
           self.desc+="; nFriends: {}; nFrienships: {}.".format(self.nfriends,self.nfriendships,)
        self.desc+="\nisInteraction: {}".format(self.isinteraction)
        if self.isinteraction:
              self.desc+="; nInteracted: {}; nInteractions: {}.".format(self.ninteracted,self.ninteractions,)
        self.desc+="\nisPost: {} (alias hasText: {})".format(self.hastext,self.hastext)
        if self.hastext:
              self.desc+=";\nmCharsPostsOverall: {}; dCharsPostsOverall: {}; totalCharsOverall: {}; \
                          \nmTokensPostsOverall: {}; dTokensPostsOverall: {}; totalTokensOverall: {}".format(
                    self.nposts,
                    self.mcharsposts,self.dcharsposts,self.totalchars,
                    self.mtokensposts,self.dtokensposts,self.totaltokens,
                    )
        
        P.rdf.triplesScaffolding(self.snapshoturi,[ 
                                  NS.po.triplifiedIn,
                                  NS.po.triplifiedBy,
                                  NS.po.donatedBy,
                                  NS.po.availableAt,
                                  NS.po.onlineMetaXMLFile,
                                  NS.po.onlineMetaTTLFile,
                                  NS.po.metaXMLFileName,
                                  NS.po.metaTTLFileName,
                                  NS.po.acquiredThrough,
                                  NS.po.socialProtocolTag,
                                  NS.po.socialProtocol,
                                  NS.rdfs.comment,
                                  ]+foo["uris"],
                                  [
                                  datetime.datetime.now(),
                                  "scripts/",
                                  self.snapshotid[:-4],
                                  self.online_prefix,
                                  self.online_prefix+self.mrdf,
                                  self.online_prefix+self.mttl,
                                  self.mrdf,
                                  self.mttl,
                                  "Netvizz",
                                  "Facebook",
                                  P.rdf.ic(NS.po.Platform,"Facebook",self.meta_graph,self.snapshoturi),
                                  self.desc,
                                  ]+foo["vals"],
                                  self.meta_graph)
    def rdfGDFFriendshipNetwork(self,fnet):
        if sum([("user" in i) for i in fnet["individuals"]["label"]])==len(fnet["individuals"]["label"]):
            # nomes falsos, ids espurios
            self.friendships_anonymized=True
        else:
            self.friendships_anonymized=False
        tkeys=list(fnet["individuals"].keys())
        if "groupid" in tkeys:
            self.groupid=fnet["individuals"]["groupid"][0]
            tkeys.remove("groupid")
        else:
            self.groupid=None
        iname= tkeys.index("name")
        ilabel=tkeys.index("label")
        insert={"uris":[],"vals":[]}
        if self.friendships_anonymized:
            self.friendsvars=[trans(i) for j,i in enumerate(tkeys) if j not in (ilabel,iname)]
        else:
            self.friendsvars=[trans(i) for i in tkeys]
        count=0
        for tkey in tkeys:
            insert["uris"]+=[eval("NS.facebook."+trans(tkey))]
            insert["vals"]+=[fnet["individuals"][tkey]]
            count+=1
        self.nfriends=len(insert["vals"][0])
        insert_uris=insert["uris"][:]
        for vals_ in zip(*insert["vals"]): # cada participante recebe valores na ordem do insert_uris
            name_="{}-{}".format(self.snapshotid,vals_[iname])
            if self.friendships_anonymized:
                if vals_[ilabel] and ("user" not in vals_[ilabel]):
                    raise ValueError("Anonymized networks should have no informative name. Found: "+vals_[ilabel])
                insert_uris_=[el for i,el in enumerate(insert_uris) if i not  in (ilabel,iname) and vals_[i]]
                vals_=[el for i,el in enumerate(vals_) if (i not in (ilabel,iname)) and el]
            else:
                insert_uris_=[el for i,el in enumerate(insert_uris) if vals_[i]]
                vals_=[el for el in vals_ if el]
            ind=P.rdf.ic(NS.facebook.Participant,name_,self.friendship_graph,self.snapshoturi)
            P.rdf.triplesScaffolding(ind,insert_uris_+[NS.po.snapshot],vals_+[self.snapshoturi],context=self.friendship_graph)
        c("escritos participantes")
        friendships_=[fnet["relations"][i] for i in ("node1","node2")]
        i=0
        for uid1,uid2 in zip(*friendships_):
            uids=[r.URIRef(NS.facebook.Participant+"#{}-{}".format(self.snapshotid,i)) for i in (uid1,uid2)]
            flabel="{}-{}-{}".format(self.snapshotid,uid1,uid2)
            friendship_uri=P.rdf.ic(NS.facebook.Friendship,flabel,self.friendship_graph,self.snapshoturi)
            P.rdf.triplesScaffolding(friendship_uri,[NS.po.snapshot]+[NS.facebook.member]*2,
                                        [self.snapshoturi]+uids,self.friendship_graph)
            if (i%1000)==0:
                c("friendships",i)
            i+=1
        self.nfriendships=len(friendships_[0])
        c("escritas amizades")

    def rdfInteractionNetwork(self,fnet):
        #tg=P.rdf.makeBasicGraph([["po","fb"],[NS.po,NS.facebook]])
        if sum([("user" in i) for i in fnet["individuals"]["label"]])==len(fnet["individuals"]["label"]):
            # nomes falsos, ids espurios
            self.interactions_anonymized=True
        else:
            self.interactions_anonymized=False
        tkeys=list(fnet["individuals"].keys())
        if "groupid" in tkeys:
            self.groupid2=fnet["individuals"]["groupid"][0]
            tkeys.remove("groupid")
        else:
            self.groupid2=None
        iname= tkeys.index("name")
        ilabel=tkeys.index("label")
        if self.interactions_anonymized:
            self.varsfriendsinteraction=[trans(i) for j,i in enumerate(tkeys) if j not in (ilabel,iname)]
        else:
            self.varsfriendsinteraction=[trans(i) for i in tkeys]
        insert={"uris":[],"vals":[]}
        for tkey in tkeys:
            insert["uris"]+=[eval("NS.facebook."+trans(tkey))]
            insert["vals"]+=[fnet["individuals"][tkey]]
        self.ninteracted=len(insert["vals"][0])
        insert_uris=insert["uris"][:]
        for vals_ in zip(*insert["vals"]):
            name_="{}-{}".format(self.snapshotid,vals_[iname])
            if self.interactions_anonymized:
                insert_uris_=[el for i,el in enumerate(insert_uris) if i not  in (ilabel,iname) and vals_[i]]
                vals_=[el for i,el in enumerate(vals_) if i not  in (ilabel,iname) and vals_[i]]
            else:
                insert_uris_=[el for i,el in enumerate(insert_uris) if vals_[i]]
                vals_=[el for i,el in enumerate(vals_) if vals_[i]]
            ind=P.rdf.ic(NS.facebook.Participant,name_,self.interaction_graph,self.snapshoturi)
            P.rdf.triplesScaffolding(ind,insert_uris_+[NS.po.snapshot],vals_+[self.snapshoturi],self.interaction_graph)
        c("escritos participantes")
        self.interactionsvarsfoo=["node1","node2","weight"]
        interactions_=[fnet["relations"][i] for i in self.interactionsvarsfoo]
        self.ninteractions=len(interactions_[0])
        self.interactionsvars=["iFrom","iTo","weight"]
        i=0
        for uid1,uid2,weight in zip(*interactions_):
            weight_=int(weight)
            if weight_-weight != 0:
                raise ValueError("float weights in fb interaction networks?")
            iid="{}-{}-{}".format(self.snapshotid,uid1,uid2)
            ind=P.rdf.ic(NS.facebook.Interaction,iid,self.interaction_graph,self.snapshoturi)
            
            uids=[r.URIRef(NS.facebook.Participant+"#{}-{}".format(self.snapshotid,i)) for i in (uid1,uid2)]
            P.rdf.triplesScaffolding(ind,[NS.facebook.iFrom,NS.facebook.iTo]+[NS.po.snapshot,NS.facebook.weight],uids+[self.snapshoturi,weight_],self.interaction_graph)
            if (i%1000)==0:
                c("interactions: ", i)
            i+=1
        c("escritas interações")

def trans(tkey):
    if tkey=="name":
        return "numericID"
    if tkey=="label":
        return "name"
    if tkey=="posts":
        return "nPosts"
    return tkey

