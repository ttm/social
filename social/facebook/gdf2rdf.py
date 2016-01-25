import percolation as P, social as S, rdflib as r, builtins as B, re, datetime, dateutil, os, shutil
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
        self.social_graph="social_facebook"
        P.context(self.friendship_graph,"remove")
        P.context(self.interaction_graph,"remove")
        P.context(self.meta_graph,"remove")
        self.snapshotid=snapshotid
        self.snapshot=self.snapshoturi=snapshoturi
        self.isfriendship= bool(filename_friendships)
        self.isinteraction=bool(filename_interactions)
        self.hastext=bool(filename_posts)
        fnet=readGDF(data_path+filename_friendships)     # return networkx graph
        fnet_=self.rdfGDFFriendshipNetwork(fnet)   # writes to self.friendship_graph
        if self.isinteraction:
            inet=readGDF(data_path+filename_interactions)    # return networkx graph
            self.rdfInteractionNetwork(inet)      # writes to self.interaction_graph
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
        data=[i.split("\t") for i in open(filename_posts_,"r")[:-1]]
        tvars=data[0]
        standard_vars=['id','type','message','created_time','comments','likes','commentsandlikes']
        if sum([i==j for i,j in zip(tvars,standard_vars)]):
            raise ValueError("the tab file format was not understood")
        data=data[1:]
        triples=[]
        for post in data:
            ind=P.rdf.ic(NS.facebook.Post,post[0],self.posts_graph,self.snapshoturi)
            triples+=[
                     (ind,NS.po.snapshot,self.snapshoturi),
                     (ind,NS.facebook.postID,post[0]),
                     (ind,NS.facebook.postType,post[1]),
                     (ind,NS.facebook.postText,post[2]),
                     (ind,NS.facebook.createdAt,dateutil.parser.parse(post[3])),
                     (ind,NS.facebook.nComments,post[4]),
                     (ind,NS.facebook.nLikes,post[5]),
                     ]
        P.add(triples,context=self.posts_graph)

    def writeAllFB(self):
        self.final_path_="{}{}/".format(self.final_path,self.snapshotid)
        if not os.path.isdir(self.final_path_):
            os.mkdir(self.final_path_)
        #fnet,inet,mnet
        if self.isfriendship:
            g=P.context(self.friendship_graph)
            g.serialize(self.final_path_+self.snapshotid+"Friendship.ttl","turtle"); c("ttl")
            g.serialize(self.final_path_+self.snapshotid+"Friendship.rdf","xml")
            c("serialized friendships")
        if self.isinteraction:
            g=P.context(self.interaction_graph)
            g.serialize(self.final_path_+self.snapshotid+"Interaction.ttl","turtle"); c("ttl")
            g.serialize(self.final_path_+self.snapshotid+"Interaction.rdf","xml")
            c("serialized interaction")
        if self.hastext:
            g=P.context(self.posts_graph)
            g.serialize(self.final_path_+self.snapshotid+"Posts.ttl","turtle"); c("ttl")
            g.serialize(self.final_path_+self.snapshotid+"Posts.rdf","xml")
            c("serialized posts")
        g=P.context(self.meta_graph)
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
        shutil.copy(self.data_path+self.filename_friendships,self.final_path_+"base/")
        if self.isinteraction:
            shutil.copy(self.data_path+self.filename_interactions,self.final_path_+"base/")
            tinteraction="""\n{} individuals with metadata {}
and {} interactions with metadata {} constitute the interaction 
network in file:
{}
or
{}
(anonymized: {}).""".format( self.ninteracted,str(self.varsfriendsinteraction),
                        self.ninteractions,str(self.interactionsvars),
                        self.online_prefix+"/rdf/"+self.irdf,
                        self.online_prefix+"/rdf/"+self.ittl,
                        self.interactions_anonymized)
            originals="{}data/{}\n{}data/{}".format(self.online_prefix,self.filename_friendships,
                                                      self.online_prefix,self.filename_interactions)
        else:
            tinteraction=""
            originals="{}data/{}".format(self.online_prefix,self.filename_friendships)
#        P.rdf.writeAll(mnet,aname+"Meta",fpath_,1)
        # faz um README
        datetime_string=P.get(r.URIRef(self.snapshoturi),NS.po.dateObtained,None,context="social_facebook")[2]
        if not os.path.isdir(self.final_path+"base"):
            os.mkdir(self.final_path+"base")
        with open(self.final_path_+"README","w") as f:
            f.write("""This repo delivers RDF data from the facebook
friendship network of {snapid} collected around {date}.
{nf} individuals with metadata {fvars}
and {nfs} friendships constitute the friendship network in file:
{frdf} \nor \n{fttl}
(anonymized: {fan}).
{tinteraction}
Metadata for discovery is in file:
{mrdf} \nor \n{mttl}
Original files:
{origs}
Ego network: {ise}
Group network: {isg}
Friendship network: {isf}
Interaction network: {isi}
All files should be available at the git repository:
{ava}
\n""".format(
                snapid=self.snapshotid,date=datetime_string,
                nf=self.nfriends,fvars=str(self.friendsvars),
                        nfs=self.nfriendships,
                        frdf=self.frdf,fttl=self.fttl,
                        fan=self.friendships_anonymized,
                        tinteraction=tinteraction,
                        mrdf=self.mrdf,
                        mttl=self.mttl,
                        origs=originals,
                        ise=self.isego,
                        isg=self.isgroup,
                        isf=self.isfriendship,
                        isi=self.isinteraction,
                        ava=self.available_dir
                        ))

    def makeMetadata(self):
        if self.groupid and self.groupid2 and (self.groupid!=self.groupid2):
            raise ValueError("Group IDS are different")
        foo={"uris":[],"vals":[]}
        self.online_prefix=online_prefix="https://raw.githubusercontent.com/OpenLinkedSocialData{}{}/master/".format(self.umbrella_dir,self.snapshotid)
        if self.isfriendship:
            foo["uris"]+=[
                         NS.facebook.onlineOriginalFriendshipFile,
                         NS.facebook.originalFriendshipFilename,
                         NS.po.onlineFriendshipXMLFile,
                         NS.po.onlineFriendshipTTLFile,
                         NS.po.friendshipXMLFilename,
                         NS.po.friendshipTTLFilename,
                         ]+\
                         [
                         NS.facebook.nFriends,
                         NS.facebook.nFriendships,
                         NS.facebook.friendshipsAnonymized 
                         ]+\
                         [NS.facebook.frienshipParticipantAttribute]*len(self.friendsvars)
            self.ffile=ffile="{}/base/{}".format(online_prefix,self.filename_friendships)
            self.frdf="{}Friendship.rdf".format(self.snapshotid)
            self.fttl="{}Friendship.ttl".format(self.snapshotid)
            foo["vals"]+=[
                         ffile,
                         self.filename_friendships,
                         online_prefix+"/rdf/"+self.frdf,
                         online_prefix+"/rdf/"+self.fttl,
                         self.frdf,
                         self.fttl,
                         self.nfriends,
                         self.nfriendships,
                         self.friendships_anonymized
                         ]+list(self.friendsvars)

        if self.isinteraction:
            foo["uris"]+=[
                         NS.facebook.onlineOriginalInteractionFile,
                         NS.facebook.originalInteractionFilename,
                         NS.po.onlineInteractionXMLFile,
                         NS.po.onlineinteractionTTLFile,
                         NS.po.interactionXMLFilename,
                         NS.po.interactionTTLFilename,
                         ]+\
                         [
                         NS.facebook.nInteracted,
                         NS.facebook.nInteractions,
                         NS.facebook.interactionsAnonymized 
                         ]+\
                         [NS.facebook.interactionParticipantAttribute]*len(self.interactionsvars)
            ifile="{}/base/{}".format(online_prefix,self.snapshotid)
            self.irdf=irdf="{}Interaction.rdf".format(online_prefix,self.snapshotid)
            self.ittl=ittl="{}Interaction.ttl".format(online_prefix,self.snapshotid)
            foo["vals"]+=[
                          ifile,
                          self.filename_interactions,
                          online_prefix+"/rdf/"+irdf,
                          online_prefix+"/rdf/"+ittl,
                          irdf,
                          ittl,
                          self.ninteractions,
                          self.ninteracted,
                          self.interactions_anonymized,
                          ]+list(self.interactionsvars)
        foo["uris"]+=[
                     NS.facebook.isGroup,
                     NS.facebook.isEgo,
                     NS.facebook.isFriendship,
                     NS.facebook.isInteraction,
                     ]
        self.isego=  bool(P.get(r.URIRef(self.snapshoturi),a,NS.po.EgoSnapshot  ))
        self.isgroup=bool(P.get(r.URIRef(self.snapshoturi),a,NS.po.GroupSnapshot))
        foo["vals"]+=[self.isgroup,self.isego,self.isfriendship,self.isinteraction]

        #https://github.com/OpenLinkedSocialData/fbGroups/tree/master/AdornoNaoEhEnfeite29032013_fb
        self.available_dir=available_dir=online_prefix+self.snapshotid
        self.mrdf=mrdf="{}Meta.rdf".format(self.snapshotid)
        self.mttl=mttl="{}Meta.ttl".format(self.snapshotid)
        if "ninteracted" not in dir(self):
            self.ninteracted,self.ninteractions=0,0
        desc="facebook network from {} . Ego: {}. Group: {}. Friendship: {}. Interaction: {}.\
                nfriends: {}; nfrienships: {}; ninteracted: {}; ninteractions: {}".format(
                    self.snapshotid,self.isego,self.isgroup,self.isfriendship,self.isinteraction,
                    self.nfriends,self.nfriendships,self.ninteracted,self.ninteractions)
        ind2=P.rdf.ic(NS.po.Platform,"Facebook",self.meta_graph,self.snapshoturi)
        P.rdf.triplesScaffolding(self.snapshoturi,[ 
                                  NS.po.triplifiedIn,
                                  NS.po.donatedBy,
                                  NS.po.availableAt,
                                  NS.po.onlineMetaXMLFile,
                                  NS.po.onlineMetaTTLFile,
                                  NS.po.metaXMLFilename,
                                  NS.po.metaTTLFilename,
                                  NS.po.acquiredThrough,
                                  NS.po.socialProtocolTag,
                                  NS.po.socialProtocol,
                                  NS.rdfs.comment,
                                  ]+foo["uris"],
                                  [
                                  datetime.datetime.now(),
                                  self.snapshotid[:-4],
                                  available_dir,
                                  online_prefix+"/rdf/"+mrdf,
                                  online_prefix+"/rdf/"+mttl,
                                  mrdf,
                                  mttl,
                                  "Netvizz",
                                  "Facebook",
                                  ind2,
                                  desc,
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
            if self.friendships_anonymized:
                if count not in (ilabel,iname):
                    insert["uris"]+=[eval("NS.facebook."+trans(tkey))]
                    insert["vals"]+=[fnet["individuals"][tkey]]
                count+=1
                continue
            insert["uris"]+=[eval("NS.facebook."+trans(tkey))]
            insert["vals"]+=[fnet["individuals"][tkey]]
            count+=1
        self.nfriends=len(insert["vals"][0])
        insert_uris=insert["uris"][:]
        for vals_ in zip(*insert["vals"]): # cada participante recebe valores na ordem do insert_uris
            name_="{}-{}".format(self.snapshotid,vals_[iname])
            if self.friendships_anonymized:
                insert_uris_=[el for i,el in enumerate(insert_uris) if i not  in (ilabel,iname) and vals_[i]]
                vals_=[el for i,el in enumerate(vals_) if (i not in (ilabel,iname)) and el]
            else:
                insert_uris_=[el for i,el in enumerate(insert_uris) if vals_[i]]
                vals_=[el for el in vals_ if el]
            ind=P.rdf.ic(NS.facebook.Participant,name_,self.friendship_graph,self.snapshoturi)
#            P.rdf.link([tg],ind,insert_uris_,vals_)
#            P.rdf.link_([tg],ind,[NS.po.snapshot],[snapshot])
            P.rdf.triplesScaffolding(ind,insert_uris_+[NS.po.snapshot],vals_+[self.snapshoturi],context=self.friendship_graph)
        c("escritos participantes")
        friendships_=[fnet["relations"][i] for i in ("node1","node2")]
        i=0
        for uid1,uid2 in zip(*friendships_):
            uids=[r.URIRef(NS.facebook.Participant+"#{}-{}".format(self.snapshotid,i)) for i in (uid1,uid2)]
#            P.add((uids[0],NS.facebook.friend,uids[1]),context=self.friendship_graph)
            # make friendship
            flabel="{}-{}-{}".format(self.snapshotid,uid1,uid2)
            ind=P.rdf.ic(NS.facebook.Friendship,flabel,self.friendship_graph,self.snapshoturi)
            P.rdf.triplesScaffolding(ind,[NS.po.snapshot]+[NS.facebook.member]*2,
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
            insert["uris"]+=[eval("NS.facebook.interactionUserAttribute"+"#"+trans(tkey))]
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
        self.interactionsvars=["node1","node2","weight"]
        interactions_=[fnet["relations"][i] for i in self.interactionsvars]
        self.ninteractions=len(interactions_[0])
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
        return "uid"
    if tkey=="label":
        return "name"
    return tkey

