import percolation as P, social as S, rdflib as r, builtins as B, re, datetime, os, shutil
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

    def __init__(self,data_path="../data/fb/",filename_friendship="foo.gdf",filename_interaction="foo_interaction.gdf",\
            final_path="./fb/",snapshoturi=None,snapshotid=None):
        #self.snapshotid=snapshotid
        #self.snapshot=self.snapshoturi=snapshoturi
        self.isfriendship= bool(filename_friendship)
        self.isinteraction=bool(filename_interaction)
        fnet=readGDF(data_path+fname)     # return networkx graph
        fnet_=self.rdfGDFFriendshipNetwork(fnet)   # return rdflib graph
        if B.interaction:
            inet=readGDF(dpath+fnamei)    # return networkx graph
            inet_=rdfInteractionNetwork(inet)      # return rdflib graph
        else:
            inet_=0
        for i in locals():
            if i !="self":
                exec("self.{}={}".format(i,locals()[i]))
        meta=makeMetadata(fnet_,inet_)     # return rdflib graph with metadata about the structure
        writeAllFB(fnet_,inet_,meta)  # write linked data tree

    def writeAllFB(self,fnet,inet,mnet):
        fpath_="{}{}/".format(self.final_path,aname)
        if self.friendship:
            P.rdf.writeAll(fnet,self.snapshotid+"Friendship",fpath_,False,1)
        if self.interaction:
            P.rdf.writeAll(inet,self.snapshotid+"Interaction",fpath_)
        # copia o script que gera este codigo
        if not os.path.isdir(self.final_path+"scripts"):
            os.mkdir(self.final_path+"scripts")
        shutil.copy(self.scriptpath,self.final_path+"scripts/")
        # copia do base data
        if not os.path.isdir(self.final_path+"base"):
            os.mkdir(self.final_path+"base")
        shutil.copy(self.data_path+self.filename_friendships,self.final_path+"base/")
        if self.isinteraction:
            shutil.copy(self.data_path+self.filename_interaction,self.final_path+"base/")
            tinteraction="""\n{} individuals with metadata {}
and {} interactions with metadata {} constitute the interaction 
network in file:
{}
or
{}
(anonymized: {}).""".format( self.ninteracted,str(self.interactedvars),
                        self.ninteractions,str(self.interactionvars),
                        self.online_prefix+"/rdf/"+self.irdf,
                        self.online_prefix+"/rdf/"+self.ittl,
                        self.interactions_anononymized)
            originals="{}/data/{}\n{}/data/{}".format(self.online_prefix,self.filename_friendships,
                                                      self.online_prefix,self.filename_interactions)
        else:
            tinteraction=""
            originals="{}/data/{}".format(self.online_prefix,self.filename_friendships)
        P.rdf.writeAll(mnet,aname+"Meta",fpath_,1)
        # faz um README
        with open(fpath_+"README","w") as f:
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
                            Friendship network: {isf}
                            Interaction network: {isi}
                            All files should be available at the git repository:
                            {ava}
                            \n""".format(
                snapid=self.snapshotid,date=self.datetime_string,
                nf=self.nfriends,fvars=str(self.fvars),
                        nfs=self.nfriendships,
                        frdf=self.frdf,fttl=self.fttl,
                        fan=self.friendships_anonymized,
                        tinteraction=tinteraction,
                        mrdf=self.mrdf,
                        mttl=self.mttl,
                        origs=originals,
                        ise=self.isego,
                        isf=self.isfriendship,
                        isi=self.isinteraction,
                        ava=self.available_dir
                        ))

    def makeMetadata(self,fnet,inet):
        if self.groupid and self.groupid2 and (self.groupid!=self.groupid2):
            raise ValueError("Group IDS are different")
        tg2=P.rdf.makeBasicGraph([["po","fb"],[P.rdf.ns.per,P.rdf.ns.fb]])
        snapshot=P.rdf.IC([tg2],P.rdf.ns.po.FacebookSnapshot,aname,"Snapshot {}".format(self.snapshotid))

        foo={"uris":[],"vals":[]}
        self.online_prefix=online_prefix="https://raw.githubusercontent.com/OpenLinkedSocialData/{}master/".format(umbrella_dir)
        if self.friendship:
            foo["uris"]+=[
                         NS.fb.onlineOriginalFriendshipFile,
                         NS.fb.originalFriendshipFilename,
                         NS.po.onlineFriendshipXMLFile,
                         NS.po.onlineFriendshipTTLFile,
                         NS.po.FriendshipXMLFilename,
                         NS.po.FriendshipTTLFilename,
                         ]+\
                         [
                         NS.fb.nFriends,
                         NS.fb.nFriendships,
                         NS.fb.friendshipsAnonymized 
                         ]+\
                         [NS.fb.frienshipParticipantAttribute]*len(self.friendsvars)
            self.ffile=ffile="{}/base/{}".format(online_prefix,self.filename_friendship)
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
                         self.friendships_anonymized]+list(self.friendsvars)

        if self.interaction:
            foo["uris"]+=[
                          NS.fb.onlineOriginalInteractionFile,
                          NS.fb.originalInteractionFilename,
                          NS.po.onlineInteractionXMLFile,
                          NS.po.onlineinteractionTTLFile,
                          NS.po.interactionXMLFilename,
                          NS.po.interactionTTLFilename,
                          ]+\
                        [ NS.fb.nInteracted,
                          NS.fb.nInteractions,
                          NS.fb.interactionsAnonymized ]+\
                        [ NS.fb.interactionParticipantAttribute ]*len(self.interactionsvars)
            ifile="{}/base/{}".format(online_prefix,self.snapshotid)
            irdf="{}Interaction.rdf".format(online_prefix,self.snapshotid)
            ittl="{}Interaction.ttl".format(online_prefix,self.snapshotid)
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
                     NS.fb.isGroup,
                     NS.fb.isEgo,
                     NS.fb.isFriendship,
                     NS.fb.isInteraction,
                     ]
        foo["vals"]+=[self.isEgo,self.isFriendship,self.isInteraction]

        #https://github.com/OpenLinkedSocialData/fbGroups/tree/master/AdornoNaoEhEnfeite29032013_fb
        self.available_dir=available_dir=online_prefix+self.snapshotid
        mrdf="{}Meta.rdf".format(self.snapshotid)
        mttl="{}Meta.ttl".format(self.snapshotid)
        desc="facebook network from {} . Ego: {}. Friendship: {}. Interaction: {}.\
                nfriends: {}; nfrienships: {}; ninteracted: {}; ninteractions: ".format(
                    self.name,self.isego,self.isfriendship,self.isinteraction,
                    self.nfriends,self.nfriendships,self.ninteracted,self.ninteractions)
        P.rdf.link([tg2],snapshot,[ 
                                  NS.po.triplifiedIn,
                                  NS.po.donatedBy,
                                  NS.po.availableAt,
                                  NS.po.onlineMetaXMLFile,
                                  NS.po.onlineMetaTTLFile,
                                  NS.po.MetaXMLFilename,
                                  NS.po.MetaTTLFilename,
                                  NS.po.acquiredThrough,
                                  NS.po.socialProtocolTag,
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
                                  desc,
                                  ]+foo["vals"],
                                  "Snapshot {}".format(self.snapshot))
        ind2=P.rdf.IC([tg2],NS.po.Platform,"Facebook")
        P.rdf.linkClasses([tg2],snapshot,"Snapshot {}".format(aname),
                   [NS.po.socialProtocol],
                   [ind2],
                   )
        return tg2
    def rdfGDFFriendshipNetwork(self,fnet):
        tg=P.rdf.makeBasicGraph([["po","facebook"],[NS.po,NS.facebook]])
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
        if self.friendships_anonymized:
            self.friendsvars=[trans(i) for j,i in enumerate(tkeys) if j not in (ilabel,iname)]
        else:
            self.friendsvars=[trans(i) for i in tkeys]
        insert={"uris":[],"vals":[]}
        for tkey in tkeys:
            insert["uris"]+=[eval("NS.facebook."+trans(tkey))]
            insert["vals"]+=[fnet["individuals"][tkey]]
        self.nfriends=len(insert["vals"][0])
        insert_uris=insert["uris"][:]
        for vals_ in zip(*insert["vals"]): # cada participante recebe valores na ordem do insert_uris
            name_="{}-{}".format(self.snapshotid,vals_[iname])
            if friendships_anonymized:
                insert_uris_=[el for i,el in enumerate(insert_uris) if i not  in (ilabel,iname) and vals_[i]]
                vals_=[el for i,el in enumerate(vals_) if (i not in (ilabel,iname)) and el]
            else:
                insert_uris_=[el for i,el in enumerate(insert_uris) if vals_[i]]
                vals_=[el for i,el in vals_ if el]
            ind=P.rdf.IC([tg],P.rdf.ns.fb.Participant,name_)
            P.rdf.link([tg],ind,insert_uris_,vals_)
            P.rdf.link_([tg],ind,[NS.po.snapshot],[snapshot])
        if self.friendships_anonymized:
            B.friends_vars=[trans(i) for j,i in enumerate(tkeys) if j not in (ilabel,iname)]
        else:
            B.friends_vars=[trans(i) for i in tkeys]
        c("escritos participantes")
        friendships_=[fnet["relations"][i] for i in ("node1","node2")]
        i=0
        for uid1,uid2 in zip(*friendships_):
            uids=[r.URIRef(NS.facebook.Participant+"#{}-{}".format(self.snapshotid,vals_[iname])) for i in (uid1,uid2)]
            P.rdf.link_([tg],uids[0],[P.rdf.ns.fb.friend],[uids[1]])
            # make friendship
            flabel="{}-{}-{}".format(self.snapshotid,uids[0],uids[1])
            ind=P.rdf.IC([tg],P.rdf.ns.fb.Friendship,flabel)
            P.rdf.link_([tg],ind,flabel,[NS.facebook.snapshot]+[NS.facebook.member]*2,
                                        [snapshot]+uids)
            if (i%1000)==0:
                c("friendships",i)
            i+=1
        self.nfriendships=len(friendships_[0])
        c("escritas amizades")
        return tg

    def rdfInteractionNetwork(fnet):
        tg=P.rdf.makeBasicGraph([["po","fb"],[NS.po,NS.facebook]])
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
            self.varsiriendsinteraction=[trans(i) for i in tkeys]
        insert={"uris":[],"vals":[]}
        for tkey in tkeys:
            insert["uris"]+=[eval("NS."+trans(tkey))]
            insert["vals"]+=[fnet["individuals"][tkey]]
        self.ninteracted=len(insert["vals"][0])
        insert_uris=foo["uris"][:]
        for vals_ in zip(*insert["vals"]):
            name_="{}-{}".format(self.snapshotid,vals_[iname])
            if self.interactions_anonymized:
                insert_uris_=[el for i,el in enumerate(insert_uris) if i not  in (ilabel,iname) and vals_[i]]
                vals_=[el for i,el in enumerate(vals_) if i not in (ilabel,iname)]
            else:
                insert_uris_=[el for i,el in enumerate(insert_uris) if vals_[i]]
                vals_=[el for i,el in vals_ if el]
            ind=P.rdf.IC([tg],P.rdf.ns.fb.Participant,name_)
            P.rdf.link([tg],ind,insert_uris,vals_)
            P.rdf.link_([tg],ind,[NS.po.snapshot],[snapshot])
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
            ind=P.rdf.IC([tg],NS.Interaction,iid)
            
            uids=[r.URIRef(NS.facebook.Participant+"#{}-{}".format(self.snapshotid,i)) for i in (uid1,uid2)]
            P.rdf.link_([tg],ind,[NS.facebook.iFrom,P.rdf.ns.fb.iTo]+[NS.po.snapshot],uids+[self.snapshot])
            P.rdf.link([tg],ind,[ NS.facebook.weight],[weight_])
            if (i%1000)==0:
                c("interactions: ", i)
            i+=1
        c("escritas interações")
        return tg

def trans(tkey):
    if tkey=="name":
        return "uid"
    if tkey=="label":
        return "name"
    return tkey

