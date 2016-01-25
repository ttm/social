from percolation.rdf import NS, a
subClassOf=NS.rdfs.subClassOf
subPropertyOf=NS.rdfs.subPropertyOf
po=NS.po; social=NS.social
def snapshots():
    triples=[
            (po.EgoSnapshot,       subClassOf, po.Snapshot),
            (po.FriendshipSnapshot,subClassOf, po.Snapshot),
            (po.FacebookSnapshot,  subClassOf, po.Snapshot),

            (po.FacebookEgoFriendshipSnapshot,subClassOf,FacebookSnapshot),
            (po.FacebookEgoFriendshipSnapshot,subClassOf,FriendshipSnapshot),
            (po.FacebookEgoFriendshipSnapshot,subClassOf,EgoSnapshot),

            (po.FacebookGroupFriendshipSnapshot, subClassOf,po.FacebookSnapshot),
            (po.FacebookGroupFriendshipSnapshot, subClassOf,po.GroupSnapshot),
            (po.FacebookGroupFriendshipSnapshot, subClassOf,po.FriendshipSnapshot),

            (po.FacebookGroupFriendshipInteractionSnapshot, subClassOf,po.FacebookGroupFriendshipSnapshot),
            (po.FacebookGroupFriendshipInteractionSnapshot, subClassOf,po.InteractionSnapshot),

            (po.rawFile,NS.rdfs.range, po.File),

            (social.nFacebookParsedFiles,subPropertyOf,social.nParsedFiles),

            (po.GroupFriendshipNetwork, subClassOf, po.FriendshipNetwork),
            (po.EgoFriendshipNetwork, subClassOf, po.FriendshipNetwork),
            ]
    return triples
