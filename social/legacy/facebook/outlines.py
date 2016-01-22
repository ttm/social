import social as S
def triplifyOneSnapshot():
    S.facebook.access.parseLegacyFiles()
    snapshoturi=P.get((None,a,NS.po.FacebookSnapshot))[0]
    S.facebook.renderAny(snapshoturi)
def publishOneSnapshot():
    S.facebook.access.parseLegacyFiles()
    snapshoturi=P.get((None,a,NS.po.FacebookSnapshot))[0]
    S.facebook.publishAny(snapshoturi)

