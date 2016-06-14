import social as S

# ss = S.facebook.access.parseLegacyFiles()
# ss=[i for i in ss if i.endswith("gdf_fb")]
# last_triplification_class = S.facebook.render.publishAll(ss)

# ss=S.twitter.access.parseLegacyFiles()
# ss = S.twitter.access.parseLegacyFiles('../../socialLegacy/data/tw/')
ss = S.twitter.access.parseLegacyFiles()
# ss = [i for i in ss if 'porn' not in i and 'fuck' not in i]
last_triplification_class = S.twitter.render.publishAll(ss)

# ss=S.irc.access.parseLegacyFiles()
# ss=[i for i in ss if i.endswith("gdf_fb")]
# last_triplification_class=S.irc.render.publishAll(ss)
