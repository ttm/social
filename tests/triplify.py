import social as S

# ss = S.facebook.access.parseLegacyFiles()
# ss=[i for i in ss if i.endswith("gdf_fb")]
# last_triplification_class = S.facebook.render.publishAll(ss)

# ss=S.twitter.access.parseLegacyFiles()
# ss = S.twitter.access.parseLegacyFiles('../../socialLegacy/data/tw/')
# ss = S.twitter.access.parseLegacyFiles()
# ss = [i for i in ss if 'porn' not in i and 'fuck' not in i]
# ss = [i for i in ss if not any([j in i for j in ('Syria','art','MAMA2015','QuartaSemRacismoClubeSDV','ForaDilma','obama','science','god','porn','SnapDetremura')])]
# last_triplification_class = S.twitter.render.publishAll(ss)

ss = S.irc.access.parseLegacyFiles()
ss = [i for i in ss if any([j in i for j in ('labmacambira_lalenia.txt', '#foradoeixo.log', '#matehackers_.log')])]
print(ss)
last_triplification_class = S.irc.render.publishAll(ss)
