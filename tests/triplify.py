import social as S
import os

# ss = S.facebook.access.parseLegacyFiles()
# last_triplification_class = S.facebook.render.publishAll(ss)
# ss_ = []
# ids = os.listdir('./facebook_snapshots')
# for snap in ss:
#     if not any([id_ in snap for id_ in ids]):
#         ss_.append(snap)
#     else:
#         print('found snapshot: '+snap)
# # # ss=[i for i in ss if i.endswith("gdf_fb")]
# # ss=[i for i in ss if 'AntonioAnzo' in i]
# last_triplification_class = S.facebook.render.publishAll(ss_)
#
# ss = S.twitter.access.parseLegacyFiles()
# # last_triplification_class = S.twitter.render.publishAll(ss)
# ss_ = []
# ids = os.listdir('./twitter_snapshots')
# for snap in ss:
#     if not any([id_ in snap for id_ in ids]):
#         ss_.append(snap)
#     else:
#         print('found snapshot: '+snap)
# ss_ = [i for i in ss_ if 'Syria' not in i and 'porn' not in i]
# last_triplification_class = S.twitter.render.publishAll(ss_)

# ss = S.twitter.access.parseLegacyFiles('../../socialLegacy/data/tw/')
# ss = [i for i in ss if 'porn' not in i and 'fuck' not in i]
# ss = [i for i in ss if not any([j in i for j in ('Syria','art','MAMA2015','QuartaSemRacismoClubeSDV','ForaDilma','obama','science','god','porn','SnapDetremura')])]
#
ss = S.irc.access.parseLegacyFiles()
# # ss = [i for i in ss if any([j in i for j in ('labmacambira_lalenia.txt', '#foradoeixo.log', '#matehackers_.log')])]
# # ss = [i for i in ss if any([j in i for j in ('foradoeixo.log', 'matehackers_.log')])]
# ss = [i for i in ss if any([j in i for j in ('oradoeixo.log',)])]
# ss = [i for i in ss if any([j in i for j in ('hackerspace-cps',)])]
# print(ss)
last_triplification_class = S.irc.render.publishAll(ss)
