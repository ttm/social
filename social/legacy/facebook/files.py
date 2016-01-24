files_dict={}

data=[
("RenatoFabbri19112014.gdf",None,"781909429","renato.fabbri"),
("PedroPauloRocha10032013.gdf",None,"836944624","dpedropaulorocha"),
("MarceloSaldanha19112014.gdf",None,"100000028547604","IBEBrasil"),
("MariliaPisani06052014.gdf",None,"100000812625301","marilia.pisani"),
("RafaelReinehr09042013.gdf",None,"814059950","reinehr"), #gml better
("VJPixel23052014.gdf",      None,"614664810",None),
("MassimoCanevacci19062013.gdf",     None,"1327838394","massimo.canevacci"),
("RenatoFabbri06022014.gdf",     None,"781909429","renato.fabbri"),
]

for snapshot in data:
    files_dict["ego_"+snapshot[0]]=snapshot[2:]

data=[ # AVLAB
("CalebLuporini25022014.gdf",     None,"1110305437","calebml"),
("DanielGonzales23022014.gdf",    None,"100002080034739","daniel.gonzalezxavier"),
("JoaoMekitarian23022014.gdf",    None,"100002080034739","joaopaulo.mekitarian"),
("MariliaPisani25022014.gdf",     None,"100000812625301","marilia.pisani"),
("RenatoFabbri22022014.gdf",      None,"781909429","renato.fabbri"),
("FelipeBrait23022014.gdf",       None,"1420435978","felipe.brait"),
("JulianaSouza23022014.gdf",      None,"520322516","juliana.desouza2"),
("NatachaRena22022014.gdf",       None,"665770837","natacha.rena"),
("SarahLuporini25022014.gdf",     None,"1528620900","sarah.cura"),
("CamilaBatista23022014.gdf",     None,"100001707143512","camila.batista.3382"),
("KarinaGomes22022014.gdf",       None,"100000176551181","karina.gomes.71"),
("OrlandoCoelho22022014.gdf",     None,"1060234340","orlando.coelho.98"),
("SatoBrasil25022014.gdf",        None,"1060234340","sato.dobrasil"),
("CarlosDiego25022014.gdf",       None,"689266676","cdiegosr"),
("PalomaKliss25022014.gdf",       None,"100008456088732",0),
("CristinaMekitarian23022014.gdf",None,"1771691370","cristina.mekitarian"),
("MarcelaLucatelli25022014.gdf",  None,"520656478","justinamoira"),
("PedroRocha25022014.gdf",None,"836944624","dpedropaulorocha"),
("JoaoMeirelles25022014.gdf",     None,"1194439813","joao.meirelles.10"),
("LucasOliveira26022014.gdf",     None,"1060987164",0),
('BarthorLaZule22022014.gdf',None,"100000077490764","barthor.la.zule"),
("FideoFuturalista22022014.gdf",     None,None,None),
]

for snapshot in data:
    files_dict["avlab_"+snapshot[0]]=snapshot[2:]

data=[ # POSAVLAB
        ("AnaCelia18032014.gdf",None,"1450596979",0),
        ("FabiBorges08032014.gdf",None,"598339469","antennarush"),
        ("RicardoPoppi18032014.gdf",None,"100000099352333","ricardopoppi"),
        ("ElenaGarnelo04032014.gdf",None,"1361932044","elena.garnelo"),
        ("GeorgeSanders08032014.gdf",None,"1347483608","george.sander"),
        ("GrazielleMachado18032014.gdf",None,"1847090892","GrazielleMachado"),
        ("RenatoFabbri19032014.gdf",None,"781909429","renato.fabbri")
        ]

for snapshot in data:
    files_dict["posavlab_"+snapshot[0]]=snapshot[2:]

data=[
        ("AntonioAnzoategui18022013.gml",None,"100003608428288","antonio.anzoateguifabbri"),
        ("BrunoMialich31012013.gml",None,"10000045475708","bruno.mialich"),
        ("CalebLuporini13042013.gml",None,"1110305437","calebml"),
        ("CalebLuporini19022013.gml",None,"1110305437","calebml"),
        ("CamilaBatista23022014.gml",None,"100001707143512","camila.batista.3382"),
        ("DanielPenalva18022013.gml",None,"100000077490764","barthor.la.zule"),
        ("GabrielaThume19022013.gml",None,"100002011676407","gabithume"),
        ("GrahamForrest28012013.gml",None,0,0),
        ("LailaManuelle17012013.gml",None,"1713144485","laila.manuelle"),
        ("LarissaAnzoategui20022013.gml",None,"1760577842","larissa.chogui"),
        ("LuisCirne07032013.gml",None,"717903828","lufcirne"),
        ("MariliaMelloPisani10042013.gml",None,"100000812625301","marilia.pisani"),
        ("Mirtes16052013.gml",None,0,0),
        ("PedroPauloRocha10032013.gml",None,"836944624","dpedropaulorocha"),
        ("PeterForrest28012013.gml",None,"770029747","peter.forrest.18"), # ateh aqui ok
        ("RafaelReinehr09042013.gml",None,"814059950","reinehr"), #gml better
        ("RamiroGiroldo20022013.gml",None,"100001810878626","ramiro.giroldo"),
        ('RenatoFabbri02032013.gml',None,"781909429","renato.fabbri"),
        ("RenatoFabbri03032013.gml",None,"781909429","renato.fabbri"),
        ("RenatoFabbri11072013.gml",None,"781909429","renato.fabbri"),
        ("RenatoFabbri18042013.gml",None,"781909429","renato.fabbri"),
        ("RenatoFabbri20012013.gml",None,"781909429","renato.fabbri"),
        ("RenatoFabbri29112012.gml",None,"781909429","renato.fabbri"),
        ("RicardoFabbri18022013.gml",None,"1011765","ricardofabbri"),
        ("RitaWu08042013.gml",None,"100009639240215",0),
        ("RonaldCosta12062013.gml",None,"1457302032","scherolt"),
        ("ThaisTeixeira19022013.gml",None,"100001089120349","thais.t.fabbri"),
        ("VilsonVieira18022013.gml",None,"529899682","aut0mata"),
        ("ViniciusSampaio18022013.gml",None,"529899682","sampaio.vinicius"),
        ]

for snapshot in data:
    files_dict[snapshot[0]]=snapshot[2:]

data=[("AdornoNaoEhEnfeite29032013.gdf","AdornoNaoEhEnfeite29032013_interacoes.gdf","265217103529531",0,"https://www.facebook.com/groups/265217103529531/permalink/525654127485826/"),
("PartidoPirata23032013.gdf","PartidoPirata23032013_interactions.gdf",0,"partidopiratabrasil","https://www.facebook.com/groups/partidopiratabrasil/permalink/10151409024509317/"),
("DemocraciaPura06042013.gdf","DemocraciaPura06042013_interactions.gdf",0,"democraciapura","https://www.facebook.com/groups/democraciapura/permalink/310907215704321/"),
("ComputerArt10032013.gdf","ComputerArt10032013_interactions.gdf",0,"computerart","https://www.facebook.com/groups/computerart/permalink/259389137529870/"),
("RedeTranzmidias02032013.gdf","RedeTranzmidias02032013_interactions.gdf","318333384951196",0,"https://www.facebook.com/groups/318333384951196/permalink/346658712118663/"),
("AtivistasDaInclusaoDigital09032013.gdf","AtivistasDaInclusaoDigital09032013_interactions.gdf","423602557691243",0,"https://www.facebook.com/groups/423602557691243/permalink/525201037531394/"),
("Coolmeia06032013.gdf","Coolmeia06032013_interacoes.gdf", 0,"coolmeia",["https://www.facebook.com/groups/coolmeia/permalink/380091142098291/","https://www.facebook.com/groups/coolmeia/permalink/489757754464962/"]),
("Economia14042013.gdf","Economia14042013_interactions.gdf",0,"economa1","https://www.facebook.com/groups/economa1/permalink/586007714743535/"),
("DemocraciaDiretaJa14032013.gdf","DemocraciaDiretaJa14032013_interacoes.gdf",0,"ddjbrasil","https://www.facebook.com/groups/ddjbrasil/permalink/347023325397298/"),
("EconomiaCriativaDigital03032013.gdf","EconomiaCriativaDigital03032013_interactions.gdf",0,"economiacriativadigital","https://www.facebook.com/groups/economiacriativadigital/permalink/438313682916103/"),
("PoliticasCulturasBrasileiras08032013.gdf","PoliticasCulturasBrasileiras08032013_interacoes.gdf",0,"pcult","https://www.facebook.com/groups/pcult/permalink/519626544747423/"),
("Auricultura10042013.gdf","Auricultura10042013_interactions.gdf","373029202732392",0,0),
("CienciasComFronteiras29032013.gdf","CienciasComFronteiras29032013_interacoes.gdf",0,"contraaexclusao","https://www.facebook.com/groups/contraaexclusao/permalink/269103356558439/"),
("LivingBridgesPlanet29032013.gdf","LivingBridgesPlanet29032013_interactions.gdf",0,"livingbridgesplanet","https://www.facebook.com/groups/livingbridgesplanet/permalink/352950408144951/"),
("MobilizacoesCulturaisInteriorSP13032013.gdf","MobilizacoesCulturaisInteriorSP13032013_interacoes.gdf","131639147005593",0,"https://www.facebook.com/groups/131639147005593/permalink/144204529082388/"),
("PracaPopular16032013.gdf","PracaPopular16032013_interactions.gdf","215924991863921",0,"https://www.facebook.com/groups/215924991863921/permalink/319279541528465/"),
("SolidarityEconomy12042013.gdf","SolidarityEconomy12042013_interactions.gdf","9149038282",0,"https://www.facebook.com/groups/9149038282/permalink/10151461945623283/"),
("StudyGroupSNA05042013.gdf","StudyGroupSNA05042013_interactions.gdf","140630009439814",0,"https://www.facebook.com/groups/140630009439814/permalink/151470598355755/"),
("THackDay26032013.gdf","THackDay26032013_interacoes.gdf",0,"thackday",0),
("SiliconValleyGlobalNetwork27042013.gdf","SiliconValleyGlobalNetwork27042013_interactions.gdf","109971182359978",0,"https://www.facebook.com/groups/109971182359978/permalink/589326757757749/"),
("DemocraciaDiretaJa14072013.gdf","DemocraciaDiretaJa14072013_interacoes.gdf",0,"ddjbrasil","https://www.facebook.com/groups/ddjbrasil/permalink/347023325397298/"),
("Tecnoxamanismo08032014.gdf","Tecnoxamanismo08032014_interactions.gdf","505090906188661",0,["https://www.facebook.com/groups/505090906188661/permalink/733144993383250/","https://www.facebook.com/groups/505090906188661/permalink/733157380048678/"]),
("Tecnoxamanismo15032014.gdf","Tecnoxamanismo15032014_interactions.gdf","505090906188661",0,["https://www.facebook.com/groups/505090906188661/permalink/733144993383250/","https://www.facebook.com/groups/505090906188661/permalink/733157380048678/"]),
("Latesfip08032014.gdf","Latesfip08032014_interactions.gdf","183557128478424",0,"https://www.facebook.com/groups/183557128478424/permalink/266610616839741/"),
("EducacoesEAprendizagensXXI02032013.gdf",None,None,"geaxxi","https://www.facebook.com/groups/geaxxi/permalink/433229973421182/"),
("RedeCulturaDigitalIndigena08042013.gdf",None,None,"",""),
]

for snapshot in data:
    files_dict[snapshot[0]]=snapshot[2:]

