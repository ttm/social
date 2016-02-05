import percolation as P, social as S, numpy as n, pickle, dateutil, nltk as k, os, datetime, shutil, rdflib as r, codecs
from percolation.rdf import NS, U, a, po, c
class LogPublishing:
    def __init__(self,snapshoturi,snapshotid,filename="foo.txt",\
            data_path="../data/irc/",final_path="./irc_snapshots/",umbrella_dir="irc_snapshots/"):
        c(snapshoturi,snapshotid,filename)
        isego=False
        isgroup=True
        isfriendship=False
        isinteraction=True
        hastext=True
        interactions_anonymized=False

        irc_graph="social_log"
        meta_graph="social_irc_meta"
        social_graph="social_irc"
        P.context(irc_graph,"remove")
        P.context(meta_graph,"remove")
        final_path_="{}{}/".format(final_path,snapshotid)
        online_prefix="https://raw.githubusercontent.com/OpenLinkedSocialData/{}master/{}/".format(umbrella_dir,snapshotid)
        locals_=locals().copy(); del locals_["self"]
        for i in locals_:
            exec("self.{}={}".format(i,i))
        self.rdfLog()
        self.makeMetadata()
        self.writeAllIRC()
    def rdfLog(self):
        with codecs.open(self.data_path+self.filename,"rb","iso-8859-1")  as f:
            text=textFix(f.read())
        # msgregex=r"\[(\d{2}):(\d{2}):(\d{2})\] \* ([^ ?]*)[ ]*(.*)" # DELETE ???
        #rmessage= r"\[(\d{2}):(\d{2}):(\d{2})\] \<(.*?)\>[ ]*(.*)" # message
        # lista arquivos no dir
        rdate=r"(\d{4})(\d{2})(\d{2})" # date
        rsysmsg=r"(\d{4})\-(\d{2})\-(\d{2})T(\d{2}):(\d{2}):(\d{2})  \*\*\* (\S+) (.*)" # system message (?)
        rmsg=r"(\d{4})\-(\d{2})\-(\d{2})T(\d{2}):(\d{2}):(\d{2})  \<(.*?)\> (.*)" # message
        messages=re.findall(rmsg,t)
        system_messages=re.findall(rsysmsg,t)
        NICKS=set([Q(i[-2]) for i in messages]+[Q(i[-2]) for i in system_messages])
        triples=[]
        for message in messages:
            year, month, day, hour, minute, second, nick, text=message
            nick=Q(nick)
            # achar direct message com virgula! TTM

            tokens=k.word_tokenize(text)
            tokens=[i for i in tokens if i not in set(string.punctuation)]
            direct_nicks=[] # for directed messages at
            mention_nicks=[] # for mentioned fellows
            direct=1
            for token in tokens:
                if token not in NICKS:
                    direct=0
                else:
                    if direct:
                        direct_nicks+=[token]
                    else:
                        mendion_nicks+=[token]
            text_=text[text.index(nicks2[-1])+len(nicks2[-1])+1:].lstrip()

            useruri=P.rdf.ic(po.Participant,"{}-{}".format(self.snapshoturi,nick),self.irc_graph,self.snapshoturi)
            triples+=[
                    (useruri,po.nick,nick),
                    ]

        locals_=locals().copy(); del locals_["self"]
        for i in locals_:
            exec("self.{}={}".format(i,i))

    def makeMetadata(self):
        pass
    def writeAllIRC(self):
        pass

strange="Ã¡","Ã ","Ã¢","Ã£","Ã¤","Ã©","Ã¨","Ãª","Ã«","Ã­","Ã¬","Ã®","Ã¯","Ã³","Ã²","Ã´","Ãµ","Ã¶","Ãº","Ã¹","Ã»","Ã¼","Ã§","Ã","Ã€","Ã‚","Ãƒ","Ã„","Ã‰","Ãˆ","ÃŠ","Ã‹","Ã","ÃŒ","ÃŽ","Ã","Ã“","Ã’","Ã”","Ã•","Ã–","Ãš","Ã™","Ã›","Ãœ","Ã‡","Ã"
correct="á", "à", "â", "ã", "ä", "é", "è", "ê", "ë", "í", "ì", "î", "ï", "ó", "ò", "ô", "õ", "ö", "ú", "ù", "û", "ü", "ç", "Á", "À", "Â", "Ã", "Ä", "É", "È", "Ê", "Ë", "Í", "Ì", "Î", "Ï", "Ó", "Ò", "Ô", "Õ", "Ö", "Ú", "Ù", "Û", "Ü", "Ç","Ú"
def textFix(string):
    # https://berseck.wordpress.com/2010/09/28/transformar-utf-8-para-acentos-iso-com-php/
    for st, co in zip(strange,correct):
        string=string.replace(st,co)
    return string


