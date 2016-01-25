# social
the python package to access data from social networking platforms and protocols (twitter, facebook, diáspora, instagram, irc, etc)
and render them as RDF linked data.

## core features
  - gadgets to access resources through search, stream and bots.
  - routines to render RDF data publication from data in other formats.
  - compliance to the percolation package for harnessing open linked social data.

## install with
    $ pip install social
or

    $ python setup.py install

For greater control of customization (and debugging), clone the repo and install with pip with -e:

    $ git clone https://github.com/ttm/social.git
    $ pip install -e <path_to_repo>
This install method is especially useful when reloading modified module in subsequent runs of social
(usually with the standard importlib).

Social is integrated to percolation packageto enable anthropological physics experiments and social harnessing:
- https://github.com/ttm/percolation

## coding conventions
A function name has a verb if it changes state of initialized objects, if it only "returns something", it is has no verb in name.

Classes, functions and variables are writen in CamelCase, headlessCamelCase and lowercase, respectively.
Underline is used only in variable names where the words in variable name make something unreadable (usually because the resulting name is big).

The code is the documentation. Code should be very readable to avoid writing unnecessary documentation and duplicating routine representations. This adds up to using docstrings to give context to the objects or omitting the docstrings.

Tasks might have c("some status message") which are printed with time interval in seconds between P.check calls.
These messages are turned of by setting S.QUIET=True or calling S.silence() which just sets S.QUIET=True

The usual variables in scripts are: P for percolation, NS for P.rdf.NS for namespace, a for NS.rdf.type, c for P.utils.check, S for social, r for rdflib, x for networkx

The file cureimport.py in newtests avoids cluttering the header of the percolation file while hacking framework. In using the Python interpreter, subsequent runs of scripts don't reload or raise error with importlib if the prior error was on load. Just load it first: import cureimport, percolation as P, etc.

Every feature should be related to at least one legacy/ outline.

Routines should be oriented towards making RDF data from data in other formats or towards retrieving data from social networking platforms and protocols.

### package structure
Data not in RDF are kept in the data/ directory.
Rendered RDF data should be in S.PERCOLATIONDIR="~./percolation/rdf/" unless otherwise specified.
Each platform/protocol has an umbrella module in which are modules for accessing current data in platforms
and expressing them as RDF.
This package relies heavily in the percolation package to assist rendering of RDF data.


#### the modules are:
bootstrap.py for starting system with basic variables and routines

facebook/\*
- render.py for expressing contents of gml, gdf and tab files in RDF. 
- access.py for access to data in the facebook platform (through bots and other interfaces)
- ontology.py for ontological relations used in facebook data


twitter/\*
- render.py for rendering contents of tweets in RDF. 
- access.py for access to data in from twitter through standard search and stream and through other APIs. 

irc/\*
- render.py for rendering contents of irc logs in RDF. 
- access.py for access to IRC logs (through a list of current URLs of text logs). 

legacy/\* for standard usage outlines of data access, rendering and publication, should mimic social modules structure
- conversion for .gdf files conversion to networkx graphs and networkx graphs to rdf.


other modules envisioned are:
- instagram
- diaspora
- flickr
- noosfero
- blogs (for platforms such as wordpress and blogger)

## usage example
```python
import social as S

S.legacy.facebook.publishLegacy() # publish as rdf all fb structures shipped with social
S.legacy.twitter.publishLegacy() # publish as rdf all tw structures shipped with social
S.legacy.irc.publishLegacy() # publish as rdf all irc structures shipped with social

S.twitter.access.stream("#love",100) # get tweets with the #love hashtag on the fly
S.twitter.render.publish("#love") # publish as rdf the tweets on #love obtained

# start a browser bot to retrieve the friendship network of the user
S.facebook.access.MeBot(login="me@memail.com",password="mepassword","ego_friendship")
S.facebook.render.renderFromBot("ego") # publish as rdf 


S.irc.access.logs("alogname") # alogname from S.irc.acces.logs.available()
S.irc.render.renderLog("alogname") # publish as rdf 
```

## further information
Analyses and media rendering of the published RDF data is dealt with by the percolation package: https://github.com/ttm/percolation

Gmane email data expression as RDF is available through the gmane package: https://github.com/ttm/gmane

Social participation platform data is available through the participation package: https://github.com/ttm/participation
