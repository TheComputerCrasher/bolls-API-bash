# Bolls API command-line client
A utility for easily accessing the [bolls.life API](https://bolls.life/api/) to get specific portions of the Bible from a CLI. Human-readable text is the default, but can be changed to raw JSON for scripting or whatever with the ```-j``` / ```--raw-json``` flag (see [Usage](README.md#Usage) section below). 

## Dependencies
[```python3```](https://github.com/python/cpython) (obviously), [```pycurl```](https://pypi.org/project/pycurl/), [```jq```](https://pypi.org/project/jq/), and internet access.

In theory, the script should also work if you are running the site locally as per the [official docs](https://github.com/Bolls-Bible/bain/blob/master/docs/LOCAL_DEV_WITH_DOCKER_COMPOSER.md). You *should* just have to change ```BASE_URL``` in line 23 from https://bolls.life to https://bolls.local, but I have not tested this. 

## Installation
Download [bolls.py](bolls.py), put it wherever you'd like, and run ```python3 /path/to/bolls.py``` ```<subcommands>```. I recommend putting this under an alias like ```bolls``` (which is what I use for the example commands). 

## Packaging?
If you want to package this script for your own OS or PyPI or whatever, that's great, but I will not be doing so. Besides, this is a CLI-only program and pretty much everything supports Python fairly easily, so I recommend the installation method above unless you want to share your packaged version with the rest of the internet. 

## License
I (TheComputerCrasher) put this under the CC0 (public domain) license since the code was written by Codex-5.2 (generative AI). Only the ideas, some small edits, and this README are truly mine. Feel free to use this in your own projects if you would like (especially the people at [bolls.life](https://bolls.life/))! Credit is appreciated but not required, as per the CC0 licence. 

## Usage
(taken from ```bolls --help```)

Command flags (choose one):

* ```-h``` / ```--help``` - Show this help page

* ```-d``` / ```--list-dictionaries``` - List all available Hebrew/Greek dictionaries

* ```-D``` / ```--define``` ```<dictionary> <Hebrew/Greek word>``` - Get definitions for a Hebrew or Greek word

* ```-t``` / ```--list-translations``` - List all available Bible translations

* ```-b``` / ```--books``` ```<translation>``` - List all books of a chosen translation

* ```-v``` / ```--verse``` ```<translation(s)> <book> <chapter>[:<verse(s)>]``` - Get one or multiple verses from the same chapter

* ```-r``` / ```--random``` ```<translation>``` - Get a single random verse

* ```-s``` / ```--search <translation> <search term> [options]``` - Search text in verses

Search options (choose any amount or none when using -s):

* ```-m``` / ```--match-case``` - Make search case-sensitive

* ```-w``` / ```--match-whole``` - Only search complete phrase matches (requires multiple words)

* ```-B``` / ```--book ``` ```<book/ot/nt>``` - Search in a specific book, or in just the Old or New Testament

* ```-p``` / ```--page``` ```<#>``` - Go to a specific page of the search results

* ```-l``` / ```--page-limit``` ```<#>``` - Limits the number of pages of search results

Notes:

* ```<book>``` can be a number or a name (case-insensitive).

* ```<translation>``` must be the abbreviation (case-insensitive), not the full name. Multiple translations are separated by commas.

* ```<verse(s)>``` can be a single number, multiple numbers separated by commas (e.g. 1,5,9), or a range (e.g. 13-17).

Modifier flags (choose one or none):

* ```-j``` / ```--raw-json``` - Disable formatting

* ```-i``` / ```--include-all``` - Include everything (verse id, translation, book number, etc.) in -v

* ```-C``` / ```--include-comments``` - Include commentary (currently not working)

Examples:
```
bolls --translations
bolls -d
bolls --books AMP
bolls -r msg -j
bolls --verses esv Genesis 1
bolls -v esv 1 1 -j
bolls --verses nlt,nkjv genesis 1
bolls -v NIV Luke 2:15-17
bolls --verses niv,nkjv genesis 1:1-3 -c
bolls -v niv genesis 1
bolls -s ylt -m -w -l 3 -p 1 Jesus wept
bolls --search YLT --match-case --match-whole --page-limit 3 --page 1 Jesus wept
bolls -D BDBT אֹ֑ור
```

## TODO
* Allow -v to get verses from multiple chapters at once
* Maybe figure out how this would work for anyone running bolls.life locally as per the [official docs](https://github.com/Bolls-Bible/bain/blob/master/docs/LOCAL_DEV_WITH_DOCKER_COMPOSER.md).
* Maybe figure out how I want to share this with the internet, but this is kinda a niche project and anyone can freely edit if they find it so may not be worth it
