# Bolls API command-line client
A utility for easily accessing the [bolls.life API]([https://bolls.life/api/](https://github.com/Bolls-Bible/bain/blob/master/docs/API.md)) to get specific portions of the Bible from a CLI. Human-readable text is the default, but can be changed to raw JSON for scripting or whatever with the ```-j``` / ```--raw-json``` flag (see [Usage](README.md#Usage) section below). 

This script has only been tested on English translations, but it should be general enough that it works with other languages as well.

## Dependencies
[```python3```](https://github.com/python/cpython) (obviously), [```pycurl```](https://pypi.org/project/pycurl/), [```jq```](https://pypi.org/project/jq/), and either internet access or a [local copy of bolls.life](https://github.com/Bolls-Bible/bain/blob/master/docs/LOCAL_DEV_WITH_DOCKER_COMPOSER.md). If using the latter, change BASE_URL in line 23 to https://bolls.local or http://localhost:8080 or whatever.

## Installation
Download [bolls.py](bolls.py), put it wherever you'd like, and run ```python3 /path/to/bolls.py``` ```<subcommands>```. I recommend putting this under an alias like ```bolls``` (which is what I use for the example commands). 

## Packaging?
If you want to package this script for your own OS or PyPI or whatever, that's great, but I will not be doing so. Besides, this is a CLI-only program and pretty much everything supports Python fairly easily, so I recommend the installation method above unless you want to share your packaged version with the rest of the internet. 

## License
I (TheComputerCrasher) put this under the Unilicense (public domain license) since the code was written by Codex-5.2 (generative AI). Only the ideas, some small edits, and this README are truly mine. Feel free to use this in your own projects if you would like (especially the people at [bolls.life](https://bolls.life/))! Credit is appreciated but not required, as per the CC0 licence. 

## Usage
(taken from ```bolls --help```)

Command flags (choose one):

* ```-h``` / ```--help``` - Show this help page

* ```-d``` / ```--list-dictionaries``` - List all available Hebrew/Greek dictionaries

* ```-D``` / ```--define``` ```<dictionary> <Hebrew/Greek word>``` - Get definitions for a Hebrew or Greek word

* ```-t``` / ```--list-translations``` - List all available Bible translations

* ```-b``` / ```--books``` ```<translation>``` - List all books of a chosen translation

* ```-v``` / ```--verse``` ```<translation(s)> <book> [chapter(s) [:verse(s)] ]``` - Get text from the Bible

* ```-r``` / ```--random``` ```<translation>``` - Get a single random verse

* ```-s``` / ```--search <translation> <search term> [options]``` - Search text in verses

Search options (choose any amount or none when using -s):

* ```-m``` / ```--match-case``` - Make search case-sensitive

* ```-w``` / ```--match-whole``` - Only search complete phrase matches (requires multiple words)

* ```-B``` / ```--book``` ```<book/ot/nt>``` - Search in a specific book, or in just the Old or New Testament

* ```-p``` / ```--page``` ```<#>``` - Go to a specific page of the search results

* ```-l``` / ```--page-limit``` ```<#>``` - Limits the number of pages of search results

Notes:

* ```<book>``` can be a number or a name.

* ```<translation>``` must be the abbreviation, not the full name. Multiple translations are separated by commas.

* ```[verse(s)]``` and ```[chapter(s)]``` can be a single number, multiple numbers separated by commas (e.g. 1,5,9), or a range (e.g. 13-17).

* Use / to use multiple ```-v``` commands at once (see examples).

* In -v, omit verses to get a full chapter, and omit chapters to get the full book.

Modifier flags (choose one or none):

* ```-j``` / ```--raw-json``` - Disable formatting

* ```-i``` / ```--include-all``` - Include everything (verse id, translation, book number, etc.) in -v

* ```-C``` / ```--include-comments``` - Include commentary (currently not working)

* ```-f``` / ```--file``` - Save output to a .txt or .json file in current working directory

* ```-u``` ```--url``` - Print the URL (and POST body) that would have been called from the API

Examples:
```
bolls -d
bolls -D BDBT אֹ֑ור
bolls --translations
bolls --books AMP
bolls --verses esv Genesis 1
bolls -v esv 1 1
bolls --verses nlt,nkjv exodus 2:1,5,7 -a
bolls -v NIV Luke 2:15-17 -u
bolls --verses niv,nkjv genesis 1:1-3 -c
bolls -v nlt genesis 1:1-3 / esv luke 2 / ylt,nkjv deuteronomy 6:5
bolls --verses niv genesis -f
bolls -r msg -j
bolls --search ylt -m -w -l 3 -p 1 Jesus wept
bolls -s YLT --match-case --match-whole --page-limit 3 --page 1 Jesus wept
```

## TODO
* Most of all, try to reduce server load, which I think /get-verses does (please correct me if I'm wrong!)
* Use official [soft links](https://github.com/Bolls-Bible/bain/blob/master/docs/API.md#soft-links) instead of my own implementation of it ([they](https://github.com/Bohooslav) totally stole my idea frfr)
* Ensure double line breaks between chapters
* Figure out if /get-book works with pycurl
* Maybe figure out how I want to share this with the internet, but this is kinda a niche project and anyone can freely edit if they find it so may not be worth it
