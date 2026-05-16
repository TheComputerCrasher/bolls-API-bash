# Bolls API command-line client
A utility for easily accessing the [bolls.life API](https://github.com/Bolls-Bible/bain/blob/master/docs/API.md) to get specific portions of the Bible from a CLI. Human-readable text is the default, but can be changed to raw JSON for scripting or whatever with the ```-j``` / ```--raw-json``` flag (see [Usage](README.md#Usage) section below). 

This script has only been tested on English translations, but it should be general enough that it works with other languages as well.

## Dependencies
[```python3```](https://github.com/python/cpython) (obviously), [```pycurl```](https://pypi.org/project/pycurl/), [```jq```](https://pypi.org/project/jq/), and either internet access or a [local copy of bolls.life](https://github.com/Bolls-Bible/bain/blob/master/docs/LOCAL_DEV_WITH_DOCKER_COMPOSER.md). If using the latter, change BASE_URL in line 23 to https://bolls.local or http://localhost:8080 or whatever.

## Installation
Download [bolls.py](bolls.py), put it wherever you'd like, and run ```python3 /path/to/bolls.py``` ```<subcommands>```. I recommend putting this under an alias like ```bolls``` (which is what I use for the example commands). 

## Packaging?
If you want to package this script for your own OS or PyPI or whatever, that's great, but I will not be doing so. Besides, this is a CLI-only program and pretty much everything supports Python fairly easily, so I recommend the installation method above unless you want to share your packaged version with the rest of the internet. 

## License
I (TheComputerCrasher) put this under the Unlicense (public domain license) since the code was written by ChatGPT Codex (generative AI). Only the ideas, some small edits, and this README are truly mine. Feel free to use this in your own projects if you would like (especially the people at [bolls.life](https://bolls.life/))! Credit is appreciated but not required, as per the Unlicense. 

## Usage
(taken from ```bolls --help```)

Command flags (choose one):

```-h``` / ```--help```
* Show this help page

```-d``` / ```--list-dictionaries```
* List all available Hebrew/Greek dictionaries

```-D``` / ```--define``` ```<dictionary> <Hebrew/Greek word>```
* Get definitions for a Hebrew or Greek word

```-t``` / ```--list-translations```
* List all available Bible translations

```-b``` / ```--books``` ```<translation>```
* List all books of a chosen translation

```-v``` / ```--verse``` ```<translation(s)> <book> [chapter(s) [:verse(s)] ]```
* Get text from the Bible

```-r``` / ```--random``` ```<translation>```
* Get a single random verse

```-s``` / ```--search <translation> <search term> [options]```
* Search text in verses

Search options (choose any amount or none when using -s):

```-m``` / ```--match-case``` 
* Make search case-sensitive

```-w``` / ```--match-whole```
* Only search complete phrase matches (requires multiple words)

```-B``` / ```--book``` ```<book/ot/nt>``` 
* Search in a specific book, or in just the Old or New Testament

```-p``` / ```--page``` ```<#>``` -
* Go to a specific page of the search results

```-l``` / ```--page-limit``` ```<#>```
* Limits the number of pages of search results

Notes:
* ```<book>``` can be a number or a name.
* ```<translation>``` must be the abbreviation, not the full name. Multiple translations are separated by commas.
* ```[verse(s)]``` and ```[chapter(s)]``` can be a single number, multiple numbers separated by commas (e.g. 1,5,9), or a range (e.g. 13-17).
* Use / to use multiple ```-v``` commands at once (see examples).
* In -v, omit verses to get a full chapter, and omit chapters to get the full book.

Modifier flags (choose one or none):

```-j``` / ```--raw-json```
* Disable formatting

```-i``` / ```--include-all```
* Include everything (verse id, translation, book number, etc.) in -v

```-C``` / ```--include-comments```
* Include commentary (currently not working)

```-f``` / ```--file```
* Save output to a .txt or .json file in current working directory

```-n``` / ```--no-api```
* Use local translation files for -v (downloads if missing one, refuses if missing two or more)

```-u``` / ```--url```
* Print the URL (and POST body) that would have been called from the API

Examples:

```bolls -d```
* Lists all the available dictionaries.

```bolls -D BDBT אֹ֑ור```
* Translates אֹ֑ור to English using Brown-Driver-Briggs' Hebrew Definitions / Thayer's Greek Definitions.

```bolls --translations```
* Lists all the available Bible translations.

```bolls --books AMP```
* Lists the books in the Amplified translation.

```bolls --verses ESV genesis 1 --no-api --include-comments``` and ```bolls -v esv 1 1 -n -C```
* Shows the text of Genesis 1 from a downloaded copy of the English Standard Version, including commentary.

```bolls --verses nlt,nkjv exodus 2:1,5,7 -a```
* Shows Exodus 2:1, 2:5, and 2:7 from both the New Living Translation and the New King James Version, with all the descriptive information for each verse.

```bolls -v niv jon 1:1-3 / esv luk 2 / ylt,nkjv deu 6:5```
* Shows John 1:1-3 from the New International Version, Luke 2 from the English Standard Version, and Deuteronomy 6:5 from Young's Literal Translation and the New King James Version.

```bolls --verses niv 1 Corinthians```
* Shows the entirety of 1 Corinthians from the New international Version.

```bolls -r MSG -j -f```
* Shows a random verse from the Message translation in raw JSON format, and saves it to a file.

```bolls --random nlt -u```
* Shows the URL that the script would have used to get a random verse from the New Living Translation.

```bolls --search YLT --match-case --match-whole --page-limit 3 Jesus wept``` and ```bolls -s ylt -m -w -l 3 Jesus wept```
* Searches Young's Literal Translation for "Jesus wept", case-sensitive and matching the entire phrase, with a limit of 3 pages.


## TODO
* Add headers to each chapter when it shows multiple chapters (e.g. "Genesis 1", "Luke 2")
  * Maybe add verse numbers to the header as well
* Make verse and chapter numbers optional
* Maybe make --no-api work with other commands, but at that point I'd practically just be remaking bolls.life's API
* Maybe figure out how I want to share this with the internet, but this is kinda a niche project and anyone can freely edit if they find it so may not be worth it
