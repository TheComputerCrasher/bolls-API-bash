# Bolls API command-line client
A utility for easily accessing the [bolls.life API](https://bolls.life/api/) to get specific portions of the Bible from a CLI. Human-readable text is the default, but can be changed to raw JSON for scripting or whatever with the ```-j``` / ```--raw-json``` flag (see [Usage](README.md#Usage) section below).

## Dependencies
[```python3```](https://github.com/python/cpython) (obviously), [```pycurl```](https://pypi.org/project/pycurl/), [```jq```](https://pypi.org/project/jq/), and internet acess.

In theory, the script should also work if you are running the site locally as per the [official docs](https://github.com/Bolls-Bible/bain/blob/master/docs/LOCAL_DEV_WITH_DOCKER_COMPOSER.md). You *should* just have to change ```BASE_URL``` in line 23 from https://bolls.life to https://bolls.local, but I have not tested this yet.

## Installation
Download [bolls.py](/bolls.py), put it wherever you'd like, and run ```python3 /path/to/bolls.py``` ```<subcommands>```. I recommend putting this under an alias like ```bolls``` (which is what I use for the example commands). 

## Packaging?
If you want to package this script for your own OS or PyPI or whatever, that's great, but I don't know how to do so and it seems too complicated for just a simple script like this. Besides, this is a CLI-only program and pretty much everything supports Python fairly easily, so I recommend the installation method above. 

## License
I (TheComputerCrasher) put this under the CC0 (public domain) license since the code was written by Codex-5.2 (generative AI). Only the ideas, some small edits, and this README are truly mine. Feel free to use this in your own projects if you would like (especially the people at [bolls.life](https://bolls.life/))! Credit is appreciated but not required, as per the CC0 licence.

## Usage
(taken from ```bolls --help```)

Command flags:

  ```-h``` / ```--help``` - Show this help page

  ```-t``` / ```--list-translations``` - List all available Bible translations

  ```-d``` / ```--list-dictionaries``` - List all available Hebrew/Greek dictionaries

  ```-b``` / ```--books``` ```<translation>``` - List all books of a chosen translation

  ```-c``` / ```--chapter``` ```<translation> <book> <chapter>``` - Get an entire chapter

  ```-v``` / ```--verse``` ```<translation> <book> <chapter> <verse(s)>``` - Get one or multiple verses from the same chapter

  ```-r``` / ```--random``` ```<translation>``` - Get a random verse

  ```-D``` / ```--define``` ```<dictionary> <Hebrew/Greek word>``` - Get definitions for a Hebrew or Greek word

  ```-p``` / ```--parallel``` ```<translations> <book> <chapter> <verse(s)>``` OR ```--parallel``` ```<JSON array or file>``` - Compare one or multiple verses from the same chapter across translations (the translations must have the same books, or this will compare different verses)

  ```-s``` / ```--search <translation> <search term> [options]``` - Search verses by text

  Search options:

  ```-m``` / ```--match-case``` ```<true/false>``` - Makes search case-sensitive

  ```-w``` / ```--match-whole-word``` ```<true/false>``` - Only search complete matches (e.g. "Dan" instead of "Daniel")

  ```-B``` / ```--book ``` ```<book/ot/nt>``` - Search in a specific book, or in just the Old or New Testament

  ```-P``` / ```--page``` ```<#>``` - Go to a specific page of the search results

  ```-l``` / ```--page-limit``` ```<#>``` - Limits the number of pages of search results

Notes:

  ```<book>``` can be a number or a name (case-insensitive).
  
  ```<translation>``` must be the abbreviation, not the full name (case-insensitive).

Modifier flags:

  ```-j``` / ```--raw-json``` - Disable formatting

  ```-i``` / ```--include-all``` - Include everything (verse id, translation, book number, etc.) in -v and -c

  ```-C``` / ```--include-comments``` - Include commentary in -c

Examples:
```
  bolls --translations
  bolls -d
  bolls --books AMP
  bolls -r msg
  bolls --chapter -C Genesis 1
  bolls -v -a '[{"translation":"niv","book":Luke,"chapter":2,"verses":[15,16,17]}]'
  bolls --verse niv luke 2 '15,16,17'
  bolls -p 'NKJV,NLT' John 1 '1,2,3,4,5'
  bolls --parallel '{"translations":["NKJV","NLT"],"book":62,"chapter"1,"verses":[1,2,3,4,5]}' -j
  bolls -s YLT haggi --match-case --match-whole-word --page-limit 128 --page 1
  bolls --search kjv love -B genesis
  bolls -D BDBT אֹ֑ור
```

## TODO
* Change ```-m``` and ```-w``` to not need a second parameter because there's only 2 options (true/false)
* ```--match-whole-word``` is reversed (true returns false and vice versa)
* Figure out how this would work for anyone running bolls.life locally as per the [official docs](https://github.com/Bolls-Bible/bain/blob/master/docs/LOCAL_DEV_WITH_DOCKER_COMPOSER.md)
* Maybe figure out how I want to share this with the internet, but this is kinda a niche project and anyone can freely edit if they find it so may not be worth it
