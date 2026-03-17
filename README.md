# Bolls API - command-line client
A utility for easily accessing the [bolls.life API](https://bolls.life/api/) to get specific portions of the Bible from a Bash terminal or script. Human-readable text is the default, but can be changed to raw JSON for scripting or whatever with the ```-j``` / ```--raw-json``` flag (see Usage section below).

## Dependencies
Required: ```python3```, ```curl```, and internet access.

Optional (but recommended): ```jq``` (required for pretty-printing and formatting the JSON the API provides)

## Installation
Download bolls.sh, put it wherever you'd like, and add ```source /path/to/bolls.sh``` to your .bashrc. If your terminal and/or scripts do not use Bash, you'll have to translate this script to the Shell language you use. 

It'd probably be better if this was all Python or another non-Shell language, but when I started the project I didn't need any fancy Python stuff. I may consider changing it once all the major features I want are finished.

## License
I put this under the CC0 (public domain) license since the code was written by Codex-5.2 (generative AI). Only the ideas and some small edits are truly mine. Feel free to use this in your own projects if you would like (especially the people at [bolls.life](https://bolls.life/))! Credit is appreciated but not required, as per the CC0 licence.

## Usage
(taken from ```bolls --help```)

Command flags:

  ```-h``` / ```--help``` - Show this help page

  ```-t``` / ```--list-translations``` - List all available Bible translations

  ```-d``` / ```--list-dictionaries``` - List all available Hebrew/Greek dictionaries

  ```-b``` / ```--books``` ```<translation>``` - List all books of a chosen translation

  ```-c``` / ```--chapter``` ```<translation> <book> <chapter>``` - Get an entire chapter

  ```-v``` / ```--verse``` ```<translation> <book> <chapter> <verse(s)>``` - Get one or multiple verses from the same chapter

  ```-p``` / ```--parallel``` ```<translations> <book> <chapter> <verse(s)>``` OR ```--parallel <JSON array or file>``` - Compare one or multiple verses from the same chapter across translations (the translations must have the same books, or this will compare different verses)

  ```-r``` / ```--random``` ```<translation>``` - Get a random verse

  ```-f``` / ```--define``` ``` <dictionary> <Hebrew/Greek word>``` - Get definitions for a Hebrew or Greek word

Notes:
  <book> can be a number or a name (case-insensitive).
  <translation> must be the abbreviation, not the full name.

Modifier flags:

  ```-j``` / ```--raw-json``` - Disable formatting

  ```-i``` / ```--include-all``` - Include all JSON keys in -v and -c

  ```-n``` / ```--no-comment``` - Remove commentary from -c

Examples:
```
  bolls --translations
  bolls -d
  bolls --books AMP
  bolls -r MSG
  bolls --chapter -n Genesis 1
  bolls -v -i '[{"translation":"NIV","book":Luke,"chapter":2,"verses":[15,16,17]}]'
  bolls --verse NIV Luke 2 '15,16,17'
  bolls -p 'NKJV,NLT' John 1 '1,2,3,4,5'
  bolls --parallel '{"translations":["NKJV","NLT"],"book":62,"chapter"1,"verses":[1,2,3,4,5]}' -j
  bolls -s YLT haggi --match-case false --match-whole-word true --page-limit 128 --page 1
  bolls --search KJV love --book Genesis
  bolls -f BDBT אֹ֑וראֹ֑ור
```
## TODO
* Update README with latest changes
* Maybe translate into Python with ```pycurl``` and ```jq```
