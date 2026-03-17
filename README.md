# Bolls API - Bash client
A utility for easily accessing the [bolls.life API](https://bolls.life/api/) to get specific portions of the Bible from a Bash terminal or script. Human-readable text is the default, but can be changed to raw JSON for scripting or whatever with the ```-j``` / ```--raw-json``` flag (see Usage section below).

## Dependencies
Required: ```python3```, ```curl```, and internet access.

Optional: ```jq``` (required for pretty-printing and other formatting)

## Installation
Download bolls.sh, put it wherever you'd like, and add ```source /path/to/bolls.sh``` to your .bashrc. If your terminal and/or scripts do not use Bash, you'll have to translate this script to another Shell language. 

## License
I put this under the CC0 (public domain) license since the code was written by AI. Only the ideas and a couple small edits are truly mine. Feel free to use this in your own projects if you would like (especially the people at [bolls.life](https://bolls.life/))! Credit is appreciated but not required, as per the CC0 licence.

## Usage
(taken from ```bolls --help```)

Command flags:

  ```-h``` / ```--help```
  Show the help page

  ```-t``` / ```--list-translations```
  List all available Bible translations

  ```-d``` / ```--list-dictionaries```
  List all available Hebrew/Greek dictionaries

  ```-b``` / ```--books``` ```<translation>```
  List all books of a chosen translation

  ```-c``` / ```--chapter``` ```<translation> <book> <chapter>```
  Get an entire chapter

  ```-v``` / ```--verse``` ```<translation> <book> <chapter> <verse(s)>```
  Get one or multiple verses from the same chapter

  ```-p``` / ```--parallel``` ```<translations> <book> <chapter> <verse(s)>``` OR ```--parallel <JSON array or file>```
  Compare one or multiple verses from the same chapter across translations
  (the translations must have the same books, or this will compare different verses)

  ```-r``` / ```--random``` ```<translation>```
  Get a random verse

  ```-f``` / ```--define``` ``` <dictionary> <Hebrew/Greek word>```
  Get definitions for a Hebrew or Greek word

Note:
  <book> can be a number or a name (case-insensitive), even in JSON files.

Modifier flags:

  ```-j``` / ```--raw-json```
  Disable formatting

Examples:
  ```bolls --translations
  bolls -d
  bolls --books AMP
  bolls -a MSG
  bolls --verse '[{"translation":"NIV","book":Luke,"chapter":2,"verses":[15,16,17]}]'
  bolls -s NIV Luke 2 '15,16,17'
  bolls --parallel 'NKJV,NLT' John 1 '1,2,3,4,5'
  bolls -p '{"translations":["NKJV","NLT"],"book":62,"chapter"1,"verses":[1,2,3,4,5]}'
bolls --define BDBT אֹ֑ור
```
## TODO
* for ```--chapter``` specifically, make only the "text" (and optionally "comment" using [bolls.life/get-text](https://bolls.life/get-text/) instead of [bolls.life/get-chapter](https://bolls.life/get-chapter/) with a ```--no-comments``` /```-c``` flag) attributes available when formatting is enabled
* add a ```--search``` / ```-s``` flag using [bolls.life/api/#Search](https://bolls.life/api/#Search)
* remove the ```pk``` JSON key by default and add a ```--p``` / ```--include-pk``` flag that keeps it
* add a ```-n``` / ```--no-comments``` flag that removes the ```comment``` JSON key
