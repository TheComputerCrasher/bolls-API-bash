# bolls-API-bash
A utility for accessing the [bolls.life API](https://bolls.life/api) from a Bash terminal. Human-readable text is the default, but can be changed to raw JSON with the ```-j``` flag (see Documentation section below).

## Dependencies
Required: ```python3```, ```curl```, and internet access.

Optional: ```jq``` (required for pretty-printing and other formatting)

## Installation
Download bolls.sh, put it wherever you'd like, and add ```source /path/to/bolls.sh``` to your .bashrc. If your terminal does not use Bash, too bad. 

## License
I put this under the CC0 (public domain) license since the code is not mine at all and was written by AI. Only the ideas and a couple small edits are truly mine. Feel free to use this in your own projects if you would like (especially the people at [bolls.life](https://bolls.life))!

## Documentation
(taken from ```bolls --help```)

Flags:

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
  Compare one or multiple verses in the same chapter across translations
  (the translations must have the same books, or this will compare different verses)

  ```-r``` / ```--random``` ```<translation>```
  Get a random verse

  ```-f``` / ```--define`````` <dictionary> <Hebrew/Greek word>```
  Get definitions for a Hebrew or Greek word

  ```-j``` / ```--raw-json```
  Use with another flag to output raw JSON
  (disable pretty-printing and other formatting)

Note:
  <book> can be a number or a name (case-insensitive), even in JSON files.

Examples:
  ```bolls -t
  bolls --list-dictionaries
  bolls -b AMP
  bolls --verse ESV John 3 16
  bolls -a MSG
  bolls --get-verses '[{"translation":"NIV","book":Luke,"chapter":2,"verses":[15,16,17]}]'
  bolls -s NIV Luke 2 '15,16,17'
  bolls --parallel 'NKJV,NLT' John 1 '1,2,3,4,5'
  bolls -p '{"translations":["NKJV","NLT"],"book":62,"chapter"1,"verses":[1,2,3,4,5]}'
bolls --define BDBT אֹ֑ור
```
