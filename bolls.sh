# bolls.sh: Bash client for bolls.life API

bolls() {
  local base="https://bolls.life"
  local raw_json=0
  local include_all=0
  local no_comment=0
  local _args=()
  local _a
  for _a in "$@"; do
    case "$_a" in
      -j|--raw-json) raw_json=1 ;;
      -i|--include-all) include_all=1 ;;
      -n|--no-comments) no_comment=1 ;;
      *) _args+=("$_a") ;;
    esac
  done
  local cmd="${_args[0]:--h}"
  if [[ ${#_args[@]} -gt 0 ]]; then
    _args=("${_args[@]:1}")
  else
    _args=()
  fi
  set -- "${_args[@]}"

  # helper: pretty-print JSON if jq is available
  _bolls_pretty() {
    local json="$1"
    local jq_prefix="${2:-}"
    if [[ "$raw_json" -eq 1 ]]; then
      printf '%s' "$json"
      return 0
    fi
    if command -v jq >/dev/null 2>&1; then
      local filter
      filter="$(cat <<'JQ'

def indent($n): " " * ($n * 4);

def strip_html:
  if type == "string" then
    gsub("<(br|p) */?>"; "
") | gsub("<[^>]*>"; "")
  else . end;

def scalar:
  if . == null then "null"
  elif (type == "string") then (strip_html)
  else tostring
  end;

def is_scalar: (type == "string" or type == "number" or type == "boolean" or type == "null");

def keyfmt: gsub("_"; " ");

def fmt($v; $n):
  if ($v|type) == "object" then
    $v|to_entries|map(
      if (.value|type) == "object" or (.value|type) == "array" then
        "\(indent($n))\(.key|keyfmt):\n\(fmt(.value; $n+1))"
      else
        "\(indent($n))\(.key|keyfmt): \(.value|scalar)"
      end
    )|join("\n")
  elif ($v|type) == "array" then
    if ($v|length) == 0 then ""
    else
      ($v|map(fmt(.;$n))) as $lines
      | if ($v|all(is_scalar)) then ($lines|join("\n")) else ($lines|join("\n\n")) end
    end
  else
    "\(indent($n))\($v|scalar)"
  end;

fmt(.;0)

JQ
)"
      if [[ -n "$jq_prefix" ]]; then
        printf '%s' "$json" | jq -r "$jq_prefix | $filter"
      else
        printf '%s' "$json" | jq -r "$filter"
      fi
    else
      printf '%s' "$json"
    fi
  }

  # helper: jq prefix to keep only text/comment keys
  _bolls_jq_text_comment() {
    cat <<'JQ'

def keep_text_comment:
  if type == "array" then map(keep_text_comment)
  elif type == "object" then
    if (has("comment") and .comment != null) then
      [ .text, {comment} ]
    else
      .text
    end
  else .
  end;

keep_text_comment

JQ
  }

  # helper: jq prefix to keep only text and drop comments
  _bolls_jq_text_only() {
    cat <<'JQ'

def keep_text_only:
  if type == "array" then map(keep_text_only)
  elif type == "object" then
    .text
  else .
  end;

keep_text_only

JQ
  }


  # helper: perform GET
  _bolls_get() {
    local url="$1"
    local jq_prefix="${2:-}"
    local out
    out="$(curl -sS --fail "$url")" || return $?
    _bolls_pretty "$out" "$jq_prefix"
  }

  # helper: perform POST with JSON body (string or file)
  _bolls_post() {
    local url="$1"; local body="$2"; local jq_prefix="${3:-}"
    if [[ -z "$body" ]]; then
      echo "Error: POST body required" >&2; return 2
    fi
    # if body looks like a filename, read it
    local out
    if [[ -f "$body" ]]; then
      out="$(curl -sS --fail -H "Content-Type: application/json" -d @"$body" "$url")" || return $?
    else
      out="$(curl -sS --fail -H "Content-Type: application/json" -d "$body" "$url")" || return $?
    fi
    _bolls_pretty "$out" "$jq_prefix"
  }

  # helper: validate JSON string or file
  _bolls_validate_json() {
    local json_in="$1"
    if [[ -z "$json_in" ]]; then
      echo "Error: JSON required" >&2; return 2
    fi
    if [[ -f "$json_in" ]]; then
      python3 - <<'PY' "$json_in" || return 2
import json,sys
path = sys.argv[1]
try:
    with open(path, 'r', encoding='utf-8') as f:
        json.load(f)
except Exception as e:
    print(f"Invalid JSON in file {path}: {e}", file=sys.stderr)
    sys.exit(2)
PY
    else
      python3 - <<'PY' "$json_in" || return 2
import json,sys
s = sys.argv[1]
try:
    json.loads(s)
except Exception as e:
    print(f"Invalid JSON: {e}", file=sys.stderr)
    sys.exit(2)
PY
    fi
  }
  # helper: URL-encode a string
  _bolls_urlencode() {
    python3 - <<'PY' "$1" || return 2
import urllib.parse,sys
print(urllib.parse.quote(sys.argv[1]))
PY
  }

  # helper: normalize translation code to uppercase
  _bolls_norm_translation() {
    if [[ -z "$1" ]]; then
      return 0
    fi
    printf '%s' "$1" | tr '[:lower:]' '[:upper:]'
  }

  # helper: turn comma/space list into JSON array (strings or ints)
  _bolls_json_array() {
    local raw="$1"
    local kind="${2:-string}"
    python3 - <<'PY' "$raw" "$kind" || return 2
import json,sys
raw = sys.argv[1].strip()
kind = sys.argv[2]
if raw.startswith('['):
    try:
        json.loads(raw)
        print(raw)
        sys.exit(0)
    except Exception:
        pass
parts = []
for chunk in raw.split(','):
    chunk = chunk.strip()
    if not chunk:
        continue
    for p in chunk.split():
        if p:
            parts.append(p)
if kind == 'int':
    vals = []
    for p in parts:
        try:
            vals.append(int(p))
        except Exception:
            print(f"Invalid number in list: {p}", file=sys.stderr)
            sys.exit(2)
    print(json.dumps(vals))
else:
    print(json.dumps(parts))
PY
  }

  # helper: uppercase translation codes in a JSON array
  _bolls_uppercase_translations() {
    python3 - <<'PY' "$1" || return 2
import json,sys,os
arg = sys.argv[1]
try:
    if os.path.isfile(arg):
        with open(arg, 'r', encoding='utf-8') as f:
            data = json.load(f)
    else:
        data = json.loads(arg)
except Exception as e:
    print(f"Error: invalid translations JSON: {e}", file=sys.stderr)
    sys.exit(2)
if not isinstance(data, list):
    print("Error: translations must be a JSON array", file=sys.stderr)
    sys.exit(2)
out = [(v.upper() if isinstance(v, str) else v) for v in data]
print(json.dumps(out))
PY
  }

  # helper: get first translation from JSON array
  _bolls_first_translation() {
    local translations_json="$1"
    python3 - <<'PY' "$translations_json" || return 2
import json,sys
try:
    data = json.loads(sys.argv[1])
except Exception as e:
    print(f"Error: invalid translations JSON: {e}", file=sys.stderr)
    sys.exit(2)
if not isinstance(data, list) or not data:
    print("Error: translations list is empty!", file=sys.stderr)
    sys.exit(2)
print(data[0])
PY
  }

  # helper: resolve book name to number using translations_books.json
  _bolls_book_to_id() {
    local translation
      translation="$(_bolls_norm_translation "$1")"
    local book="$2"
    if [[ -z "$translation" || -z "$book" ]]; then
      echo "Error: translation and book required for lookup" >&2; return 2
    fi
    if [[ "$book" =~ ^[0-9]+$ ]]; then
      printf '%s' "$book"
      return 0
    fi
    local cache="${TMPDIR:-/tmp}/bolls_translations_books.json"
    if [[ ! -s "$cache" ]]; then
      curl -sS --fail "$base/static/bolls/app/views/translations_books.json" -o "$cache" || return $?
    fi
    python3 - <<'PY' "$cache" "$translation" "$book" || return 2
import json,sys,re
path, translation, book = sys.argv[1], sys.argv[2], sys.argv[3]
def norm(s):
    return re.sub(r'[^a-z0-9]+','', s.lower())
with open(path, 'r', encoding='utf-8') as f:
    data = json.load(f)
keys = {k.lower(): k for k in data.keys()}
tkey = translation.lower()
if tkey not in keys:
    print(f"Error: unknown translation '{translation}' for book lookup. \nTry bolls -t to see all available translations, and be sure to use the abbreviation.", file=sys.stderr)
    sys.exit(2)
t = keys[tkey]
target = norm(book)
candidates = []
for entry in data[t]:
    name = entry.get('name','')
    n = norm(name)
    if n == target:
        print(entry.get('bookid'))
        sys.exit(0)
    if n.startswith(target):
        candidates.append(entry)
if len(candidates) == 1:
    print(candidates[0].get('bookid'))
    sys.exit(0)
if len(candidates) > 1:
    print(f"Error: book name '{book}' is ambiguous for translation '{t}'.", file=sys.stderr)
    sys.exit(2)
print(f"Error: unknown book '{book}' for translation '{t}'. \nTry bolls -b '{t}' to find what book you\'re looking for.", file=sys.stderr)
sys.exit(2)
PY
  }

  # helper: normalize book names inside JSON bodies
  _bolls_normalize_books_in_json() {
    local json_in="$1"
    local mode="$2"
    local cache="${TMPDIR:-/tmp}/bolls_translations_books.json"
    if [[ ! -s "$cache" ]]; then
      curl -sS --fail "$base/static/bolls/app/views/translations_books.json" -o "$cache" || return $?
    fi
    python3 - <<'PY' "$json_in" "$mode" "$cache" || return 2
import json,sys,os,re
json_in, mode, cache = sys.argv[1], sys.argv[2], sys.argv[3]
def load_arg(arg):
    if os.path.isfile(arg):
        with open(arg, 'r', encoding='utf-8') as f:
            return json.load(f)
    return json.loads(arg)
def norm(s):
    return re.sub(r'[^a-z0-9]+','', s.lower())
with open(cache, 'r', encoding='utf-8') as f:
    data = json.load(f)
keys = {k.lower(): k for k in data.keys()}
def book_to_id(translation, book):
    if isinstance(book, int):
        return book
    if isinstance(book, str) and book.isdigit():
        return int(book)
    if not isinstance(book, str):
        return book
    tkey = translation.lower()
    if tkey not in keys:
        raise ValueError(f"unknown translation '{translation}' for book lookup. \nTry bolls -t to see all available translations, and be sure to use the abbreviation.")
    t = keys[tkey]
    target = norm(book)
    candidates = []
    for entry in data[t]:
        name = entry.get('name','')
        n = norm(name)
        if n == target:
            return entry.get('bookid')
        if n.startswith(target):
            candidates.append(entry)
    if len(candidates) == 1:
        return candidates[0].get('bookid')
    if len(candidates) > 1:
        raise ValueError(f"book name '{book}' is ambiguous for translation '{t}'")
    raise ValueError(f"unknown book '{book}' for translation '{t}'. \nTry bolls -b '{t}' to find what book you\'re looking for.")
try:
    obj = load_arg(json_in)
    if mode == 'get-verses':
        if not isinstance(obj, list):
            raise ValueError('get-verses JSON must be an array')
        for entry in obj:
            if not isinstance(entry, dict):
                raise ValueError('get-verses items must be objects')
            if 'translation' not in entry or 'book' not in entry:
                raise ValueError('get-verses items must include translation and book')
            if isinstance(entry.get('translation'), str):
                entry['translation'] = entry['translation'].upper()
            entry['book'] = book_to_id(entry['translation'], entry['book'])
    elif mode == 'parallel':
        if not isinstance(obj, dict):
            raise ValueError('parallel JSON must be an object')
        translations = obj.get('translations')
        if not translations or not isinstance(translations, list):
            raise ValueError('parallel JSON must include translations array')
        translations = [t.upper() if isinstance(t, str) else t for t in translations]
        obj['translations'] = translations
        if 'book' in obj:
            obj['book'] = book_to_id(translations[0], obj['book'])
    else:
        raise ValueError('unknown mode')
    print(json.dumps(obj))
except Exception as e:
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(2)
PY
  }

  case "$cmd" in
    -h|--help)
      cat <<'USAGE'

Command flags (choose one):
  -h / --help
  Show this help page

  -t / --translations
  List all available Bible translations

  -d / --dictionaries
  List all available Hebrew/Greek dictionaries

  -b / --books <translation>
  List all books of a chosen translation

  -v / --verse <translation> <book> <chapter> <verse(s)>
  Get one or multiple verses from the same chapter

  -c / --chapter <translation> <book> <chapter>
  Get an entire chapter

  -r / --random <translation>
  Get a random verse

  -f / --define <dictionary> <Hebrew/Greek word>
  Get definitions for a Hebrew or Greek word

  -p / --parallel <translations> <book> <chapter> <verse(s)>
  Compare verses across translations

  -s / --search <translation> <search term> [options]
  Search verses by text
  Search options (choose any amount or none):

    --match-case <true/false>

    --match-whole-word <true/false>

    --book <book/ot/nt>

    --page <#>
    
    --page-limit <#>

Notes:
  <book> can be a number or a name (case-insensitive).
  <translation> must be the abbreviation, not the full name (case-insensitive).


Modifier flags (choose one or none):

  -j / --raw-json
  Disable formatting

  -i / --include-all
  Include all JSON keys ("pk:", "translation:", "book", etc.) in -v and -c

  -n / --no-comments
  Remove commentary from -c


Examples:
  bolls --translations
  bolls -d
  bolls --books AMP
  bolls -r msg
  bolls --chapter -n Genesis 1
  bolls -v -i '[{"translation":"NIV","book":Luke,"chapter":2,"verses":[15,16,17]}]'
  bolls --verse niv Luke 2 '15,16,17'
  bolls -p 'NKJV,NLT' John 1 '1,2,3,4,5'
  bolls --parallel '{"translations":["NKJV","NLT"],"book":62,"chapter"1,"verses":[1,2,3,4,5]}' -j
  bolls -s YLT haggi --match_case false --match_whole-word true --page-limit 128 --page 1
  bolls --search kjv love --book Genesis
  bolls -f BDBT אֹ֑ור

USAGE
      return 0
      ;;
    --translations|-t)
      _bolls_get "$base/static/bolls/app/views/languages.json" ;;
    --dictionaries|-d)
      _bolls_get "$base/static/bolls/app/views/dictionaries.json" ;;
    --books|-b)
      if [[ -z "$1" ]]; then echo "Usage: bolls --books <translation>" >&2; return 2; fi
      local translation
      translation="$(_bolls_norm_translation "$1")"
      _bolls_get "$base/get-books/${translation}/" ;;
    --chapter|-c)
      if [[ -z "$1" || -z "$2" || -z "$3" ]]; then echo "Usage: bolls --chapter <translation> <book> <chapter>" >&2; return 2; fi
      local book_id
      local translation
      translation="$(_bolls_norm_translation "$1")"
      book_id="$(_bolls_book_to_id "$translation" "$2")" || return $?
      local jq_text_comment
      if [[ "$include_all" -eq 1 ]]; then
        jq_text_comment=""
      elif [[ "$no_comment" -eq 1 ]]; then
        jq_text_comment="$(_bolls_jq_text_only)"
      else
        jq_text_comment="$(_bolls_jq_text_comment)"
      fi
      _bolls_get "$base/get-chapter/${translation}/${book_id}/${3}/" "$jq_text_comment" ;;
    --verse|-v)
      # accepts full JSON array/file OR simple args
      if [[ -z "$1" ]]; then echo "Usage: bolls --verse <translation> <book> <chapter> <verses> OR bolls --verse <JSON array or file>" >&2; return 2; fi
      local jq_text_comment
      if [[ "$include_all" -eq 1 ]]; then
        jq_text_comment=""
      elif [[ "$no_comment" -eq 1 ]]; then
        jq_text_comment="$(_bolls_jq_text_only)"
      else
        jq_text_comment="$(_bolls_jq_text_comment)"
      fi
      if [[ -z "$2" && -z "$3" && -z "$4" ]]; then
        local body
        body="$(_bolls_normalize_books_in_json "$1" get-verses)" || return $?
        _bolls_post "$base/get-verses/" "$body" "$jq_text_comment"
        return $?
      fi
      if [[ -z "$2" || -z "$3" || -z "$4" ]]; then echo "Usage: bolls --verse <translation> <book> <chapter> <verses> OR bolls --verse <JSON array or file>" >&2; return 2; fi
      local translation
      translation="$(_bolls_norm_translation "$1")"
      local book="$2"
      local chapter="$3"
      local verses_json="$4"
      local book_id
      book_id="$(_bolls_book_to_id "$translation" "$book")" || return $?
      # normalize verses
      if [[ -f "$verses_json" ]]; then
        verses_json="$(cat "$verses_json")"
      else
        verses_json="$(_bolls_json_array "$verses_json" int)" || return $?
      fi
      local body
      body="$(printf '[{\"translation\":\"%s\",\"book\":%s,\"chapter\":%s,\"verses\":%s}]' "$translation" "$book_id" "$chapter" "$verses_json")"
      _bolls_validate_json "$body" || return $?
      _bolls_post "$base/get-verses/" "$body" "$jq_text_comment" ;;
    --search|-s)
      if [[ -z "$1" || -z "$2" ]]; then
        echo "Usage: bolls --search <translation> <search term> [--match_case <true/false>] [--match_whole <true/false>] [--book <book/ot/nt>] [--page <int>] [--limit <int>]" >&2; return 2
      fi
      local translation
      translation="$(_bolls_norm_translation "$1")"; shift
      local piece="$1"; shift
      local match_case=""
      local match_whole=""
      local book=""
      local page=""
      local limit=""
      while [[ $# -gt 0 ]]; do
        case "$1" in
          --match_case|--match-case)
            match_case="$2"; shift 2 ;;
          --match_whole|--match-whole|--match_whole_word|--match-whole-word)
            match_whole="$2"; shift 2 ;;
          --book)
            book="$2"; shift 2 ;;
          --page)
            page="$2"; shift 2 ;;
          --limit|--page_limit|--page-limit)
            limit="$2"; shift 2 ;;
          *)
            echo "Unknown search option: $1" >&2; return 2 ;;
        esac
      done
      if [[ -n "$book" ]]; then
        case "$book" in
          ot|nt|OT|NT)
            book="$(printf '%s' "$book" | tr 'A-Z' 'a-z')" ;;
          *)
            if [[ "$book" =~ ^[0-9]+$ ]]; then
              :
            else
              book="$(_bolls_book_to_id "$translation" "$book")" || return $?
            fi
            ;;
        esac
      fi
      local query
      query="search=$(_bolls_urlencode "$piece")"
      if [[ -n "$match_case" ]]; then query+="&match_case=$(_bolls_urlencode "$match_case")"; fi
      if [[ -n "$match_whole" ]]; then query+="&match_whole=$(_bolls_urlencode "$match_whole")"; fi
      if [[ -n "$book" ]]; then query+="&book=$(_bolls_urlencode "$book")"; fi
      if [[ -n "$page" ]]; then query+="&page=$(_bolls_urlencode "$page")"; fi
      if [[ -n "$limit" ]]; then query+="&limit=$(_bolls_urlencode "$limit")"; fi
      _bolls_get "$base/v2/find/${translation}?${query}" ;;
    --parallel|-p)
      # accepts full JSON object/file OR simple args
      if [[ -z "$1" ]]; then echo "Usage: bolls --parallel <translations> <book> <chapter> <verses> OR bolls parallel <JSON array or file>" >&2; return 2; fi
      if [[ -z "$2" && -z "$3" && -z "$4" ]]; then
        local body
        body="$(_bolls_normalize_books_in_json "$1" parallel)" || return $?
        _bolls_post "$base/get-parallel-verses/" "$body"
        return $?
      fi
      # translations: JSON array, comma list, or file; verses: JSON array, comma list, or file
      if [[ -z "$2" || -z "$3" || -z "$4" ]]; then echo "Usage: bolls --parallel <translations> <book> <chapter> <verses> OR bolls parallel <JSON array or file>" >&2; return 2; fi
      local translations_json="$1"
      local book="$2"
      local chapter="$3"
      local verses_json="$4"
      local first_translation
      local book_id
      # normalize translations
      if [[ -f "$translations_json" ]]; then
        translations_json="$(cat "$translations_json")"
      else
        translations_json="$(_bolls_json_array "$translations_json" string)" || return $?
      fi
      translations_json="$(_bolls_uppercase_translations "$translations_json")" || return $?
      # normalize verses
      if [[ -f "$verses_json" ]]; then
        verses_json="$(cat "$verses_json")"
      else
        verses_json="$(_bolls_json_array "$verses_json" int)" || return $?
      fi
      first_translation="$(_bolls_first_translation "$translations_json")" || return $?
      book_id="$(_bolls_book_to_id "$first_translation" "$book")" || return $?
      local body
      body="$(printf '{\"translations\":%s,\"verses\":%s,\"book\":%s,\"chapter\":%s}' "$translations_json" "$verses_json" "$book_id" "$chapter")"
      _bolls_validate_json "$body" || return $?
      _bolls_post "$base/get-parallel-verses/" "$body" ;;
    --random|-r)
      if [[ -z "$1" ]]; then echo "Usage: bolls --random <translation>" >&2; return 2; fi
      local translation
      translation="$(_bolls_norm_translation "$1")"
      _bolls_get "$base/get-random-verse/${translation}/" ;;
    --define|-f)
      if [[ -z "$1" || -z "$2" ]]; then echo "Usage: bolls --define <dictionary> <Hebrew/Greek word>" >&2; return 2; fi
      # URL-encode query
      local dict="$1"; shift
      local query="$*"
      query="$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote('''$query'''))")"
      _bolls_get "$base/dictionary-definition/${dict}/${query}/" ;;
    -*)
      echo "Unknown flag: $cmd" >&2; return 2 ;;
    *)
      echo "Unknown subcommand: $cmd" >&2; return 2 ;;
  esac
}