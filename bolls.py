#!/usr/bin/env python3

# bolls.py - Python client for bolls.life API

import json
import os
import re
import sys
import tempfile
from io import BytesIO

try:
    import pycurl
except Exception as exc:  # pragma: no cover
    print(f"Error: pycurl is required: {exc}", file=sys.stderr)
    sys.exit(2)

try:
    import jq as jqmod
except Exception:
    jqmod = None

BASE_URL = "https://bolls.life"

JQ_PRETTY_FILTER = r"""

def indent($n): " " * ($n * 4);

def strip_html:
  if type == "string" then
    gsub("<(br|p) */?>"; "\n") | gsub("<[^>]*>"; "")
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

""".strip()

JQ_TEXT_COMMENT = r"""

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

""".strip()

JQ_TEXT_ONLY = r"""

def keep_text_only:
  if type == "array" then map(keep_text_only)
  elif type == "object" then
    .text
  else .
  end;

keep_text_only

""".strip()


def _print_help() -> None:
    print(
        """
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

""".strip()
    )


def _curl_get(url: str) -> str:
    buf = BytesIO()
    curl = pycurl.Curl()
    try:
        curl.setopt(pycurl.URL, url)
        curl.setopt(pycurl.WRITEDATA, buf)
        curl.setopt(pycurl.FAILONERROR, True)
        curl.setopt(pycurl.NOSIGNAL, True)
        curl.perform()
    except pycurl.error as exc:
        errno, msg = exc.args
        print(f"Error: HTTP request failed ({errno}): {msg}", file=sys.stderr)
        raise
    finally:
        curl.close()
    return buf.getvalue().decode("utf-8", errors="replace")


def _curl_post(url: str, body: str) -> str:
    buf = BytesIO()
    curl = pycurl.Curl()
    try:
        curl.setopt(pycurl.URL, url)
        curl.setopt(pycurl.WRITEDATA, buf)
        curl.setopt(pycurl.FAILONERROR, True)
        curl.setopt(pycurl.NOSIGNAL, True)
        curl.setopt(pycurl.HTTPHEADER, ["Content-Type: application/json"])
        curl.setopt(pycurl.POSTFIELDS, body.encode("utf-8"))
        curl.perform()
    except pycurl.error as exc:
        errno, msg = exc.args
        print(f"Error: HTTP request failed ({errno}): {msg}", file=sys.stderr)
        raise
    finally:
        curl.close()
    return buf.getvalue().decode("utf-8", errors="replace")


def _jq_pretty(raw: str, jq_prefix: str | None) -> str:
    program = JQ_PRETTY_FILTER
    if jq_prefix:
        program = f"{jq_prefix}\n| {JQ_PRETTY_FILTER}"
    compiled = jqmod.compile(program)
    out = compiled.input_text(raw).first()
    if out is None:
        return ""
    if isinstance(out, (dict, list)):
        return json.dumps(out, indent=2, ensure_ascii=False)
    return str(out)


def _print_json(raw: str, raw_json: bool, jq_prefix: str | None = None) -> None:
    if raw_json:
        sys.stdout.write(raw)
        return
    if jqmod is not None:
        try:
            rendered = _jq_pretty(raw, jq_prefix)
            if rendered and not rendered.endswith("\n"):
                rendered += "\n"
            sys.stdout.write(rendered)
            return
        except Exception:
            pass
    try:
        data = json.loads(raw)
    except Exception:
        sys.stdout.write(raw)
        return
    print(json.dumps(data, indent=2, ensure_ascii=False))


def _norm_translation(s: str) -> str:
    return s.upper()

def _urlencode(s: str) -> str:
    from urllib.parse import quote

    return quote(s)



def _choose_jq_prefix(include_all: bool, no_comment: bool) -> str | None:
    if include_all:
        return None
    if no_comment:
        return JQ_TEXT_ONLY
    return JQ_TEXT_COMMENT


def _json_array(raw: str, kind: str) -> str:
    s = raw.strip()
    if s.startswith("["):
        try:
            json.loads(s)
            return s
        except Exception:
            pass
    parts = []
    for chunk in s.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        for piece in chunk.split():
            if piece:
                parts.append(piece)
    if kind == "int":
        vals = []
        for part in parts:
            try:
                vals.append(int(part))
            except Exception:
                raise ValueError(f"Invalid number in list: {part}")
        return json.dumps(vals)
    return json.dumps(parts)


def _ensure_books_cache() -> str:
    cache = os.path.join(tempfile.gettempdir(), "bolls_translations_books.json")
    if not os.path.isfile(cache) or os.path.getsize(cache) == 0:
        raw = _curl_get(f"{BASE_URL}/static/bolls/app/views/translations_books.json")
        with open(cache, "w", encoding="utf-8") as f:
            f.write(raw)
    return cache


def _load_books_data() -> dict:
    cache = _ensure_books_cache()
    with open(cache, "r", encoding="utf-8") as f:
        return json.load(f)


def _book_to_id(translation: str, book: object) -> object:
    if isinstance(book, int):
        return book
    if isinstance(book, str) and book.isdigit():
        return int(book)
    if not isinstance(book, str):
        return book
    data = _load_books_data()
    keys = {k.lower(): k for k in data.keys()}
    tkey = translation.lower()
    if tkey not in keys:
        raise ValueError(
            f"unknown translation '{translation}' for book lookup. \n"
            "Try bolls -t to see all available translations, and be sure to use the abbreviation."
        )
    t = keys[tkey]

    def norm(s: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", s.lower())

    target = norm(book)
    candidates = []
    for entry in data[t]:
        name = entry.get("name", "")
        n = norm(name)
        if n == target:
            return entry.get("bookid")
        if n.startswith(target):
            candidates.append(entry)
    if len(candidates) == 1:
        return candidates[0].get("bookid")
    if len(candidates) > 1:
        raise ValueError(f"book name '{book}' is ambiguous for translation '{t}'.")
    raise ValueError(
        f"unknown book '{book}' for translation '{t}'. \n"
        f"Try bolls -b '{t}' to find what book you're looking for."
    )


def _normalize_get_verses_json(arg: str) -> str:
    if os.path.isfile(arg):
        with open(arg, "r", encoding="utf-8") as f:
            obj = json.load(f)
    else:
        obj = json.loads(arg)
    if not isinstance(obj, list):
        raise ValueError("get-verses JSON must be an array")
    for entry in obj:
        if not isinstance(entry, dict):
            raise ValueError("get-verses items must be objects")
        if "translation" not in entry or "book" not in entry:
            raise ValueError("get-verses items must include translation and book")
        if isinstance(entry.get("translation"), str):
            entry["translation"] = entry["translation"].upper()
        entry["book"] = _book_to_id(entry["translation"], entry["book"])
    return json.dumps(obj)


def _uppercase_translations(translations_json: str) -> str:
    try:
        data = json.loads(translations_json)
    except Exception as exc:
        raise ValueError(f"invalid translations JSON: {exc}")
    if not isinstance(data, list):
        raise ValueError("translations must be a JSON array")
    out = [(v.upper() if isinstance(v, str) else v) for v in data]
    return json.dumps(out)


def _first_translation(translations_json: str) -> str:
    try:
        data = json.loads(translations_json)
    except Exception as exc:
        raise ValueError(f"invalid translations JSON: {exc}")
    if not isinstance(data, list) or not data:
        raise ValueError("translations list is empty!")
    return data[0]


def _normalize_parallel_json(arg: str) -> str:
    if os.path.isfile(arg):
        with open(arg, "r", encoding="utf-8") as f:
            obj = json.load(f)
    else:
        obj = json.loads(arg)
    if not isinstance(obj, dict):
        raise ValueError("parallel JSON must be an object")
    translations = obj.get("translations")
    if not translations or not isinstance(translations, list):
        raise ValueError("parallel JSON must include translations array")
    translations = [t.upper() if isinstance(t, str) else t for t in translations]
    obj["translations"] = translations
    if "book" in obj:
        obj["book"] = _book_to_id(translations[0], obj["book"])
    return json.dumps(obj)


def _validate_json(body: str) -> None:
    try:
        json.loads(body)
    except Exception as exc:
        raise ValueError(f"Invalid JSON: {exc}")


def _read_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def main(argv: list[str]) -> int:
    raw_json = False
    include_all = False
    no_comment = False

    args = []
    for a in argv:
        if a in ("-j", "--raw-json"):
            raw_json = True
        elif a in ("-i", "--include-all"):
            include_all = True
        elif a in ("-n", "--no-comments"):
            no_comment = True
        else:
            args.append(a)

    cmd = args[0] if args else "-h"
    rest = args[1:]

    try:
        if cmd in ("-h", "--help"):
            _print_help()
            return 0
        if cmd in ("-t", "--translations"):
            raw = _curl_get(f"{BASE_URL}/static/bolls/app/views/languages.json")
            _print_json(raw, raw_json)
            return 0
        if cmd in ("-d", "--dictionaries"):
            raw = _curl_get(f"{BASE_URL}/static/bolls/app/views/dictionaries.json")
            _print_json(raw, raw_json)
            return 0
        if cmd in ("-b", "--books"):
            if not rest:
                print("Usage: bolls --books <translation>", file=sys.stderr)
                return 2
            translation = _norm_translation(rest[0])
            raw = _curl_get(f"{BASE_URL}/get-books/{translation}/")
            _print_json(raw, raw_json)
            return 0
        if cmd in ("-c", "--chapter"):
            if len(rest) < 3:
                print("Usage: bolls --chapter <translation> <book> <chapter>", file=sys.stderr)
                return 2
            translation = _norm_translation(rest[0])
            book = rest[1]
            chapter = rest[2]
            book_id = _book_to_id(translation, book)
            jq_prefix = _choose_jq_prefix(include_all, no_comment)
            raw = _curl_get(f"{BASE_URL}/get-chapter/{translation}/{book_id}/{chapter}/")
            _print_json(raw, raw_json, jq_prefix)
            return 0
        if cmd in ("-v", "--verse"):
            if not rest:
                print(
                    "Usage: bolls --verse <translation> <book> <chapter> <verses> "
                    "OR bolls --verse <JSON array or file>",
                    file=sys.stderr,
                )
                return 2
            jq_prefix = _choose_jq_prefix(include_all, no_comment)
            if len(rest) == 1:
                body = _normalize_get_verses_json(rest[0])
                raw = _curl_post(f"{BASE_URL}/get-verses/", body)
                _print_json(raw, raw_json, jq_prefix)
                return 0
            if len(rest) < 4:
                print(
                    "Usage: bolls --verse <translation> <book> <chapter> <verses> "
                    "OR bolls --verse <JSON array or file>",
                    file=sys.stderr,
                )
                return 2
            translation = _norm_translation(rest[0])
            book = rest[1]
            chapter = rest[2]
            verses_arg = rest[3]
            book_id = _book_to_id(translation, book)
            if os.path.isfile(verses_arg):
                verses_json = _read_file(verses_arg)
            else:
                verses_json = _json_array(verses_arg, "int")
            try:
                chapter_val = int(chapter)
            except ValueError:
                raise ValueError(f"Invalid chapter: {chapter}")
            try:
                verses_list = json.loads(verses_json)
            except Exception as exc:
                raise ValueError(f"Invalid JSON: {exc}")
            body_obj = [
                {
                    "translation": translation,
                    "book": book_id,
                    "chapter": chapter_val,
                    "verses": verses_list,
                }
            ]
            body = json.dumps(body_obj)
            raw = _curl_post(f"{BASE_URL}/get-verses/", body)
            _print_json(raw, raw_json, jq_prefix)
            return 0
        if cmd in ("-p", "--parallel"):
            if not rest:
                print(
                    "Usage: bolls --parallel <translations> <book> <chapter> <verses> "
                    "OR bolls --parallel <JSON object or file>",
                    file=sys.stderr,
                )
                return 2
            if len(rest) == 1:
                body = _normalize_parallel_json(rest[0])
                raw = _curl_post(f"{BASE_URL}/get-parallel-verses/", body)
                _print_json(raw, raw_json)
                return 0
            if len(rest) < 4:
                print(
                    "Usage: bolls --parallel <translations> <book> <chapter> <verses> "
                    "OR bolls --parallel <JSON object or file>",
                    file=sys.stderr,
                )
                return 2
            translations_arg = rest[0]
            book = rest[1]
            chapter = rest[2]
            verses_arg = rest[3]
            if os.path.isfile(translations_arg):
                translations_json = _read_file(translations_arg)
            else:
                translations_json = _json_array(translations_arg, "string")
            translations_json = _uppercase_translations(translations_json)
            if os.path.isfile(verses_arg):
                verses_json = _read_file(verses_arg)
            else:
                verses_json = _json_array(verses_arg, "int")
            try:
                chapter_val = int(chapter)
            except ValueError:
                raise ValueError(f"Invalid chapter: {chapter}")
            try:
                translations_list = json.loads(translations_json)
                verses_list = json.loads(verses_json)
            except Exception as exc:
                raise ValueError(f"Invalid JSON: {exc}")
            first_translation = _first_translation(translations_json)
            book_id = _book_to_id(first_translation, book)
            body_obj = {
                "translations": translations_list,
                "verses": verses_list,
                "book": book_id,
                "chapter": chapter_val,
            }
            body = json.dumps(body_obj)
            raw = _curl_post(f"{BASE_URL}/get-parallel-verses/", body)
            _print_json(raw, raw_json)
            return 0

        if cmd in ("-s", "--search"):
            if len(rest) < 2:
                print(
                    "Usage: bolls --search <translation> <search term> "
                    "[--match_case <true/false>] [--match_whole <true/false>] "
                    "[--book <book/ot/nt>] [--page <int>] [--limit <int>]",
                    file=sys.stderr,
                )
                return 2
            translation = _norm_translation(rest[0])
            piece = rest[1]
            opts = rest[2:]
            match_case = None
            match_whole = None
            book = None
            page = None
            limit = None
            i = 0
            while i < len(opts):
                opt = opts[i]
                if opt in ("--match_case", "--match-case"):
                    if i + 1 >= len(opts):
                        raise ValueError("Missing value for --match-case")
                    match_case = opts[i + 1]
                    i += 2
                elif opt in ("--match_whole", "--match-whole", "--match-whole-word"):
                    if i + 1 >= len(opts):
                        raise ValueError("Missing value for --match-whole-word")
                    match_whole = opts[i + 1]
                    i += 2
                elif opt == "--book":
                    if i + 1 >= len(opts):
                        raise ValueError("Missing value for --book")
                    book = opts[i + 1]
                    i += 2
                elif opt == "--page":
                    if i + 1 >= len(opts):
                        raise ValueError("Missing value for --page")
                    page = opts[i + 1]
                    i += 2
                elif opt in ("--limit", "--page-limit"):
                    if i + 1 >= len(opts):
                        raise ValueError("Missing value for --limit")
                    limit = opts[i + 1]
                    i += 2
                else:
                    raise ValueError(f"Unknown search option: {opt}")
            if book:
                if book.lower() in ("ot", "nt"):
                    book = book.lower()
                elif book.isdigit():
                    pass
                else:
                    book = str(_book_to_id(translation, book))
            query = f"search={_urlencode(piece)}"
            if match_case is not None:
                query += f"&match_case={_urlencode(match_case)}"
            if match_whole is not None:
                query += f"&match_whole={_urlencode(match_whole)}"
            if book is not None:
                query += f"&book={_urlencode(book)}"
            if page is not None:
                query += f"&page={_urlencode(page)}"
            if limit is not None:
                query += f"&limit={_urlencode(limit)}"
            raw = _curl_get(f"{BASE_URL}/v2/find/{translation}?{query}")
            _print_json(raw, raw_json)
            return 0

        if cmd in ("-f", "--define"):
            if len(rest) < 2:
                print("Usage: bolls --define <dictionary> <Hebrew/Greek word>", file=sys.stderr)
                return 2
            dict_code = rest[0]
            query = " ".join(rest[1:]).strip()
            if not query:
                print("Usage: bolls --define <dictionary> <Hebrew/Greek word>", file=sys.stderr)
                return 2
            query_enc = _urlencode(query)
            raw = _curl_get(f"{BASE_URL}/dictionary-definition/{dict_code}/{query_enc}/")
            _print_json(raw, raw_json)
            return 0

        if cmd in ("-r", "--random"):
            if not rest:
                print("Usage: bolls --random <translation>", file=sys.stderr)
                return 2
            translation = _norm_translation(rest[0])
            raw = _curl_get(f"{BASE_URL}/get-random-verse/{translation}/")
            _print_json(raw, raw_json)
            return 0

        if cmd.startswith("-"):
            print(f"Unknown flag: {cmd}", file=sys.stderr)
            return 2
        print(f"Unknown subcommand: {cmd}", file=sys.stderr)
        return 2
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2
    except pycurl.error:
        return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
