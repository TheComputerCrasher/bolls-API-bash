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
PARALLEL_CHAPTER_MAX_VERSE = 300
_MAX_VERSE_CACHE: dict[tuple[str, int, int], int] = {}

JQ_PRETTY_FILTER = r"""

def indent($n): " " * ($n * 4);

def strip_html:
  if type == "string" then
    gsub("(?is)<s[^>]*>.*?</s>"; "")
    | gsub("<(br|p) */?>"; "\n")
    | gsub("<[^>]*>"; "")
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
  if type == "array" then map(keep_text_comment) | map(select(. != null and . != ""))
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
  if type == "array" then map(keep_text_only) | map(select(. != null and . != ""))
  elif type == "object" then
    .text
  else .
  end;

keep_text_only

""".strip()


def _print_help() -> None:
    print("""
Command flags (choose one):
  -h / --help
  Show this help page

  -d / --dictionaries
  List all available Hebrew/Greek dictionaries

  -D / --define <dictionary> <Hebrew/Greek word>
  Get definitions for a Hebrew or Greek word

  -t / --translations
  List all available Bible translations

  -b / --books <translation>
  List all books of a chosen translation

  -v / --verses <translation(s)> <book> <chapter>[:<verse(s)>]
  Get one or multiple verses (omit verses for full chapter).
  Use slashes to get verses from multiple places at once.

  -r / --random <translation>
  Get a single random verse

  -s / --search <translation> [options] <search term>
  Search text in verses

  Search options (choose any amount or none when using -s):
    -m / --match-case
    Make search case-sensitive

    -w / --match-whole
    Only search exact phrase matches (requires multiple words)

    -B / --book <book/ot/nt>
    Search in a specific book, or in just the Old or New Testament

    -p / --page <#>
    Go to a specific page of the search results
    
    -l / --page-limit <#>
    Limits the number of pages of search results


Notes:
  <translation> must be the abbreviation (case-insensitive), not the full name. Multiple translations are separated by commas.
  <book> can be a number or a name (case-insensitive).
  <verse(s)> can be a single number, multiple numbers separated by commas (e.g. 1,5,9), or a range (e.g. 13-17).


Modifier flags (choose one or none):
  -j / --raw-json
  Disable formatting

  -a / --include-all
  Include everything (verse id, translation, book number, etc.) in -v

  -c / --include-comments
  Include commentary (currently not working)

Examples:
  bolls --translations
  bolls -d
  bolls --books AMP
  bolls -r msg -j
  bolls --verses esv Genesis 1
  bolls -v esv 1 1 -j
  bolls --verses nlt,nkjv genesis 1
  bolls -v NIV Luke 2:15-17
  bolls --verses niv,nkjv genesis 1:1-3 -c
  bolls -v nlt genesis 1:1-3 / esv luke 2 / ylt,nkjv deuteronomy 6:5
  bolls --verses niv genesis 1
  bolls -s ylt -m -w -l 3 -p 1 Jesus wept
  bolls --search YLT --match-case --match-whole --page-limit 3 --page 1 Jesus wept
  bolls -D BDBT אֹ֑ור

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

def _drop_translation_only_entries(value: object) -> object:
    if isinstance(value, list):
        out = []
        for item in value:
            cleaned = _drop_translation_only_entries(item)
            if cleaned is None:
                continue
            out.append(cleaned)
        return out
    if isinstance(value, dict):
        # Drop objects that only contain a translation and no text/meaningful fields
        if "translation" in value and "text" not in value:
            if all(k == "translation" for k in value.keys()):
                return None
        if "translation" in value and (value.get("text") is None or value.get("text") == ""):
            has_meaningful = False
            for k, v in value.items():
                if k in ("translation", "text"):
                    continue
                if v not in (None, "", [], {}):
                    has_meaningful = True
                    break
            if not has_meaningful:
                return None
        cleaned = {}
        for k, v in value.items():
            cleaned_v = _drop_translation_only_entries(v)
            if cleaned_v is None:
                continue
            cleaned[k] = cleaned_v
        return cleaned
    return value






def _strip_s_tags(text: str) -> str:
    return re.sub(r"<s[^>]*>.*?</s>", "", text, flags=re.IGNORECASE | re.DOTALL)


def _strip_s_tags_in_data(value: object) -> object:
    if isinstance(value, str):
        return _strip_s_tags(value)
    if isinstance(value, list):
        return [_strip_s_tags_in_data(v) for v in value]
    if isinstance(value, dict):
        return {k: _strip_s_tags_in_data(v) for k, v in value.items()}
    return value

def _print_json(
    raw: str,
    raw_json: bool,
    jq_prefix: str | None = None,
    drop_translation_only: bool = False,
) -> None:
    if drop_translation_only:
        try:
            data = json.loads(raw)
        except Exception:
            data = None
        if data is not None:
            data = _drop_translation_only_entries(data)
            raw = json.dumps(data, ensure_ascii=False)
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
    data = _strip_s_tags_in_data(data)
    print(json.dumps(data, indent=2, ensure_ascii=False))





def _split_slash_groups(args: list[str]) -> list[list[str]]:
    groups = []
    current = []
    for token in args:
        if token == "/":
            if current:
                groups.append(current)
                current = []
            continue
        if "/" not in token or token.startswith("/") or token.startswith("./") or token.startswith("../"):
            current.append(token)
            continue
        parts = token.split("/")
        for i, part in enumerate(parts):
            part = part.strip()
            if part:
                current.append(part)
            if i < len(parts) - 1:
                if current:
                    groups.append(current)
                    current = []
    if current:
        groups.append(current)
    return groups



def _run_verses(rest: list[str], include_all: bool, add_comments: bool, raw_json: bool) -> int:
    if not rest:
        print(
            "Usage: bolls --verses <translation(s)> <book> <chapter>[:<verse(s)>]",
            file=sys.stderr,
        )
        return 2
    jq_prefix = _choose_jq_prefix(include_all, add_comments)
    if len(rest) == 1:
        body = _normalize_get_verses_json(rest[0])
        raw = _curl_post(f"{BASE_URL}/get-verses/", body)
        _print_json(raw, raw_json, jq_prefix, drop_translation_only=(include_all or raw_json))
        return 0
    translations_list = _parse_translations_arg(rest[0])
    ref_args = rest[1:]
    if not ref_args:
        print(
            "Usage: bolls --verses <translation(s)> <book> <chapter>[:<verse(s)>]",
            file=sys.stderr,
        )
        return 2
    mode, book, chapter_val, verses_list = _parse_v_reference(ref_args)
    body_obj_list = []
    for translation in translations_list:
        book_id = _book_to_id(translation, book)
        if mode == "chapter":
            max_verse = _max_verse_for_chapter(translation, book_id, chapter_val)
            verses = list(range(1, max_verse + 1))
        else:
            verses = verses_list
        body_obj_list.append(
            {
                "translation": translation,
                "book": book_id,
                "chapter": chapter_val,
                "verses": verses,
            }
        )
    body = json.dumps(body_obj_list)
    raw = _curl_post(f"{BASE_URL}/get-verses/", body)
    _print_json(raw, raw_json, jq_prefix, drop_translation_only=(include_all or raw_json))
    return 0

def _norm_translation(s: str) -> str:
    return s.upper()

def _urlencode(s: str) -> str:
    from urllib.parse import quote

    return quote(s)



def _choose_jq_prefix(include_all: bool, add_comments: bool) -> str | None:
    if include_all:
        return None
    if add_comments == False:
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



def _parse_verses_spec(spec: str) -> list[int]:
    if not isinstance(spec, str):
        raise ValueError("Invalid verses list")
    spec = spec.strip()
    if not spec:
        raise ValueError("Invalid verses list")
    if spec.lstrip().startswith("["):
        try:
            data = json.loads(spec)
        except Exception as exc:
            raise ValueError(f"Invalid verses JSON: {exc}")
        if not isinstance(data, list):
            raise ValueError("Invalid verses JSON")
        out = []
        for item in data:
            if isinstance(item, int):
                out.append(item)
            elif isinstance(item, str) and item.isdigit():
                out.append(int(item))
            else:
                raise ValueError("Invalid verses JSON")
        return out
    parts = re.split(r"[,\s]+", spec)
    out = []
    for part in parts:
        if not part:
            continue
        m = re.fullmatch(r"(\d+)\s*[-–—]\s*(\d+)", part)
        if m:
            start = int(m.group(1))
            end = int(m.group(2))
            step = 1 if end >= start else -1
            out.extend(range(start, end + step, step))
            continue
        if not part.isdigit():
            raise ValueError(f"Invalid verse number: {part}")
        out.append(int(part))
    if not out:
        raise ValueError("Invalid verses list")
    return out


def _parse_book_chapter_verses(args: list[str]) -> tuple[str, int, list[int]]:
    if not args:
        raise ValueError("Usage: bolls --verses <translation(s)> <book> <chapter>[:<verse(s)>]")
    joined = " ".join(args).strip()
    match = re.match(r"^(?P<book>.+?)\s+(?P<chapter>\d+)\s*:\s*(?P<verses>.+)$", joined)
    if match:
        book = match.group("book").strip()
        chapter_val = int(match.group("chapter"))
        verses_list = _parse_verses_spec(match.group("verses"))
        return book, chapter_val, verses_list
    if len(args) < 3:
        raise ValueError("Usage: bolls --verses <translation(s)> <book> <chapter>[:<verse(s)>]")
    verses_arg = args[-1]
    chapter = args[-2]
    book = " ".join(args[:-2]).strip()
    if not book:
        raise ValueError("Missing book name")
    try:
        chapter_val = int(chapter)
    except ValueError:
        raise ValueError(f"Invalid chapter: {chapter}")
    if os.path.isfile(verses_arg):
        verses_json = _read_file(verses_arg)
        try:
            verses_list = json.loads(verses_json)
        except Exception as exc:
            raise ValueError(f"Invalid JSON: {exc}")
    else:
        verses_list = _parse_verses_spec(verses_arg)
    return book, chapter_val, verses_list

def _parse_book_chapter(args: list[str]) -> tuple[str, int]:
    if not args:
        raise ValueError("Usage: bolls --verses <translation(s)> <book> <chapter>[:<verse(s)>]")
    joined = " ".join(args).strip()
    match = re.match(r"^(?P<book>.+?)\s+(?P<chapter>\d+)$", joined)
    if match:
        book = match.group("book").strip()
        chapter_val = int(match.group("chapter"))
        return book, chapter_val
    if len(args) < 2:
        raise ValueError("Usage: bolls --verses <translation(s)> <book> <chapter>[:<verse(s)>]")
    chapter = args[-1]
    book = " ".join(args[:-1]).strip()
    if not book:
        raise ValueError("Missing book name")
    try:
        chapter_val = int(chapter)
    except ValueError:
        raise ValueError(f"Invalid chapter: {chapter}")
    return book, chapter_val


def _parse_translations_arg(arg: str) -> list[str]:
    if os.path.isfile(arg):
        translations_json = _read_file(arg)
    else:
        translations_json = _json_array(arg, "string")
    translations_json = _uppercase_translations(translations_json)
    try:
        translations_list = json.loads(translations_json)
    except Exception as exc:
        raise ValueError(f"Invalid JSON: {exc}")
    if not isinstance(translations_list, list) or not translations_list:
        raise ValueError("translations list is empty!")
    return translations_list


def _parse_v_reference(args: list[str]) -> tuple[str, str, int, list[int] | None]:
    if not args:
        raise ValueError("Usage: bolls --verses <translation(s)> <book> <chapter>[:<verse(s)>]")
    if any(":" in a for a in args):
        book, chapter_val, verses_list = _parse_book_chapter_verses(args)
        return "verses", book, chapter_val, verses_list
    if len(args) == 2:
        book, chapter_val = _parse_book_chapter(args)
        return "chapter", book, chapter_val, None
    if len(args) < 2:
        raise ValueError("Usage: bolls --verses <translation(s)> <book> <chapter>[:<verse(s)>]")
    try:
        book, chapter_val, verses_list = _parse_book_chapter_verses(args)
        return "verses", book, chapter_val, verses_list
    except ValueError as exc:
        msg = str(exc)
        if msg.startswith("Invalid chapter") or msg.startswith("Usage: bolls --verses <translation(s)> <book> <chapter>[:<verse(s)>]"):
            book, chapter_val = _parse_book_chapter(args)
            return "chapter", book, chapter_val, None
        raise



def _max_verse_for_chapter(translation: str, book_id: int, chapter: int) -> int:
    cache_key = (translation.upper(), int(book_id), int(chapter))
    cached = _MAX_VERSE_CACHE.get(cache_key)
    if cached is not None:
        return cached
    limit = 32
    while True:
        verses = list(range(1, limit + 1))
        body = json.dumps(
            [
                {
                    "translation": translation,
                    "book": book_id,
                    "chapter": chapter,
                    "verses": verses,
                }
            ]
        )
        raw = _curl_post(f"{BASE_URL}/get-verses/", body)
        try:
            data = json.loads(raw)
        except Exception:
            _MAX_VERSE_CACHE[cache_key] = PARALLEL_CHAPTER_MAX_VERSE
            return PARALLEL_CHAPTER_MAX_VERSE
        verses_out = []
        if isinstance(data, list) and data:
            first = data[0]
            if isinstance(first, list):
                for item in first:
                    if isinstance(item, dict):
                        v = item.get("verse")
                        if isinstance(v, int):
                            verses_out.append(v)
        max_verse = max(verses_out) if verses_out else 0
        if max_verse < limit:
            _MAX_VERSE_CACHE[cache_key] = max_verse
            return max_verse
        if limit >= PARALLEL_CHAPTER_MAX_VERSE:
            _MAX_VERSE_CACHE[cache_key] = limit
            return limit
        limit = min(limit * 2, PARALLEL_CHAPTER_MAX_VERSE)





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
            "Try 'bolls -t' to see all available translations, and be sure to use the abbreviation."
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
        f"Try 'bolls -b {t}' to find the book you're looking for."
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
    add_comments = False

    args = []
    for a in argv:
        if a in ("-j", "--raw-json"):
            raw_json = True
        elif a in ("-a", "--include-all"):
            include_all = True
        elif a in ("-c", "--include-comments"):
            add_comments = True
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




        if cmd in ("-v", "--verses"):
            groups = _split_slash_groups(rest)
            if len(groups) <= 1:
                return _run_verses(rest, include_all, add_comments, raw_json)
            for group in groups:
                if not group:
                    continue
                rc = _run_verses(group, include_all, add_comments, raw_json)
                if rc != 0:
                    return rc
            return 0

        if cmd in ("-s", "--search"):

            if len(rest) < 2:
                print(
                    "Usage: bolls --search <translation> [--match-case] [--match-whole] "
                    "[--book <book/ot/nt>] [--page <#>] [--page-limit <#>] <search term>",
                    file=sys.stderr,
                )
                return 2
            translation = _norm_translation(rest[0])
            opts = rest[1:]
            match_case = None
            match_whole = None
            book = None
            page = None
            limit = None
            search_parts = []
            i = 0
            while i < len(opts):
                opt = opts[i]
                if opt == "--":
                    search_parts = opts[i + 1 :]
                    break
                if opt.startswith("-"):
                    if opt in ("--match_case", "--match-case", "-m"):
                        match_case = True
                        i += 1
                        continue
                    if opt in ("--match_whole", "--match-whole", "-w"):
                        match_whole = True
                        i += 1
                        continue
                    if opt in ("--book", "-B"):
                        if i + 1 >= len(opts):
                            raise ValueError("Usage: bolls --book <book/ot/nt>")
                        book = opts[i + 1]
                        i += 2
                        continue
                    if opt in ("--page", "-p"):
                        if i + 1 >= len(opts):
                            raise ValueError("Usage: bolls --page <#>")
                        page = opts[i + 1]
                        i += 2
                        continue
                    if opt in ("--limit", "--page-limit", "-l"):
                        if i + 1 >= len(opts):
                            raise ValueError("Usage: --page-limit <#>")
                        limit = opts[i + 1]
                        i += 2
                        continue
                    raise ValueError(f"Unknown search option: {opt}")
                search_parts = opts[i:]
                break
            if not search_parts:
                raise ValueError("Missing search term")
            piece = " ".join(search_parts).strip()
            if not piece:
                raise ValueError("Missing search term")
            if book:
                if book.lower() in ("ot", "nt"):
                    book = book.lower()
                elif book.isdigit():
                    pass
                else:
                    book = str(_book_to_id(translation, book))
            query = f"search={_urlencode(piece)}"
            if match_case is not None:
                query += f"&match_case={_urlencode('true')}"
            if match_whole is not None:
                query += f"&match_whole={_urlencode('true')}"
            if book is not None:
                query += f"&book={_urlencode(book)}"
            if page is not None:
                query += f"&page={_urlencode(page)}"
            if limit is not None:
                query += f"&limit={_urlencode(limit)}"
            raw = _curl_get(f"{BASE_URL}/v2/find/{translation}?{query}")
            _print_json(raw, raw_json)
            return 0

        if cmd in ("-D", "--define"):

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
