import re

# list via https://github.com/mikf/gallery-dl/blob/53f252cfb17deeae4792e7a6a2ee6a65d1258b2f/docs/gallery-dl-example.conf#L17-L27
_PATH_ILLEGAL_CHAR_REPLACEMENTS = {
    '\\': '⧹',
    '/': '⧸',
    '|': '￨',
    ':': '꞉',
    '*': '∗',
    '?': '？',
    '"': '″',
    '<': '﹤',
    '>': '﹥',
}
_PATH_ILLEGAL_CHARS_PATTERN = re.compile(
    '|'.join(map(re.escape, _PATH_ILLEGAL_CHAR_REPLACEMENTS.keys()))
)


def normalize_for_filename(s: str) -> str:
    return _PATH_ILLEGAL_CHARS_PATTERN.sub(lambda m: _PATH_ILLEGAL_CHAR_REPLACEMENTS[m.group(0)], s)
