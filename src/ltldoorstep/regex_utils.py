import re

_regexes = {k: [v, None] for k, v in {
        'uk-postcode': r'([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z][0-9]{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y][0-9]{1,2})|(([A-Za-z][0-9][A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y][0-9]?[A-Za-z])))) ?[0-9][A-Za-z]{2})',
        'search-basic-date-time': r'(\d\d\d\d\\\d\d\\\d\d|\d\d\d\d-\d\d-\d\d|\d\d:\d\d:\d\d)',
        'mac-address': r'((\d|([a-f]|[A-F])){2}:){5}(\d|([a-f]|[A-F])){2}',
        'email-address': r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,63}',
        'ip-address': r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b',
        'common-street-indicators': r'\b(road|street|avenue|rd|st|ave|bvd|mews|lane|square|house|gardens|sq)\b'
    }.items()
}

def get_regex(code):
    global _regexes

    regex, compiled = _regexes[code]
    if not compiled:
        compiled = re.compile(regex)
        _regexes[code][1] = compiled

    return compiled
