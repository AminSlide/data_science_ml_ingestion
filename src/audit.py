"""Indicateurs de forme des textes, communs à l'audit (étape 2) et aux étapes suivantes."""

import re

from pyspark.sql import Column
from pyspark.sql import functions as F

# Caractères qu'un LLM lit mais qu'un écran n'affiche pas, ou affiche autrement.
INVISIBLE = {
    "largeur_nulle": [(0x00AD, 0x00AD), (0x180E, 0x180E), (0x200B, 0x200D), (0x2060, 0x2064), (0xFEFF, 0xFEFF)],
    "bidi": [(0x200E, 0x200F), (0x202A, 0x202E), (0x2066, 0x2069)],
    "controle": [(0x00, 0x08), (0x0B, 0x0C), (0x0E, 0x1F), (0x7F, 0x9F)],
}
# « Tags » Unicode : une copie invisible de l'ASCII (U+E0041 = A, etc.).
TAGS = (0xE0000, 0xE007F)


def _java_class(ranges: list[tuple[int, int]]) -> str:
    return "[" + "".join(f"\\x{{{a:X}}}-\\x{{{b:X}}}" for a, b in ranges) + "]"


def _python_class(ranges: list[tuple[int, int]]) -> str:
    return "[" + "".join(f"\\U{a:08X}-\\U{b:08X}" for a, b in ranges) + "]"


# Indices de forme (regex Java, évaluées par Spark). Ce ne sont pas encore des détecteurs
# d'obfuscation validés : ce travail vient à l'étape 6.
TEXT_FLAGS = {
    **{name: _java_class(ranges) for name, ranges in INVISIBLE.items()},
    "tags_unicode": _java_class([TAGS]),
    "remplacement": r"\x{FFFD}",
    "html": r"(?i)<(html|body|div|span|p|br|table|!doctype)\b",
    "faux_roles": r"<\|(user|assistant|system|end|im_start|im_end)\|>",
    "suite_base64": r"[A-Za-z0-9+/]{40,}={0,2}",
    "cjk": r"[\x{4E00}-\x{9FFF}]",
    "cyrillique": r"[\x{0400}-\x{04FF}]",
    "arabe": r"[\x{0600}-\x{06FF}]",
    "non_ascii": r"[^\x{00}-\x{7F}]",
}


def text_flags(text: Column) -> list[Column]:
    """Une colonne booléenne par indice de TEXT_FLAGS."""
    return [text.rlike(pattern).alias(name) for name, pattern in TEXT_FLAGS.items()]


def near_dup_key(text: Column) -> Column:
    """Clé des quasi-doublons évidents : texte en minuscules, chiffres remplacés par 0, espaces
    écrasés. Le regroupement plus fin (MinHash) vient à l'étape 5."""
    return F.trim(F.regexp_replace(F.regexp_replace(F.lower(text), r"\d", "0"), r"\s+", " "))


_INVISIBLE_RUN = re.compile("(" + _python_class([r for ranges in INVISIBLE.values() for r in ranges]) + r")\1*")
_TAG_RUN = re.compile(_python_class([TAGS]) + "+")


def reveal_hidden(text: str) -> str:
    """Le texte tel qu'un LLM le lit, rendu lisible pour un humain : les tags Unicode sont décodés,
    les caractères invisibles ou de contrôle remplacés par leur code, par exemple ⟦U+200B ×3⟧."""

    def decode_tags(match: re.Match) -> str:
        decoded = "".join(chr(ord(c) - 0xE0000) if 0xE0020 <= ord(c) <= 0xE007E else "·" for c in match.group())
        return f"⟦tags : {decoded}⟧"

    def code_point(match: re.Match) -> str:
        run = len(match.group())
        return f"⟦U+{ord(match.group(1)):04X}{f' ×{run}' if run > 1 else ''}⟧"

    # Un retour chariot seul peut faire réécrire une ligne par-dessus une autre dans un terminal.
    text = text.replace("\r\n", "\n").replace("\r", "⟦U+000D⟧")
    return _INVISIBLE_RUN.sub(code_point, _TAG_RUN.sub(decode_tags, text))
