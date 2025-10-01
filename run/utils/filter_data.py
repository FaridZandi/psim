import pandas as pd 


def _normalize_filter_expression(raw_expression: str) -> str:
    """Convert a CLI-friendly boolean expression into a pandas.query expression."""
    expression = raw_expression.strip()
    if not expression:
        return expression

    normalized_chunks = []
    i = 0
    in_quote = None
    length = len(expression)

    while i < length:
        char = expression[i]

        if in_quote:
            normalized_chunks.append(char)
            if char == in_quote and (i == 0 or expression[i - 1] != "\\"):
                in_quote = None
            i += 1
            continue

        if char in {'"', "'"}:
            in_quote = char
            normalized_chunks.append(char)
            i += 1
            continue

        if char == '&' and i + 1 < length and expression[i + 1] == '&':
            normalized_chunks.append(' and ')
            i += 2
            continue

        if char == '|' and i + 1 < length and expression[i + 1] == '|':
            normalized_chunks.append(' or ')
            i += 2
            continue

        if char == '<' and i + 1 < length and expression[i + 1] == '>':
            normalized_chunks.append(' != ')
            i += 2
            continue

        if char == '=':
            next_char = expression[i + 1] if i + 1 < length else ''
            prev_char = expression[i - 1] if i > 0 else ''

            if next_char == '=':
                normalized_chunks.append('==')
                i += 2
                continue

            if prev_char in {'!', '<', '>'}:
                normalized_chunks.append('=')
            else:
                normalized_chunks.append('==')
            i += 1
            continue

        if char.isalpha() or char == '_':
            start = i
            while i < length and (expression[i].isalnum() or expression[i] == '_'):
                i += 1
            word = expression[start:i]
            lower_word = word.lower()

            if lower_word == 'and':
                normalized_chunks.append(' and ')
            elif lower_word == 'or':
                normalized_chunks.append(' or ')
            elif lower_word == 'not':
                normalized_chunks.append(' not ')
            elif lower_word == 'in':
                normalized_chunks.append(' in ')
            elif lower_word == 'true' and word in {'true', 'TRUE'}:
                normalized_chunks.append(' True ')
            elif lower_word == 'false' and word in {'false', 'FALSE'}:
                normalized_chunks.append(' False ')
            else:
                normalized_chunks.append(word)
            continue

        normalized_chunks.append(char)
        i += 1

    return ''.join(normalized_chunks).strip()

def filter_df(df, filter):
    if filter is not None:
        # filter would be like colummn_name=value
        if "=" in filter:
            col_name, value = filter.split("=")
            df = df[
                (df[col_name] == value) |
                (df[col_name] == int(value)) |
                (df[col_name] == float(value))
            ]
        elif ">" in filter:
            col_name, value = filter.split(">")
            df = df[
                (df[col_name] > float(value))
            ]
        elif "<" in filter:
            col_name, value = filter.split("<")
            df = df[
                (df[col_name] < float(value))
            ]
    return df


def filter_df_2(df, filter_expression):
    """Vectorised dataframe filter with support for (), and/or/not, and membership tests."""
    if filter_expression is None:
        return df

    normalized_expression = _normalize_filter_expression(str(filter_expression))
    if not normalized_expression:
        return df

    try:
        return df.query(normalized_expression, engine="python")
    except Exception as exc:
        raise ValueError(
            f"Invalid filter expression '{filter_expression}' (normalized to '{normalized_expression}')"
        ) from exc
