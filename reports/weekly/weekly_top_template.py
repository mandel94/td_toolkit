import pandas as pd

MOJIBAKE_MARKERS = ("Ã", "â€™", "â€œ", "â€", "Â")


def _normalize_text(value):
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if not any(marker in text for marker in MOJIBAKE_MARKERS):
        return text
    try:
        return text.encode("latin-1").decode("utf-8").strip()
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text


def _italicize_title(title):
    normalized = _normalize_text(title)
    if not normalized:
        return "(titolo mancante)"
    safe_title = normalized.replace("*", r"\*")
    return f"*{safe_title}*"

def weekly_top_template_from_excel(excel_path, n=3, metric="Views"):
    """
    Legge un file Excel e restituisce una stringa formattata con i top articoli per categoria secondo il template richiesto.
    Il mapping tra nome sezione e nome tab è hardcodato.
    L'ordinamento dei top articoli avviene in base alla metrica specificata.
    """
    # Mapping: sezione output -> nome sheet Excel
    section_to_sheet = {
        "Si farà": "si_fara",
        "Recensioni": "Recensioni",
        "Serie TV": "Serie TV",
        "Approfondimento": "Approfondimento",
        "News": "News"
    }
    # Mapping: sezione output -> emoji
    section_emoji = {
        "Si farà": "🔎",
        "Recensioni": "🎬",
        "Serie TV": "📺",
        "Approfondimento": "📺",
        "News": "📰"
    }
    output = ["📊 Top Contenuti della Settimana 📊\n"]
    for idx, (section, sheet) in enumerate(section_to_sheet.items(), 1):
        emoji = section_emoji.get(section, "")
        try:
            df = pd.read_excel(excel_path, sheet_name=sheet)
        except Exception:
            continue
        # Ordina per la metrica specificata, prendi i primi n
        if metric in df.columns:
            top = df.sort_values(metric, ascending=False).head(n)
        else:
            top = df.head(n)
        output.append(f" {emoji} {idx}. {section} ")
        for _, row in top.iterrows():
            title = row.get("title") or row.get("Titolo") or row.get("Articolo") or "(titolo mancante)"
            author = _normalize_text(row.get("author") or row.get("Autore") or "Anonimo") or "Anonimo"
            output.append(f" • {_italicize_title(title)} ({author})")
    return "\n".join(output) + "\n"


def weekly_top_template_from_df(excel_path, n=3, metric="Views"):
    """
    Crea una stringa formattata con i top articoli per categoria secondo il template richiesto, partendo da un DataFrame.
    Il mapping tra nome sezione e nome tab è hardcodato.
    L'ordinamento dei top articoli avviene in base alla metrica specificata.
    """
    section_to_category = {
        "Si farà": "si_fara",
        "Recensioni": "Recensioni",
        "Serie TV": "Serie TV",
        "Approfondimento": "Approfondimento",
        "News": "News"
    }
    section_emoji = {
        "Si farà": "🔎",
        "Recensioni": "🎬",
        "Serie TV": "📺",
        "Approfondimento": "📺",
        "News": "📰"
    }
    df = pd.read_excel(excel_path)
    output = ["📊 Top Contenuti della Settimana 📊\n"]
    for idx, (section, category) in enumerate(section_to_category.items(), 1):
        emoji = section_emoji.get(section, "")
        # Filtro per categoria (o per colonna speciale per si_fara)
        if category == "si_fara":
            cat_df = df[df.get("Is_si_fara", False)]
        else:
            cat_df = df[df.get("Categoria", "") == category]
        if cat_df.empty:
            continue
        # Ordina per la metrica specificata, prendi i primi n
        if metric in cat_df.columns:
            top = cat_df.sort_values(metric, ascending=False).head(n)
        else:
            top = cat_df.head(n)
        output.append(f" {emoji} {idx}. {section} ")
        for _, row in top.iterrows():
            title = row.get("title") or row.get("Titolo") or row.get("Articolo") or "(titolo mancante)"
            author = _normalize_text(row.get("author") or row.get("Autore") or "Anonimo") or "Anonimo"
            output.append(f" • {_italicize_title(title)} ({author})")
    return "\n".join(output) + "\n"
