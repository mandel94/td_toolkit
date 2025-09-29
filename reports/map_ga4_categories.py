# Category mapping: Maps broader category names to a set of specific path keywords.
from re import sub


CATEGORIES = {
    "News": {"latest-news", "focus-italia"},
    # "Anticipazioni": {"anticipazioni"},
    "Recensioni": {"review", "netflix-film", "sky-film", "disney-film", "mubi", "mubi-film",
                    "approfondimenti", "streaming"},
    "In Sala": {"in-sala"},
    "Cult Movies": {"cult-movie"},
    "Animazione": {"animazione", "animazione/anime"},
    "Approfondimento": {"approfondimento"},
    "Festival di Cinema": {"festival-di-cinema"},
    "Trailers": {"trailers"},
    "Serie TV": {"serie-tv", "netflix-serie-tv", "prime-video-serietv", "sky-serie-tv",
                 "disney-serietv", "paramount-serie-tv", "appletv-serietv", "tim-vision-serie-tv"},
    "Guide e Film da Vedere": {"film-da-vedere"},
    "Speciali e Magazine": {"magazine-2", "taxidrivers-magazine"},
    "Live Streaming On Demand": {"live-streaming-on-demand"},
    "Rubriche": {"rubriche"},
    "Interviste": {"interviews"}
}

def map_ga4_categories(path: str) -> str:
    """
    Mappa un percorso URL a una categoria di contenuto secondo parole chiave predefinite.

    Args:
        path (str): Il percorso URL da categorizzare. Esempio: "recensioni/netflix-film/in-sala"

    Returns:
        str: Il nome della categoria mappata. Possibili valori includono:
            - "News"
            - "Recensioni"
            - "Recensioni / In Sala"
            - "In Sala"
            - "Cult Movies"
            - "Animazione"
            - "Approfondimento"
            - "Festival di Cinema"
            - "Trailers"
            - "Trailers / In Sala"
            - "Serie TV"
            - "Guide e Film da Vedere"
            - "Speciali e Magazine"
            - "Live Streaming On Demand"
            - "Rubriche"
            - "Interviste"
            - "Anticipazioni" (prioritaria se presente nel path)
            - "Altro" (default se nessuna corrispondenza)

    Note:
        - Se "anticipazioni" è presente nel path, viene restituita la categoria "Anticipazioni".
        - Se la categoria è "Recensioni" o "Trailers" e "in-sala" è nel path, restituisce rispettivamente "Recensioni / In Sala" o "Trailers / In Sala".
        - La funzione analizza il path suddividendolo in sottoparti e controllando l'intersezione con i set di keyword definiti in CATEGORIES.
    """
    subpaths = path.split("/")
    
    if not subpaths:
        return "Altro"

    for category, keywords in CATEGORIES.items():
        for keyword in keywords:
            if keyword in subpaths:
                # Special cases
                if "si-fara" in subpaths:
                    return "Si farà"
                if "serie-tv" in subpaths:
                    return "Serie TV"
                if "anticipazioni" in subpaths:
                    return "Anticipazioni"
                if category == "Recensioni" and "in-sala" in subpaths:
                    return "Recensioni / In Sala"
                if category == "Trailers" and "in-sala" in subpaths:
                    return "Trailers / In Sala"
                return category
    return "Altro"


