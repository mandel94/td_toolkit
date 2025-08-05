# Category mapping: Maps broader category names to a set of specific path keywords.
CATEGORIES = {
    "News": {"latest-news", "focus-italia"},
    # "Anticipazioni": {"anticipazioni"},
    "Recensioni": {"review", "netflix-film", "sky-film", "disney-film", "mubi", "mubi-film",
                   "live-streaming-on-demand", "approfondimenti", "streaming"},
    "In Sala": {"in-sala"},
    "Cult Movies": {"cult-movie"},
    "Animazione": {"animazione"},
    "Approfondimento": {"approfondimento"},
    "Festival di Cinema": {"festival-di-cinema"},
    "Trailers": {"trailers"},
    "Serie TV": {"serie-tv", "netflix-serie-tv", "prime-video-serietv", "sky-serie-tv",
                 "disney-serietv", "paramount-serie-tv", "appletv-serietv", "tim-vision-serie-tv"},
    "Guide e Film da Vedere": {"film-da-vedere"},
    "Speciali e Magazine": {"magazine-2", "taxidrivers-magazine"},
    "Rubriche": {"rubriche"},
    "Interviste": {"interviews"}
}

def map_ga4_categories(path: str) -> str:
    """
    Maps a given URL path to a corresponding content category based on predefined keywords.

    :param path: The URL path string to categorize. Example: ``"recensioni/netflix-film/in-sala"``
    :type path: str

    :returns: The mapped category name. Possible values include:
              - "Recensioni"
              - "News"
              - "Serie TV"
              - "Recensioni / In Sala"
              - "Trailers / In Sala"
              - "Altro" (default if no match is found)
    :rtype: str

    The function analyzes the path by splitting it into subpaths and checking for intersections
    with keyword sets defined in the global ``CATEGORIES`` dictionary.
    """
    subpaths = path.split("/")
    
    if not subpaths:
        return "Altro"

    for category, keywords in CATEGORIES.items():
        if keywords.intersection(subpaths):
            # Special case: category mixed with "in-sala"
            # If anticipazioni is in the path, it should be prioritized
            if "anticipazioni" in subpaths:
                return "Anticipazioni"
            if category == "Recensioni" and "in-sala" in path:
                return "Recensioni / In Sala"
            if category == "Trailers" and "in-sala" in path:
                return "Trailers / In Sala"
            return category

    return "Altro"




