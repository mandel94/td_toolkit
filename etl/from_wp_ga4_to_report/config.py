# Configuration file for the ETL process

GA4_FILE_PATH = "../../data/Pagine_e_schermate_Percorso_pagina_e_classe_schermata_ultimo_anno_9marzo2025.csv"
WP_FILE_PATH = "../../data/taxidriversit.WordPress.2025-04-14.xml"
OUTPUT_FILE_PATH = "../../output/processed_data.csv"
WP_FILE_TYPE = "xml"
COLUMNS_TO_KEEP = [
    "title", "link", "category", "pagepath", "pubdate", "views",
    "_yoast_wpseo_focuskw", "_yoast_wpseo_metadesc", "_yoast_wpseo_linkdex", "content"
]

