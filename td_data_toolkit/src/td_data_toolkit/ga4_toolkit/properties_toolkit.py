import pandas as pd
from typing import Union, Dict, List

# Define GA4 property mappings
GA4_PROPERTY_DICTIONARY: Dict[str, Dict[str, str]] = {
    "it_en": {
        'Percorso pagina e classe schermata': 'Page path and screen class',
        'Visualizzazioni': 'Views',
        'Utenti attivi': 'Active users',
        'Visualizzazioni per utente attivo': 'Views per active user',
        'Durata media del coinvolgimento per utente attivo': 'Average engagement time per active user',
        'Conteggio eventi': 'Event count',
        'Eventi chiave': 'Key events',
        'Entrate totali': 'Total revenue'
    }
}

GA4_PROPERTY_MAPPING: Dict[str, Dict[str, str]] = {
    "en": {
        "pagepath": "Page path and screen class"
    },
    "it": {
        "pagepath": "Percorso pagina e classe schermata"
    }
}

def translate_label(label: str, dictionary: str = "it_en") -> str:
    """
    Translate a single label using the GA4 property dictionary.

    :param label: The label to be translated.
    :type label: str
    :param dictionary: The translation dictionary key, default is "it_en".
    :type dictionary: str
    :return: Translated label if found, otherwise the original label.
    :rtype: str

    **Example**::

        translate_label("Visualizzazioni")
        # Output: 'Views'
    """
    return GA4_PROPERTY_DICTIONARY.get(dictionary, {}).get(label, label)

def translate_labels(labels: Union[List[str], pd.Index, pd.Series], dictionary: str = "it_en") -> Union[List[str], pd.Index, pd.Series]:
    """
    Translate multiple labels from a list, pandas Index, or Series using the GA4 property dictionary.

    :param labels: Collection of labels to be translated.
    :type labels: list[str] | pd.Index | pd.Series
    :param dictionary: The translation dictionary key.
    :type dictionary: str
    :return: Translated labels in the same format as input.
    :rtype: list[str] | pd.Index | pd.Series

    **Example**::

        translate_labels(["Visualizzazioni", "Utenti attivi"])
        # Output: ['Views', 'Active users']
    """
    if isinstance(labels, list):
        return [translate_label(label, dictionary) for label in labels]
    elif isinstance(labels, (pd.Series, pd.Index)):
        return labels.map(lambda x: translate_label(x, dictionary))
    return labels

def remove_home_page(traffic_data: pd.DataFrame, source_language: str = "en") -> pd.DataFrame:
    """
    Remove rows where the page path is the homepage ('/') from the given traffic data DataFrame.

    :param traffic_data: The traffic data containing GA4 metrics.
    :type traffic_data: pd.DataFrame
    :param source_language: The language key to look up the appropriate column name.
    :type source_language: str
    :return: Filtered DataFrame without homepage rows.
    :rtype: pd.DataFrame

    :raises ValueError: If the page path column mapping is not found.

    **Example**::

        df = pd.DataFrame({
            "Page path and screen class": ["/", "/about", "/contact"]
        })
        clean_df = remove_home_page(df)
        # Output: rows with "/about" and "/contact"
    """
    pagepath_col = GA4_PROPERTY_MAPPING.get(source_language, {}).get("pagepath")
    if not pagepath_col:
        raise ValueError(f"Page path column mapping not found for language: {source_language}")
    return traffic_data[traffic_data[pagepath_col] != "/"]

def translate_dataframe_columns(traffic_data: pd.DataFrame, dictionary: str = "it_en") -> pd.DataFrame:
    """
    Translate the column names of a DataFrame using the GA4 property dictionary.

    :param traffic_data: DataFrame whose columns need translation.
    :type traffic_data: pd.DataFrame
    :param dictionary: The translation dictionary key.
    :type dictionary: str
    :return: DataFrame with translated column names.
    :rtype: pd.DataFrame

    **Example**::

        df = pd.DataFrame({
            "Visualizzazioni": [100, 200],
            "Utenti attivi": [10, 20]
        })
        translated_df = translate_dataframe_columns(df)
        # Output: columns renamed to 'Views' and 'Active users'
    """
    data_copy = traffic_data.copy(deep=True)
    data_copy.columns = translate_labels(data_copy.columns, dictionary)
    return data_copy


    

    