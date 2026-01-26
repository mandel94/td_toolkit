import pandas as pd
import os
import sys
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from reports.gemini_toolkit import gemini_batch_prompt

# Load environment variables from .env file
load_dotenv()
API_KEY = os.getenv("GEMINI_API_KEY")


def gemini_apply_prompt(titles, prompt_template):
    """
    Apply a prompt template to a title or list of titles using Gemini API.
    :param titles: A string (title) or list of strings (titles)
    :param prompt_template: A function that takes a title and returns a prompt string
    :return: Gemini response(s)
    """
    if isinstance(titles, str):
        return gemini_batch_prompt([titles], prompt_template)[0]
    elif isinstance(titles, list):
        return gemini_batch_prompt(titles, prompt_template)
    else:
        raise ValueError("titles must be a string or a list of strings")


def gemini_intent_category(title):
    """
    Categorize the search intent of the article title (e.g., informational, transactional, navigational).
    """
    prompt = f"""
Classifica la seguente stringa in una delle seguenti categorie di intento di ricerca: informazionale, transazionale, navigazionale. Restituisci solo la categoria.
Titolo: {title}
"""
    return gemini_apply_prompt(title, lambda t: prompt)


def gemini_keyword_clusters(title):
    """
    Suggest 3 related SEO keyword clusters for the article title.
    """
    prompt = f"""
Suggerisci 3 cluster di keyword SEO correlate per il seguente titolo di articolo. Restituisci solo una lista separata da virgole.
Titolo: {title}
"""
    return gemini_apply_prompt(title, lambda t: prompt)


def gemini_semantic_topic(title):
    """
    Classify the article title into a semantic topic (macro-argomento) in Italian.
    """
    prompt = f"""
Classifica semanticamente il seguente titolo di articolo in un macro-argomento rilevante per un sito di cinema italiano. Restituisci solo il nome del macro-argomento.
Titolo: {title}
"""
    return gemini_apply_prompt(title, lambda t: prompt)


def gemini_assign_main_topic(title):
    """
    Assign the most relevant main topic to the title, choosing among the main thematic categories for a cinema, film, and TV series magazine.
    """
    prompt = f"""
Attribuisci al seguente topic la categoria più pertinente, scegliendola tra le principali categorie tematiche, ricordandoti che stiamo parlando di un magazine di cinema, film e serie tv.
Titolo: {title}
"""
    return gemini_apply_prompt(title, lambda t: prompt)


if __name__ == "__main__":
    metadata = pd.read_csv("data/SEO Analysis Session 1.csv", sep=";")
    data = pd.read_csv("data/SEO Analysis Session 1.csv", sep=",", skiprows=9)
    # Test Gemini with mock title
    mock_title = "The best movies of Quentin Tarantino"
    print("Intent category:", gemini_intent_category(mock_title))
    # print("Keyword clusters:", gemini_keyword_clusters(mock_title))
    # print("Semantic topic:", gemini_semantic_topic(mock_title))
    # print("Main topic:", gemini_assign_main_topic(mock_title))
    # Print semantic topic
    print("Semantic topic:", gemini_semantic_topic(mock_title))
