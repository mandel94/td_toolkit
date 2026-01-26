"""
Gemini Toolkit: Title Structure Checker
======================================

This module provides a function to check if a given movie review title follows a specific structure using the Gemini API.

Overview
--------
- The main function, `check_title_structure_with_gemini`, sends a prompt to Gemini to determine if a title matches a required pattern (e.g., "movie title, la recensione").
- The prompt is built using a functional strategy pattern, allowing for easy extension to new title structures.
- The only implemented structure is "title,review" (movie title followed by a review phrase).

Usage
-----
.. code-block:: python

    from gemini_toolkit import check_title_structure_with_gemini
    title = "Il padrino 3, la recensione"
    check_title_structure_with_gemini(title, structure="title,review")

This will print "YES" if the title matches the structure, or "NO" otherwise.

How to Contribute
-----------------
- To add a new structure, define a new prompt builder function (e.g., `build_new_structure_prompt`).
- Extend the `check_title_structure_with_gemini` function to handle the new structure value and call your builder.
- Keep the interface simple: the function should only print the Gemini response.
- Do not use object-oriented programming; keep the strategy pattern functional.
- Document any new structure clearly in the docstring and usage examples.

API Key
-------
- The Gemini API key is hardcoded for demonstration. For production, use environment variables or a config file.

"""

import requests
import json
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()
API_KEY = os.getenv('GEMINI_API_KEY')


def build_title_review_prompt(title: str) -> str:
    """
    Build a prompt for checking if a title follows the 'title, la recensione' structure.

    :param title: The title string to check.
    :type title: str
    :return: The prompt string for Gemini.
    :rtype: str
    """
    return f'''
Given the following title:
"{title}"

Determine whether it follows this structure:
"A movie title, followed by a 'la recensione' or something similar

Examples of valid titles:
- Il padrino 3, la recensione

There should be a comma, colon or period after the title of the movie

Respond only with "YES" if the structure matches, or "NO" if it does not.
'''


def check_title_structure_with_gemini(title: str, structure: str = "title,review"):
    """
    Check if a title matches a given structure using Gemini.

    :param title: The title string to check.
    :type title: str
    :param structure: The structure type (default: "title,review").
    :type structure: str
    :raises ValueError: If the structure is unknown.
    :return: None. Prints Gemini's response ("YES" or "NO").
    """
    URL = f'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={API_KEY}'
    headers = {'Content-Type': 'application/json'}

    # Strategy pattern (functional): select prompt builder based on structure
    if structure == "title,review":
        prompt = build_title_review_prompt(title)
    else:
        raise ValueError(f"Unknown structure: {structure}")

    data = {
        "contents": [
            {
                "parts": [
                    {"text": prompt.strip()}
                ]
            }
        ]
    }

    response = requests.post(URL, headers=headers, json=data)

    if response.status_code == 200:
        result = response.json()
        reply = result.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
        print("Gemini response:", reply.strip())
    else:
        print(f"Request failed with status code {response.status_code}")
        print(response.text)


def gemini_batch_prompt(titles, prompt_template):
    """
    Apply a prompt template to a list of titles using Gemini API.
    :param titles: List of titles (strings)
    :param prompt_template: A function that takes a title and returns a prompt string
    :return: List of Gemini responses
    """
    URL = f'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={API_KEY}'
    headers = {'Content-Type': 'application/json'}
    results = []
    for title in titles:
        prompt = prompt_template(title)
        data = {
            "contents": [
                {"parts": [{"text": prompt.strip()}]}
            ]
        }
        response = requests.post(URL, headers=headers, json=data)
        if response.status_code == 200:
            result = response.json()
            reply = result.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
            results.append(reply.strip())
        else:
            results.append(f"ERROR: {response.status_code}")
    return results


if __name__ == "__main__":
    title = "‘Questa sono io’ di Małgorzata Szumowska e Michał Englert, una toccante storia queer"
    check_title_structure_with_gemini(title, structure="title,review")
