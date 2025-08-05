"""
A simple client for Google Gemini using the google-genai package.
"""
import os
import google.genai as genai

class GeminiClient:
    def __init__(self, api_key: str):
        self.api_key = api_key

    def generate_text(self, prompt: str, model: str = "gemini-2.5-flash", **kwargs) -> str:
        """
        Generate text from a prompt using the specified Gemini model.
        """
        client = genai.Client(api_key=self.api_key)
        response = client.models.generate_content(
            model=model,
            contents=prompt,
            **kwargs
        )
        return response.text if hasattr(response, 'text') else response

    def chat(self, messages, model: str = "gemini-pro", **kwargs):
        """
        Start a chat session with a list of messages (dicts with 'role' and 'content').
        """
        client = genai.Client(api_key=self.api_key)
        response = client.models.chat(
            model=model,
            messages=messages,
            **kwargs
        )
        return response

    # Add more methods as needed for your use case

class WeeklyTopOfTheTops(GeminiClient):
    """
    Client for a specific Weekly Top Of The Tops task.
    Extend this class to implement the desired functionality.
    """
    def __init__(self, api_key: str):
        super().__init__(api_key=api_key)

    def generate(self, articles, model="gemini-2.5-flash", **kwargs):
        """
        Generate a summary of the weekly top articles for Taxi Drivers, using a structured prompt.
        Args:
            articles: list or DataFrame of articles with traffic data, category, author, etc.
        """
        prompt = (
            "Hai a disposizione un elenco di articoli con dati di traffico (visualizzazioni, utenti attivi, durata media ecc.), categoria e autore. "
            "Il tuo compito è:\n"
            "Analizzare i dati e identificare i contenuti con le migliori performance della settimana, tenendo conto del traffico totale, coinvolgimento (durata), numero di utenti attivi e attualità (data di pubblicazione recente).\n\n"
            "Riorganizzare i contenuti selezionati in una lista Top della Settimana, divisa in 4 sezioni tematiche:\n\n"
            "👀 Si farà\n\n"
            "🎬 Recensioni\n\n"
            "📺 Serie TV\n\n"
            "🔎 Approfondimenti\n\n"
            "📰 News\n\n"
            "Per ogni sezione, mostra i 3 titoli migliori.\n\n"
            "Mostra il titolo dell’articolo (senza URL), seguito dal nome dell’autore tra parentesi.\n\n"
            "Aggiungi emoji tematiche per rendere l’elenco più leggibile e accattivante, come nell'esempio seguente.\n\n"
            "Esempio di output desiderato (con gli attributi che troverai nei dati da inserire):\n"
            "📊 Top Contenuti della Settimana 📊\n"
            " 🔎 1. Si farà \n"
            " • {Titolo Articolo}({Nome Autore})\n"
            " • {Titolo Articolo}({Nome Autore})\n"
            " • {Titolo Articolo}({Nome Autore})\n"
            "🎬 2. Recensioni \n"
            " • {Titolo Articolo}({Nome Autore})\n"
            " • {Titolo Articolo}({Nome Autore})\n"
            " • {Titolo Articolo}({Nome Autore})\n"
            "📺 3. Serie TV \n"
            " • {Titolo Articolo}({Nome Autore})\n"
            " • {Titolo Articolo}({Nome Autore})\n"
            " • {Titolo Articolo}({Nome Autore})\n"
            "📺 4. Approfondimento \n"
            " • {Titolo Articolo}({Nome Autore})\n"
            " • {Titolo Articolo}({Nome Autore})\n"
            " • {Titolo Articolo}({Nome Autore})\n"
            "📰 4. News \n"
            " • {Titolo Articolo}({Nome Autore})\n"
            " • {Titolo Articolo}({Nome Autore})\n"
            " • {Titolo Articolo}({Nome Autore})\n\n"
            "I contenuti sono stati selezionati prendendo in considerazione il numero di visualizzazioni ricevute dall'articolo.\n\n"
            "A PARTIRE DAI DATI CHE SEGUONO:\n"
            f"{articles}\n"
        )
        return self.generate_text(prompt, model=model, **kwargs)

class GeminiKeywordCategoryMapper(GeminiClient):
    """
    Uses Gemini to map a keyword to a category/topic and subcategory related to the arts and entertainment sector.
    """
    def __init__(self, api_key: str, model: str = "gemini-2.5-flash"):
        super().__init__(api_key=api_key)
        self.model = model

    def map_keyword(self, keyword: str) -> dict:
        prompt = (
            "Dato il seguente termine chiave relativo al settore arte e intrattenimento, "
            "assegna:\n"
            "1. la categoria o topic più rilevante tra: 'Cinema', 'Serie TV', 'Musica', 'Teatro', 'Arte', 'Recensioni', 'Eventi', 'News', 'Approfondimento', 'Altro'.\n"
            "2. la sottocategoria più rilevante (ad esempio: 'film', 'regista', 'festival', 'album', 'attore', 'critica', ecc.).\n"
            "Rispondi solo in formato JSON come nell'esempio: {\"categoria\": \"...\", \"sottocategoria\": \"...\"}.\n"
            f"Keyword: {keyword}"
        )
        response = self.generate_text(prompt, model=self.model).strip()
        # Try to parse the response as JSON
        import json
        try:
            result = json.loads(response)
        except Exception:
            # Fallback: try to extract with regex or return as string
            result = {"categoria": None, "sottocategoria": None, "raw": response}
        return result

# Example usage:
# client = GeminiClient(api_key="YOUR_API_KEY")
# print(client.generate_text("Hello, Gemini!"))

if __name__ == "__main__":
    # Example usage in a script
    client = GeminiClient(api_key=os.getenv("GEMINI_API_KEY"))
    print(client.generate_text("Hello, Gemini!"))