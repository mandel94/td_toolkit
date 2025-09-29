# This code scrapes the url https://www.taxidrivers.it/archivio and extracts
# the archive of articles in a given format. 
# The code uses requests and BeautifulSoup to perform the scraping.
# You can scrape the articles as listem items with class mvp-blog-story-wrap left relative infinite-post
# Inside the list item, you can find the following attributes to extract:
# - title: 
# - wordpress_category: 
# - published (text in italian, like "2 ore fa", "14 ore fa", "1 giorno fa", "1 mese fa" e così via):
# - link to the article: href of first <a> of the list item 
# - wordpress_category: <span> element descendant of the list item, with class mvp-cd-cat 
# - published: text of <span> element descendant of the list item, with class mvp-cd-date 


from bs4 import BeautifulSoup
import pandas as pd

def scrape_archive(html_path="archive_page.html"):
	with open(html_path, "r", encoding="utf-8") as f:
		html_text = f.read()
	soup = BeautifulSoup(html_text, "html.parser")
	articles = []
	# Find all list items with the target class
	for item in soup.find_all(class_="mvp-blog-story-wrap"):
		# Title: text of first <a> inside the list item
		a_tag = item.find("a")
		title_tag = item.find("h2")
		cat_tag = item.find(class_="mvp-cd-cat")
		date_tag = item.find(class_="mvp-cd-date")	
		link = a_tag["href"] if a_tag and a_tag.has_attr("href") else None
		title = title_tag.text.strip() if title_tag else None
		# WordPress category: <span> descendant with class mvp-cd-cat
		wordpress_category = cat_tag.text.strip() if cat_tag else None
		# Published: <span> descendant with class mvp-cd-date
		published = date_tag.text.strip() if date_tag else None
		articles.append({
			"title": title,
			"wordpress_category": wordpress_category,
			"published": published,
			"link": link,
			"pagePath": link.replace("https://www.taxidrivers.it", "") if link else None
		})
	# Return articles as a dataframe
	return pd.DataFrame(articles)

# Example usage:
if __name__ == "__main__":
	archive_articles = scrape_archive()
	import pandas as pd
	df = pd.DataFrame(archive_articles)
	print(df.shape)



