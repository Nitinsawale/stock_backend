from urllib.request import urlopen
from bs4 import BeautifulSoup
import re
from server.modules.web_scraper.html_parser import HtmlParser

class WebScraper:


	def __init__(self):
		self.site_url = ""
		self.html_parser = HtmlParser()
	

	async def fetch_url_page(self, url):
		
		page = urlopen(url)
		html_bytes = page.read()
		html_string = html_bytes.decode("utf-8")
		self.html_parser = HtmlParser(BeautifulSoup(html_string, "html.parser"))
		return await self.html_parser.parse_html()

	async def filter_text(self, text):
		regex = re.compile("\n|\\s{2,}")
		return re.sub(regex,"", text)
	
	async def parse_html(self, html_data):
		await self.html_parser.parse_html()
		parent = html_data.name
		data = {parent:[]}
		for x in html_data.children:
			if x.name and hasattr(x, "children"):
				data[parent].append(await self.parse_html(x))
				
			if not hasattr(x, "children"):
				text_to_append = await self.filter_text(x.getText())
				if text_to_append:
					data[parent].append(text_to_append)
		return data
	
