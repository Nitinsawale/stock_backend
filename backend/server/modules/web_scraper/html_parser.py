import bs4
import re
class HtmlParser():


	def __init__(self, html_data=""):
		print("")
		self.html_data = html_data
		self.filter_text_regex = re.compile("\n|\\s{2,}")

	async def filter_text(self, text):
		return re.sub(self.filter_text_regex,"", text)
	

	async def parse_html_tag(self,tag):
	
		all_data = []

		if tag.name and hasattr(tag, "children"):
			all_childrens = [x for x in tag.children]
			if all_childrens:
				
				for index, x in enumerate(all_childrens):
					if type(x) == bs4.element.Tag:
						tag_data = await self.parse_html_tag(x)
						#if tag_data:
						all_data.append({"tag_name": x.name,"tag_data":tag_data, "tag_attributes":x.attrs})
					elif type(x) == bs4.element.NavigableString:
					
						strr = await self.filter_text(x.get_text())
						if strr:
							all_data.append(strr)
		return all_data

	async def parse_html(self):
		
		parent = self.html_data.name
		data = {parent:[]}
		for x in self.html_data.children:

			if type(x) == bs4.element.NavigableString:
				text_to_append = await self.filter_text(x.getText())
				if text_to_append:
					data[parent].append(text_to_append)
				continue

			elif type(x) == bs4.element.Tag:
				data['parent'] = await self.parse_html_tag(x)
		return data
