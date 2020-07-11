from html.parser import HTMLParser
import html
class StripHtml(HTMLParser):
	def __init__(self):
		super().__init__()
		self.reset()
		self.strict = False
		self.convert_charrefs= True
		self.fed = []

	def handle_data(self, d):
		self.fed.append(d)

	def get_data(self):
		return ''.join(self.fed)

class StripHtmlWix(HTMLParser):
	def __init__(self):
		super().__init__()
		self.reset()
		self.strict = False
		self.convert_charrefs= True
		self.fed = []

	def handle_starttag(self, tag, attrs):
		if tag in ['br', 'strong', 'a', 'br', 'span', 'img', 'ul', 'li']:
			# if tag == 'br':
			# 	self.fed.append('\n')
			# else:
			tag_data = '<' + tag
			if tag in ['a', 'img']:
				for i in attrs:
					name, value = i
					if name in ['href','src']:
						if 'http' not in value:
							value = 'https:' + value
						tag_data += ' ' + name + '="' + value + '" '
			self.fed.append(tag_data + '>')

	def handle_endtag(self, tag):
		if tag in ['br', 'strong', 'a', 'br', 'span', 'img',  'ul', 'li']:
			# if tag == 'br':
			# 	self.fed.append('\n')
			# else:
			# 	self.fed.append('</' + tag + '>')
			self.fed.append('</' + tag + '>')

	def handle_data(self, d):
		d = html.unescape(d)
		self.fed.append(d)

	def get_data(self):
		return ''.join(self.fed)