# Simple web spider
# Version : 0.0.1
# Ceated : 17/06/2015
# Last edited : 17/06/2015

import urllib
from pyquery import PyQuery as pq
from lxml import etree
import urlparse
import mechanize
from termcolor import colored

import os
import sys
import time
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/dbLibrary')
from sqliteDb import sqlite3Db

class amazonCrawler:
	BASE_DIR = os.path.dirname(os.path.realpath(__file__))
	checkingUrl = ""

	def __init__(self, url):
		parsedUrl = urlparse.urlparse(url)
		self.root = parsedUrl.scheme + '://' + parsedUrl.netloc + '/'
		self.level = 0
		self.levelArray = []
		self.navigationUrlArray = []
		self.navigationNumber = []
		
		self.dataObject = {
			"Dress" : [
				"http://www.amazon.in/s/ref=lp_1968445031_pg_2?rh=n%3A1571271031%2Cn%3A%211571272031%2Cn%3A1953602031%2Cn%3A1968445031&page=1&ie=UTF8&qid=1434613511",
				"http://www.amazon.in/s/ref=lp_1968448031_pg_2?rh=n%3A1571271031%2Cn%3A%211571272031%2Cn%3A1953602031%2Cn%3A1968448031&page=1&ie=UTF8&qid=1434613899",
				"http://www.amazon.in/s/ref=lp_1968500031_pg_2?rh=n%3A1571271031%2Cn%3A%211571272031%2Cn%3A1953602031%2Cn%3A1968498031%2Cn%3A1968500031&page=1&ie=UTF8&qid=1434614267",
				"http://www.amazon.in/s/ref=lp_1968501031_pg_2?rh=n%3A1571271031%2Cn%3A%211571272031%2Cn%3A1953602031%2Cn%3A1968498031%2Cn%3A1968501031&page=1&ie=UTF8&qid=1434614287",
			],

			"Top" : [
				"http://www.amazon.in/s/ref=lp_1968450031_pg_2?rh=n%3A1571271031%2Cn%3A%211571272031%2Cn%3A1953602031%2Cn%3A1968449031%2Cn%3A1968450031&page=1&ie=UTF8&qid=1434613925",
				"http://www.amazon.in/s/ref=lp_1968454031_pg_2?rh=n%3A1571271031%2Cn%3A%211571272031%2Cn%3A1953602031%2Cn%3A1968449031%2Cn%3A1968454031&page=1&ie=UTF8&qid=1434613951",
				"http://www.amazon.in/s/ref=lp_1968449031_nr_n_2?fst=as%3Aoff&rh=n%3A1571271031%2Cn%3A%211571272031%2Cn%3A1953602031%2Cn%3A1968449031%2Cn%3A1968452031&bbn=1968449031&ie=UTF8&qid=1434528315&rnid=1968449031",
				"http://www.amazon.in/s/ref=lp_1968453031_pg_2?rh=n%3A1571271031%2Cn%3A%211571272031%2Cn%3A1953602031%2Cn%3A1968449031%2Cn%3A1968453031&page=1&ie=UTF8&qid=1434614006",
				"http://www.amazon.in/s/ref=lp_1968451031_pg_2?rh=n%3A1571271031%2Cn%3A%211571272031%2Cn%3A1953602031%2Cn%3A1968449031%2Cn%3A1968451031&page=1&ie=UTF8&qid=1434614048",
				"http://www.amazon.in/s/ref=lp_1968506031_pg_2?rh=n%3A1571271031%2Cn%3A%211571272031%2Cn%3A1953602031%2Cn%3A1968505031%2Cn%3A1968506031&page=1&ie=UTF8&qid=1434614353",
				"http://www.amazon.in/s/ref=lp_1968508031_pg_2?rh=n%3A1571271031%2Cn%3A%211571272031%2Cn%3A1953602031%2Cn%3A1968505031%2Cn%3A1968508031&page=1&ie=UTF8&qid=1434614400",
				"http://www.amazon.in/s/ref=lp_1968504031_pg_2?rh=n%3A1571271031%2Cn%3A%211571272031%2Cn%3A1953602031%2Cn%3A1968498031%2Cn%3A1968504031&page=2&ie=UTF8&qid=1434614328",
				"http://www.amazon.in/s/ref=lp_1968544031_pg_2?rh=n%3A1571271031%2Cn%3A%211571272031%2Cn%3A1953602031%2Cn%3A1968542031%2Cn%3A1968544031&page=1&ie=UTF8&qid=1434614446",
				"http://www.amazon.in/s/ref=lp_1968444031_pg_2?rh=n%3A1571271031%2Cn%3A%211571272031%2Cn%3A1953602031%2Cn%3A1968542031%2Cn%3A1968444031&page=1&ie=UTF8&qid=1434614495",
				"http://www.amazon.in/s/ref=lp_1968545031_pg_2?rh=n%3A1571271031%2Cn%3A%211571272031%2Cn%3A1953602031%2Cn%3A1968542031%2Cn%3A1968545031&page=1&ie=UTF8&qid=1434614525",
				"http://www.amazon.in/s/ref=lp_1968546031_pg_2?rh=n%3A1571271031%2Cn%3A%211571272031%2Cn%3A1953602031%2Cn%3A1968542031%2Cn%3A1968546031&page=1&ie=UTF8&qid=1434614548",
				"http://www.amazon.in/s/ref=lp_1968543031_pg_2?rh=n%3A1571271031%2Cn%3A%211571272031%2Cn%3A1953602031%2Cn%3A1968542031%2Cn%3A1968543031&page=1&ie=UTF8&qid=1434614571"
			]
		}

	def run(self):
		try:
			for data in self.dataObject:
				for links in self.dataObject[data]:
					print colored('Log : Category Selected ->'+ data , 'green')

					self.startFetchingData(links, 1 if data == 'Dress' else 2)

		except Exception as e:
			print 'Error : In run function'
			print e

	def startFetchingData(self, link, navId):
		try:
			self.checkingUrl = link
			print colored('Log : Fetcing data for ->'+ self.checkingUrl , 'green')
			pageHtml = self.getHtmlContent(self.checkingUrl)

			if pageHtml != None:
				jQuery = pq(pageHtml) # python Query works exactly like jQuery
				self.fetchAllNavaigationLinks(jQuery)
				self.fetchHtmlContents(jQuery, navId)
				
				while(len(self.navigationUrlArray)>0):
					self.checkingUrl = self.navigationUrlArray[0]
					print colored('Log : Fetcing data for Loop ->'+ self.checkingUrl, 'green')
					pageHtml = self.getHtmlContent(self.checkingUrl)

					if pageHtml != None:
						jQuery = pq(pageHtml) # python Query works exactly like jQuery
						# self.fetchAllNavaigationLinks(jQuery)
						self.fetchHtmlContents(jQuery, navId)

					else:
						print colored('Log : Page html is returned as none', 'yellow')

					print colored('Log : Navigationa array length ->'+ str(len(self.navigationUrlArray)), 'blue')
					self.navigationUrlArray.pop(0)

				self.navigationNumber = []

			else:
				print colored('Log : Page html is returned as none', 'yellow')

		except Exception as e:
			print colored('Error : During startFetchingData function', 'red')
			print e

	def fetchHtmlContents(self, jQuery, navId):
		try:
			productContainer = jQuery('#mainResults')
			print colored('If part : '+ str(len(productContainer)), 'blue')
			if(len(productContainer) != 0):
				for product in jQuery('[id^="result_"]'):
					product = jQuery(product)

					selectedImage = product.find('img').eq(0)
					imgSrc = jQuery(selectedImage).attr('src').split('.')
					imgSrc[-2] = '_UX522_'
					imgSrc = '.'.join(imgSrc)

					link = jQuery(selectedImage).closest('a').attr('href')
					link = urlparse.urljoin(self.root, link)

					name = product.find('h3>a').text()
					brandName = product.find('h3>span').text().replace('by ', '').strip() if len(product.find('h3>span')) != 0 else product.find('h3>span').text()
					price = product.find('.rsltGridList>.newp a>del').text().replace('from ', '').strip() if len(product.find('.rsltGridList>.newp a>del')) != 0 else product.find('.rsltGridList>.newp a>del').text().replace('from ', '')
					discountPrice = product.find('.rsltGridList>.newp a>span').text().replace('from ', '').strip() if len(product.find('.rsltGridList>.newp a>span')) != 0 else product.find('.rsltGridList>.newp a>span').text().replace('from ', '')
					# print {'imgSrc':imgSrc, 'link':link, 'name':name, 'brandName':brandName, 'price':price, 'discountPrice':discountPrice}
					self.saveDatainDatabase({'imgSrc':imgSrc, 'link':link, 'name':name, 'brandName':brandName, 'price':price, 'discountPrice':discountPrice}, navId)
			
			else:
				productContainer = jQuery('#s-results-list-atf')
				print colored('Else part : '+ str(len(productContainer.find('.s-result-item'))), 'blue')
				# for product in productContainer.find('.s-result-item'):
				for product in jQuery('[id^="result_"]'):
					product = jQuery(product)

					selectedImage = product.find('img').eq(0)
					imgSrc = jQuery(selectedImage).attr('src').split('.')
					imgSrc[-2] = '_UX522_'
					imgSrc = '.'.join(imgSrc)

					link = jQuery(selectedImage).closest('a').attr('href')
					link = urlparse.urljoin(self.root, link)

					name = product.find('h2').text()
					brandName = product.find('h2').closest('div').siblings('div').text().replace('by ', '')
					brandName = brandName.strip() if brandName != None else brandName

					price = product.find('.currencyINR').eq(0).parent().text().replace('from ', '')
					discountPrice = product.find('.currencyINR').eq(1).parent().text().replace('from ', '')
					# print {'imgSrc':imgSrc, 'link':link, 'name':name, 'brandName':brandName, 'price':price, 'discountPrice':discountPrice}
					self.saveDatainDatabase({'imgSrc':imgSrc, 'link':link, 'name':name, 'brandName':brandName, 'price':price, 'discountPrice':discountPrice}, navId)
			

		except Exception as e:
			print colored('Error : During fetchHtmlContentsAndSave function call !!', 'red')
			print e

	def saveDatainDatabase(self, dataObject, navId):
		try:
			sqlite = sqlite3Db(self.BASE_DIR + '/dataBases/dressAndTop.sqlite3')
			sqlite.table = '`crawled_data`'
			sqlite.columns = "`link`, `img_src`, `name`, `brand_name`, `price`, `discount_price`, `availability`, `navigation_id`"
			sqlite.values = "?, ?, ?, ?, ?, ?, ?, ?"
			sqlite.insertArray = [(dataObject['link'], dataObject['imgSrc'], dataObject['name'], dataObject['brandName'], dataObject['price'], dataObject['discountPrice'], True, navId)]
			insertedId = sqlite.insert()
			sqlite.close()

			print colored('Log : Crawler data inserted ->'+ str(insertedId), 'green')

		except Exception as e:
			print colored('Error : during saveDatainDatabase function call !!', 'red')
			print e

	def fetchAllNavaigationLinks(self, jQuery):
		try:
			# self.checkingUrl
			pagination = jQuery('#pagn')
			if len(jQuery(pagination).find('span')) > 0:
				obj = self.parseUrlAndCreateParamaeterArray(self.checkingUrl, 'split')
				parsedUrl = urlparse.urlparse(self.checkingUrl)
				pageUptoNumber = jQuery(pagination).find('.pagnDisabled').text() if jQuery(pagination).find('.pagnDisabled').length != 0 else jQuery(pagination).find('.pagnLink').eq(jQuery(pagination).find('.pagnLink').length - 1).text()
				
				print colored('Log : Found page upto number ->'+ str(pageUptoNumber), 'blue')
				
				if pageUptoNumber != None:
					for i in range(int(pageUptoNumber.strip()) + 1):
						if i != 0 and i != 1:
							obj['page'] = i
							link = self.parseUrlAndCreateParamaeterArray(obj)
							link = parsedUrl.scheme + '://' + parsedUrl.netloc + parsedUrl.path + '?' + link
							self.navigationUrlArray.append(link)

		except Exception as e:
			print colored('Error : during fetchAllNavaigationLinks function call', 'red')
			print e

	def parseUrlAndCreateParamaeterArray(self, data, rfor = None):
		try:
			if rfor == 'split':
				obj = {}
				parsedUrl = urlparse.urlparse(data)
				splittedParameter = parsedUrl.query.split('&')

				for parameter in splittedParameter:
					parameter = parameter.split('=')
					if obj.get(parameter[0].strip()) == None:
						obj[parameter[0].strip()] = parameter[1].strip()

				return obj

			else:
				string = ""
				for parameter in data:
					string += str(parameter) + '=' + str(data[parameter]) if string == "" else '&' + str(parameter) + '=' + str(data[parameter])

				return string

		except Exception as e:
			print colored('Error : during parseUrlAndCreateArray function call', 'red')
			print e

	def getHtmlContent(self, url):
		html = None
		try:
			br = mechanize.Browser()
			br.set_handle_robots(False) # Do not read the robot.txt
			br.addheaders = [('User-agent', 'Firefox')] # Setting the browser type
			html = br.open(url).read()
			return html

		except Exception as e:
			print colored("Error : Got some error during fetching URL -> " + url, 'red')
			print e
			print colored("Log : Trying second method to fetch URL", 'yellow')
			
			try:
				html = urllib.urlopen(url).read()
				return html

			except Exception as e:
				print colored("Can't fetch URL at all -> ", 'red')
				print e
				return html

crawler = amazonCrawler('http://www.amazon.in/')
crawler.run()