# Youtube data fetching crawler
# Version : 0.0.2
# Ceated : 11/05/2015
# Last edited : 25/05/2015

import urllib
from pyquery import PyQuery as pq
from lxml import etree
import urlparse
import mechanize
import re
import os
# import math
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/dblibrary')
from sqliteDb import sqlite3Db

# from trendingFeedAndYoutube.sqliteDb import sqlite3Db
# from django.conf import settings

class youtubeDataSearch:
	# BASE_DIR = settings.BASE_DIR
	BASE_DIR = os.path.dirname(os.path.realpath(__file__))
	dataLink = 'https://www.youtube.com/results'
	
	notbePresentWord = ['full movie']
	considerableViewCount = -1

	titleSelector = '#watch-headline-title h1'
	descriptionSelector = '#watch-description-text #eow-description'
	viewCountSelector = 'meta[itemprop="interactionCount"]'
	metaKeywordSelector = 'meta[name="keywords"]'
	durationSelector = 'meta[itemprop="duration"]'
	thumbnailSelector = 'link[itemprop="thumbnailUrl"]'
	publishDateSelector = 'meta[itemprop="datePublished"]'
	categorySelector = 'meta[itemprop="genre"]'
	paidSelector = 'meta[itemprop="paid"]'
	channelIdSelector = 'meta[itemprop="channelId"]'
	videoIdSelector = 'meta[itemprop="videoId"]'
	embedSelector = 'link[itemprop="embedURL"]'
	familyFriendlySelector = 'meta[itemprop="isFamilyFriendly"]'
	regionAllowedSelector = 'meta[itemprop="regionsAllowed"]'

	likesSelector = {'selector':'#watch-like-dislike-buttons button', 'index':0}
	dislikesSelector = {'selector':'#watch-like-dislike-buttons button', 'index':2}

	def __init__(self, sendObject):
		try:
			self.searchTopic = sendObject['topic']
			self.selectedTable = sendObject['table']
			self.relatedSearchObject = sendObject['relatedSearch'].split(',')

			if len(self.relatedSearchObject) == 1 and len(sendObject['relatedSearch']) > len(sendObject['topic']):
				self.searchTopic = sendObject['relatedSearch']

			self.databaseId = sendObject['rowId']

			parsedURL = urlparse.urlparse(self.dataLink)
			self.baseLink = parsedURL.scheme + '://' + parsedURL.netloc + '/'
			queryArray = {
				'search_query':self.searchTopic, 
				# 'search_sort':'video_date_uploaded', 
				'filters':'video, month',
				'lclk':'video',
				'page': 1
			}
			self.fetchNavigationUrls = []
			self.visitedNavigationPageIndex = []
			self.fetchNavigationUrls.append(self.dataLink + '?' + urllib.urlencode(queryArray))
			self.visitedNavigationPageIndex.append(1)

		except Exception as e:
			print 'Error : During initialization !!'
			print e

		try:
			self.linkArray = []
			self.existedLinkArray = []
			self.nameArray = []

			sqlite = sqlite3Db(self.BASE_DIR + '/dataBases/trendingFeed.sqlite3')
			sqlite.table = "`"+ self.selectedTable +"`"
			sqlite.fields = "`title`, `link`"
			sqlite.where = '`topic_id`='+ str(self.databaseId)
			tempData = sqlite.select()
			sqlite.clear()
			sqlite.close()
			sqlite = ""
			
			for data in tempData:
				self.nameArray.append(data[0])
				self.existedLinkArray.append(data[1])

			self.totalVideoFoundCounter = len(self.nameArray)

		except Exception as e:
			print 'Error : During init database data fetch !!'
			print e

	def run(self):
		try:
			print self.fetchNavigationUrls
			print self.visitedNavigationPageIndex

			while len(self.fetchNavigationUrls) > 0:
				self.checkingUrl = self.fetchNavigationUrls[0]
				print 'Log : Checking Navigation URL ->'+ self.checkingUrl
				html = self.fetchHTML(self.checkingUrl)
				if html != None:
					self.fetchAllUrls(html)
				self.fetchNavigationUrls.pop(0)

			self.visitedNavigationPageIndex = []
			print '-------------------------------------------------------------'
			print 'Log : All link is fetched calling the processdata Function !!'
			self.processData()
			return self.totalVideoFoundCounter

		except Exception as e:
			print 'Error : Problem in run function !!'
			print e
			
	def processData(self):
		try:
			print 'Log : Total link found ->'+ str(len(self.linkArray))

			while len(self.linkArray) > 0:
				self.checkingUrl = self.linkArray[0]
				print 'Log : Checking Navigation URL ->'+ self.checkingUrl
				print 'Log : Search Topic ->'+ self.searchTopic

				html = self.fetchHTML(self.checkingUrl)
				if html != None:
					jQuery = pq(html)

					title = jQuery(self.titleSelector).text().lower()
					description = jQuery(self.descriptionSelector).text().lower()

					if title != None and description != None and title != "" and description != "":
						title = title.strip()
						description = description.strip()

						if title not in self.nameArray:
							self.nameArray.append(title)
							status = self.processText(title, description)
							# if status['status']:
							viewCount = int(jQuery(self.viewCountSelector).attr('content').strip()) if jQuery(self.viewCountSelector).attr('content') != None else int(jQuery(self.metaKeywordSelector).attr('content'))
							if viewCount > self.considerableViewCount:
								try:
									inserDataArray = [(
										self.databaseId, 
										re.sub(r'[^ -~].*', '', jQuery(self.titleSelector).text()),
										re.sub(r'[^ -~].*', '', jQuery(self.descriptionSelector).html()),   
										self.checkingUrl, 
										jQuery(self.metaKeywordSelector).attr('content').strip() if jQuery(self.metaKeywordSelector).attr('content') != None else jQuery(self.metaKeywordSelector).attr('content'), 
										jQuery(self.durationSelector).attr('content').strip() if jQuery(self.durationSelector).attr('content') != None else jQuery(self.durationSelector).attr('content'), 
										jQuery(self.thumbnailSelector).attr('href').strip() if jQuery(self.thumbnailSelector).attr('href') != None else jQuery(self.thumbnailSelector).attr('href'), 
										viewCount, 
										jQuery(self.publishDateSelector).attr('content').strip() if jQuery(self.publishDateSelector).attr('content') != None else jQuery(self.publishDateSelector).attr('content'), 
										jQuery(self.categorySelector).attr('content').strip() if jQuery(self.categorySelector).attr('content') != None else jQuery(self.categorySelector).attr('content'), 
										jQuery(self.likesSelector['selector']).eq(self.likesSelector['index']).text(), 
										jQuery(self.dislikesSelector['selector']).eq(self.dislikesSelector['index']).text(), 
										jQuery(self.paidSelector).attr('content').strip() if jQuery(self.paidSelector).attr('content') != None else jQuery(self.paidSelector).attr('content'), 
										jQuery(self.channelIdSelector).attr('content').strip() if jQuery(self.channelIdSelector).attr('content') != None else jQuery(self.channelIdSelector).attr('content'), 
										jQuery(self.videoIdSelector).attr('content').strip() if jQuery(self.videoIdSelector).attr('content') != None else jQuery(self.videoIdSelector).attr('content'), 
										jQuery(self.embedSelector).attr('href').strip() if jQuery(self.embedSelector).attr('href') != None else jQuery(self.embedSelector).attr('href'),
										jQuery(self.familyFriendlySelector).attr('content').strip() if jQuery(self.familyFriendlySelector).attr('content') != None else jQuery(self.familyFriendlySelector).attr('content'), 
										jQuery(self.regionAllowedSelector).attr('content').strip() if jQuery(self.regionAllowedSelector).attr('content') != None else jQuery(self.regionAllowedSelector).attr('content')
									)]

									if status['reason'] == 'relativeSearchFound':
										# if status['foundNumber'] >= int(math.ceil(status['relativeArraylength']/2)):
										self.insertData(inserDataArray)

									elif status['reason'] == 'noRelativeSearchPresent':
										self.insertData(inserDataArray)

								except Exception as e:
									print 'Error : During data fetch from website !!'
									print e

							# else:
							# 	print 'Log : Discarding the video reason -> View Count is low'
							# else:
							# 	print 'Log : Discarding the video reason -> '+ status['reason']
					else:
						print 'Log : Can\'t find the title and description !!'

				print '---------------------------------------------------'
				self.linkArray.pop(0)

			self.nameArray = []

		except Exception as e:
			print 'Error : during processData function !!'
			print e

	def insertData(self, inserDataArray):
		try:
			sqlite = sqlite3Db(self.BASE_DIR + '/dataBases/trendingFeed.sqlite3')

			sqlite.table = "`"+ self.selectedTable +"`"
			sqlite.columns = "`topic_id`, `title`, `description`, `link`, `meta_keywords`, `duration`, `img_thumbnail`, `views`, `publish_date`, `category`, `likes`, `dislikes`, `paid`, `channel_id`, `video_id`, `embed_url`, `family_friendly`, `region_allowed`"
			sqlite.values = "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?"
			sqlite.insertManyArray = inserDataArray
			insertId = sqlite.insertMany()
			print 'Log : Youtube video inserted ->'+ str(insertId)
			sqlite.clear()
			sqlite.close()
			sqlite = ""
			self.totalVideoFoundCounter = self.totalVideoFoundCounter + 1

		except Exception as e:
			print 'Error : Issue during database insertion !!'
			print e

	def processText(self, title, description):
		try:
			searchTopicPresent = False
			notPresentTextStatus = False

			for notPresentText in self.notbePresentWord:
				if not re.search(re.compile('\\b'+ re.escape(notPresentText.lower()) +'\\b'), self.searchTopic):
					if re.search(re.compile('\\b'+ re.escape(notPresentText.lower()) +'\\b'), title) or re.search(re.compile('\\b'+ re.escape(notPresentText.lower()) +'\\b'), description):
						notPresentTextStatus = True

			if not notPresentTextStatus:
				splitString = self.searchTopic.split(' ')
				length = len(splitString)
				counter = 0
				for spt in splitString:
					spt = spt.strip().lower()
					if re.search(re.compile('\\b'+ re.escape(spt) +'\\b'), title) or re.search(re.compile('\\b'+ re.escape(spt) +'\\b'), description):
						counter = counter + 1

				if counter == length:
					searchTopicPresent = True

				if searchTopicPresent:
					presentCounter = 0
					if len(self.relatedSearchObject) != 0:
						for i in range(len(self.relatedSearchObject)):
							splitString = self.relatedSearchObject[i].strip().lower().split(' ')
							
							length = len(splitString)
							counter = 0
							for spt in splitString:
								spt = spt.strip().lower()
								if re.search(re.compile('\\b'+ re.escape(spt) +'\\b'), title) or re.search(re.compile('\\b'+ re.escape(spt) +'\\b'), description):
									counter = counter + 1

							if counter == length:
								presentCounter = presentCounter + 1
					
						return {'status': True, 'reason': 'relativeSearchFound', 'foundNumber': presentCounter, 'relativeArraylength': len(self.relatedSearchObject)}

					else:
						return {'status': True, 'reason': 'noRelativeSearchPresent'}
				else:
					return {'status': False, 'reason': 'notPresent'}
			else:
				return {'status': False, 'reason': 'notbePresentWordPresent'}

		except Exception as e:
			print 'Error : During process title and description !!'
			print e

	def fetchHTML(self, url):
		html = None
		try:
			print "Log : Fetched page content in try block !!"
			browser = mechanize.Browser()
			browser.set_handle_robots(False) # Do not read the robot.txt
			browser.addheaders = [('User-agent', 'Firefox')] # Setting the browser type
			html = browser.open(url).read()

		except Exception as e:
			print "Error : Can't fetch data in try block trying another method !!"
			print e
			try:
				html = urllib.urlopen(url).read()

			except Exception as e:
				print "Error : Can't fetch data in catch block teminating the process !!"
				print e

		return html

	def fetchAllUrls(self, html):
		try:
			jQuery = pq(html) # Parsing html by pyquery

			# Finding navigation links
			navigationUrls = jQuery('.yt-uix-pager[role="navigation"] a[data-link-type="num"]')
			if len(navigationUrls) != 0:
				print "Log : Multi page result, "+ str(len(self.visitedNavigationPageIndex)) +" resuls found!!"
				for navAnchor in navigationUrls:
					navAnchor = jQuery(navAnchor)
					href = navAnchor.attr('href')
					href = urlparse.urljoin(self.baseLink, href).strip()

					parsedCheckingURL = urlparse.parse_qs(urlparse.urlparse(self.checkingUrl).query)
					parsedHrefURL = urlparse.parse_qs(urlparse.urlparse(href).query)

					if int(parsedHrefURL['page'][0]) != int(parsedCheckingURL['page'][0]) and href not in self.fetchNavigationUrls and int(parsedHrefURL['page'][0]) not in self.visitedNavigationPageIndex:
						self.fetchNavigationUrls.append(href)
						self.visitedNavigationPageIndex.append(int(parsedHrefURL['page'][0]))

			else:
				print "Log : Single page result, no navigation found !!"

			# Finding video urls
			allWatchLinks = jQuery('ol.section-list li a')
			if len(allWatchLinks) != 0:
				for watchLink in allWatchLinks:
					watchLink = jQuery(watchLink)
					href = watchLink.attr('href')
					href = urlparse.urljoin(self.baseLink, href).strip()
					if self.baseLink + 'watch' in href and href not in self.linkArray and href not in self.existedLinkArray:
						if watchLink.text().strip().lower() not in self.nameArray:
							self.linkArray.append(href)

			else:
				print 'Log : No video watch link found in this page !!'

		except Exception as e:
			print 'Error : Can\'t parse the html !!'
			print e


# testData = [
# 			{'topic':'ipl live score', 'relatedSearch':'ipl score, rcb vs rr, ipl live, rr vs rcb, live ipl score', 'rowId':313},
# 			{'topic':'10th result', 'relatedSearch':'cbse results, cbse results 2015, 10 result, cbse results 2015 class 10, 10th cbse result', 'rowId':304}
# 		]
# for d in testData:
# 	print d
# 	youtubeData = youtubeDataSearch(d)
# 	youtubeData.run()