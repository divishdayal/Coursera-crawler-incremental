import crawler_modified
import sqlite3
import yaml
import time
import json,string
import HTMLParser
import pycurl
import math
import StringIO
import certifi
import os
import sys
import requests


def _byteify(data, ignore_dicts = False):
	"""
	To convert any decoded JSON object from using unicode strings to UTF-8-encoded byte strings
	:param data: raw data in json files
	:param ignore_dicts:
	:return: byte string data
	"""
	if isinstance(data, unicode):
		return data.encode('utf-8')
	if isinstance(data, list):
		return [ _byteify(item, ignore_dicts=True) for item in data ]
	if isinstance(data, dict) and not ignore_dicts:
		return {
			_byteify(key, ignore_dicts=True): _byteify(value, ignore_dicts=True)
			for key, value in data.iteritems()
			}
	return data

def json_loads_byteified(json_text):
	return _byteify(
		json.loads(json_text, object_hook=_byteify),
		ignore_dicts=True
	)

def get_new_threads(rootCourseForumId, cookie,cur,fileout,userId):
	"""Get thread information and write into file with json format.
	
	Args:
		rootCourseForumId: An id(String) for constructing the API of threads.
		cookie: the cookie for visiting the course discussion page.
		fileout: the open file for writing thread data.
		userId: id for user account got by analysing the APIs. 
		
	Returns:
		threadsIdList: A list of thread id for constructing the API of posts in the next step.
	"""
	# get all the threads info available(the original post page info)

	#this list is for updating/inserting into the table 
	threadsIdList = []
	count = 0

	threadUrl = 'https://www.coursera.org/api/onDemandCourseForumQuestions.v1/?userId=%s&shouldAggregate=true&includeDeleted=false&sort=lastActivityAtDesc&fields=content%%2Cstate%%2CcreatorId%%2CcreatedAt%%2CforumId%%2CsessionId%%2ClastAnsweredBy%%2ClastAnsweredAt%%2CupvoteCount%%2CfollowCount%%2CtotalAnswerCount%%2CtopLevelAnswerCount%%2CviewCount%%2CanswerBadge%%2CisFlagged%%2CisUpvoted%%2CisFollowing%%2ConDemandSocialProfiles.v1(userId%%2CexternalUserId%%2CfullName%%2CphotoUrl%%2CcourseRole)&includes=profiles&limit=15&q=byCourseForumId&courseForumId=%s' % (userId,rootCourseForumId)
	c = pycurl.Curl()
	b = StringIO.StringIO()

	c.setopt(pycurl.CAINFO, certifi.where())
	c.setopt(c.URL, threadUrl)
	c.setopt(pycurl.CUSTOMREQUEST, 'GET')
	c.setopt(c.WRITEFUNCTION, b.write)
	c.setopt(pycurl.COOKIE, cookie)
	c.perform()
	json_data = b.getvalue()


	threads = json.loads(json_data)['elements']
	for i in range(len(threads)):
		thread = threads[i]

		#some threads don't have 'lastAnsweredAt' fields - creates problems later
		if 'lastAnsweredAt' not in thread:
			thread['lastAnsweredAt'] = ''
		
		#query using threadId(primary key)
		userId,courseId,threadId = thread['id'].split('~')
		query_string = "SELECT lastAnsweredAt FROM thread WHERE threadId = '" + threadId + "';"
		
		if threadId:
			try:
				cur.execute(query_string)
				row = cur.fetchone()
			except Exception,e:
				print e

			if(row is None or thread['lastAnsweredAt'] != row[0]):
				count+=1
				if debug:
					print 'to be updated, threadId : ', threadId
				threadsIdList.append(thread['id'])
				fileout.write(json.dumps(thread, encoding='utf-8'))
				fileout.write('\n')
	
	b.close()
	c.close()

	#------ same process above for rest of the posts {pagination} ---------------
	#----------------------------------------------------------------------------

	totalThreadsCount = json.loads(json_data)['paging']['total']
	total_num = totalThreadsCount
	loop_num = int(math.floor((total_num - 1) / 15))
	for i in range(1,loop_num+1,1):
		start_page = (i) * 15
		new_threadURL = threadUrl + '&start=%d' % start_page
		# get all the posts left
		c = pycurl.Curl()
		b = StringIO.StringIO()
		c.setopt(pycurl.CAINFO, certifi.where())
		c.setopt(c.URL, new_threadURL)
		c.setopt(pycurl.CUSTOMREQUEST, 'GET')
		c.setopt(c.WRITEFUNCTION, b.write)
		c.setopt(pycurl.COOKIE, cookie)
		c.perform()
		c.setopt(pycurl.COOKIE, cookie)
		json_data = b.getvalue()
		b.close()
		c.close()
		threads = json.loads(json_data)['elements']
		for i in range(len(threads)):
			thread = threads[i]
			if 'lastAnsweredAt' not in thread:
				thread['lastAnsweredAt'] = ''

			cur = conn.cursor()
			userId,courseId,threadId = thread['id'].split('~')
			if threadId:
				cur.execute("SELECT lastAnsweredAt from thread where threadId = '%s'" % threadId)
				row = cur.fetchone()


				if(row is None or thread['lastAnsweredAt'] != row[0]):
					count += 1
					if debug:
						print 'to be updated, threadId : ', threadId
					threadsIdList.append(thread['id'])
					fileout.write(json.dumps(thread, encoding='utf-8'))
					fileout.write('\n')
		
		
		b.close()
		c.close()

	if debug:
			print 'total threads to be updated - ', count

	#returning list of threadId's to be crawled for posts/users
	return threadsIdList

#writes/updates the thread table in the database
def write_to_sql_thread(cursor, fileout):
	"""
		cursor - the cursor from the database to make database operations - select/update
		fileout - json file from which the threads are read and then inserted into the database
	"""
	fileout.seek(0,0)
	#original cols in database
	cols_in_database = ['answerBadge','hasResolved','instReplied','totalAnswerCount','isFollowing', 'forumQuestionId' ,'lastAnsweredAt', 'topLevelAnswerCount', \
	'isFlagged','lastAnsweredBy','state','followCount' , 'title','content','viewCount','sessionId', 'forumId', 'creatorId','isUpvoted', \
						'id','courseId','threadId','createdAt','upvoteCount']

	#to be used when iterating in the for loop below(keys in needed_cols_in_database)
	needed_cols_in_database = ['answerBadge','totalAnswerCount','isFollowing', 'forumQuestionId' ,'lastAnsweredAt', 'topLevelAnswerCount', \
	'isFlagged','lastAnsweredBy','state','followCount' , 'content','viewCount','sessionId', 'forumId', 'creatorId','isUpvoted', \
						'id','createdAt','upvoteCount']
	for eachline in fileout:
		data = json.loads(json.dumps(eachline))
		data = json_loads_byteified(data)
		someitem = data.iterkeys()
		columns = list(someitem)
		cols_notin_data = list(set(columns)^set(cols_in_database))
		for i in xrange(len(cols_notin_data)):
			if cols_notin_data[i] == 'courseId':
				pass
			elif cols_notin_data[i] == 'threadId':
				pass
			elif cols_notin_data[i] == 'title':
				pass
			elif cols_notin_data[i] == 'hasResolved':
				pass
			elif cols_notin_data[i] == 'instReplied':
				pass
			else:
				data[cols_notin_data[i]] = ''
		someitem = data.iterkeys()
		columns = list(someitem)
		query = 'insert or replace into thread values (?{1})'
		query = query.format(",".join(cols_in_database),",?" * (len(cols_in_database) - 1))
		temp = []


		for keys in needed_cols_in_database:
			if keys == 'answerBadge':
				if data['answerBadge'] == {}:
					temp.append('')
					temp.append(0)
					temp.append(0)
				else:
					temp.append(data['answerBadge']['answerBadge'])
					if data['answerBadge']['answerBadge'] == 'MENTOR_RESPONDED':
						temp.append(1)
						temp.append(1)
					elif data['answerBadge']['answerBadge'] == 'INSTRUCTOR_RESPONDED':
						temp.append(1)
						temp.append(1)
					elif data['answerBadge']['answerBadge'] == 'STAFF_RESPONDED':
						temp.append(1)
						temp.append(1)
					else:
						temp.append(0)
						temp.append(0)
			elif keys == 'courseId':
				pass
			elif keys == 'userId':
				pass
			elif keys == 'content':
				temp.append(data['content']['question'])
				temp.append(data['content']['details']['definition']['value'])
			elif keys == 'isFlagged':
				if data['isFlagged'] == False:
					temp.append(0)
				else:
					temp.append(1)
			elif keys == 'isFollowing':
				if data['isFollowing'] == False:
					temp.append(0)
				else:
					temp.append(1)
			elif keys == 'state':
				if data['state'] == {}:
					temp.append('')
				else:
					temp.append('edited')
			elif keys == 'id':
				id_str = data[keys]
				userId,courseId,threadId = id_str.split('~')
				new_str = courseId + '~' + threadId
				temp.append(new_str)
				temp.append(courseId)
				temp.append(threadId)
			else:
				temp.append(data[keys])
		values = tuple(temp)
		cursor.execute(query,values)
	conn.commit()


#writes/updates the post table in the database
def write_to_sql_post(cur, fileout):
	"""
		cursor - the cursor from the database to make database operations - select/update
		fileout - json file from which the threads are read and then inserted into the database
	"""
	fileout.seek(0,0)
	cols_in_database = ['parentForumAnswerId','forumQuestionId', 'isFlagged','order','content','state','childAnswerCount','creatorId','isUpvoted', \
						'id','courseId','postId','createdAt','upvoteCount']
	for eachline in fileout:
		data = json.loads(json.dumps(eachline))
		data = json_loads_byteified(data)
		someitem = data.iterkeys()
		columns = list(someitem)
		cols_notin_data = list(set(columns)^set(cols_in_database))

		# check if any field in the database is not present in the read line in json file
		cols_notin_data = list(set(columns)^set(cols_in_database))
		for i in xrange(len(cols_notin_data)):
			if cols_notin_data[i] == 'courseId':
				pass
			elif cols_notin_data[i] == 'postId':
				pass
			elif cols_notin_data[i] == 'userId':
				pass
			elif cols_notin_data[i] == 'forumAnswerId':
				pass
			else:
				data[cols_notin_data[i]] = ''
		someitem = data.iterkeys()
		columns = list(someitem)

		query = 'insert or replace into post values (?{1})'
		query = query.format(",".join(cols_in_database),",?" * (len(cols_in_database) - 1))
		temp = []
		for keys in data.iterkeys():
			if keys == 'content':
				temp.append(data['content']['definition']['value'])
			elif keys == 'isFlagged':
				if data['isFlagged'] == False:
					temp.append(0)
				else:
					temp.append(1)
			elif keys == 'isUpvoted':
				if data['isUpvoted'] == False:
					temp.append(0)
				else:
					temp.append(1)
			elif keys == 'state':
				if data['state'] == {}:
					temp.append('')
				else:
					temp.append('edited')
			elif keys == 'id':
				id_str = data[keys]
				userId,courseId,postId = id_str.split('~')
				new_str = courseId + '~' + postId
				temp.append(new_str)
				temp.append(courseId)
				temp.append(postId)
			elif keys == 'userId':
				continue
			elif keys == 'courseId':
				continue
			elif keys == 'forumAnswerId':
				continue
			else:
				temp.append(data[keys])
		values = tuple(temp)
		cur.execute(query,values)
	conn.commit()

#writes/updates the user table in the database
def write_to_sql_user(cur, fileout):
	"""
		cursor - the cursor from the database to make database operations - select/update
		fileout - json file from which the threads are read and then inserted into the database
	"""
	fileout.seek(0,0)
	cols_in_database = ['photoUrl','courseId','userId','id','learnerId','courseRole','fullName','externalUserId']
	for eachline in fileout:
		data = json.loads(json.dumps(eachline))
		data = json_loads_byteified(data)
		someitem = data.iterkeys()
		columns = list(someitem)
		if len(columns) != len(cols_in_database):
			cols_notin_data = list(set(columns)^set(cols_in_database))
			for i in xrange(len(cols_notin_data)):
				data[cols_notin_data[i]] = ''
			someitem = data.iterkeys()
			columns = list(someitem)
		else:
			pass
		query = 'insert or replace into user values (?{1})'
		query = query.format(",".join(columns), ",?" * (len(columns) - 1))
		temp = []
		for keys in data.iterkeys():
			temp.append(data[keys])
		values = tuple(temp)
		cur.execute(query,values)
	conn.commit()

if __name__ == "__main__":
	#an instance of the crawler(imported)
	scraper = crawler_modified.CourseraScraper()


	#opening database using path defined in config.yml file 
	with open('config.yml') as f:
		config = yaml.load(f) 

	db_path = config['dbPath']
	userId = config['UserId']
	filePath = config['filePath']
	debug = config['debug']

	conn = sqlite3.connect(db_path)
	conn.text_factory = str
	cur = conn.cursor()


	#adding the cookie for signing into coursera for the wing.nus account.
	scraper.driver.add_cookie({
		'domain': '.coursera.org',
		'name': 'CAUTH',
		'value': '',
		'path': '/', 
		'expires': None
	})

	if debug:
		print 'starting to crawl'
	try:

		#For active courses -------------------------------------------------------------------------------------
		#--------------------------------------------------------------------------------------------------------
		if debug:
			print 'getting active course list...'

		#get active course names and id's
		activeCoursePageNum = config['activeCoursePageNum']
		active_course_ids, course_id_and_name = scraper.get_active_courses(activeCoursePageNum)
		if debug:
			print 'Got active course list...'

		#crawling the active courses
		for i in range(len(active_course_ids)):
			if (active_course_ids[i] == 'Bkx-PB00Eea0YQ7Ij7lJXw') :
				if debug:
					print 'Getting active courses!Total:%d...Processing:%d' % (len(active_course_ids),i+1)
				courseId = active_course_ids[i]
				courseName = course_id_and_name[courseId]
				if debug : 
					print 'crawling course - ', courseName

				#files to store the retrieved information for the courses in json format
				newFilePath = filePath + str(courseName) + '_' + str(courseId) + '_' + str(time.strftime('%Y_%m_%d')) + '/'
				isExists = os.path.exists(newFilePath)
				if not isExists:
					os.mkdir(newFilePath)
				file1 = newFilePath + 'threads.json'
				fileoutThreads = open(file1,'w+')
				file2 = newFilePath + 'posts.json'
				fileoutPosts = open(file2,'w+')
				file3 = newFilePath + 'users.json'
				fileoutUsers = open(file3,'w+')
				
				#getting cookie and other information to crawl for course info
				cookie = scraper.get_cookie(courseName)
				rootCourseForumId = scraper.get_courseforum_id(courseId,courseName,cookie=cookie)

				#The thread id's in the list are the ones that are updated/new - which need to be crawled by the crawler
				threadsIdList = get_new_threads(rootCourseForumId,cookie,cur, fileout=fileoutThreads,userId=userId)
				if debug:
					print 'got the threadsIdList to be updated'

				#writing to thread table in the databse
				write_to_sql_thread(cur, fileout = fileoutThreads)
				if debug: 
					print 'inserted into table - threads'

				#getting user/post information for each of the updated/new threads in the threadIdList
				for j in range(len(threadsIdList)):
					threadId = threadsIdList[j]
					post = scraper.get_posts(threadId,cookie,fileoutPost=fileoutPosts,fileoutUser=fileoutUsers)

				#writing to post table in the database
				write_to_sql_post(cur, fileout = fileoutPosts)
				if debug:
					print 'inserted into table - posts'

				#writing to sql table in the database
				write_to_sql_user(cur, fileout = fileoutUsers)
				if debug:
					print 'inserted into table - users'

				#closing the file pointers
				fileoutThreads.close()
				fileoutPosts.close()
				fileoutUsers.close()

		# For inactive courses -----------------------------------------------------------------------------------
		# --------------------------------------------------------------------------------------------------------
		inactiveCoursePageNum = config['inactiveCoursePageNum']
		inactive_course_ids, course_id_and_name = scraper.get_inactive_courses(inactiveCoursePageNum)
		for i in range(range(len(inactive_course_ids))):
			if debug:
				print 'Getting inactive courses!Total:%d...Processing:%d' % (len(active_course_ids),i+1)
			courseId = active_course_ids[i]
			courseName = course_id_and_name[courseId]

			newFilePath = filePath + str(courseName) + '_' + str(courseId) + '_' + str(time.strftime('%Y_%m_%d')) + '/'
			isExists = os.path.exists(newFilePath)
			if not isExists:
				os.mkdir(newFilePath)
			file1 = newFilePath + 'threads.json'
			fileoutThreads = open(file1,'w+')
			file2 = newFilePath + 'posts.json'
			fileoutPosts = open(file2,'w+')
			file3 = newFilePath + 'users.json'
			fileoutUsers = open(file3,'w+')

			cookie = scraper.get_cookie(courseName)
			rootCourseForumId = scraper.get_courseforum_id(courseId,courseName,cookie=cookie)
			threadsIdList = get_new_threads(rootCourseForumId,COOKIEie,cur, fileout=fileoutThreads,userId=userId)
			if debug:
				print 'got the threadsIdList to be updated'
			write_to_sql_thread(cur, fileout = fileoutThreads)
			if debug: 
				print 'inserted into table - threads'
			for j in range(len(threadsIdList)):
				threadId = threadsIdList[j]
				post = scraper.get_posts(threadId,cookie,fileoutPost=fileoutPosts,fileoutUser=fileoutUsers)
			write_to_sql_post(cur, fileout = fileoutPosts)
			if debug:
				print 'Successfully get all the posts!'
			fileoutThreads.close()
			fileoutPosts.close()
			fileoutUsers.close()
		


	except Exception,e:
		print e
		conn.close()
		scraper.driver.close()
		scraper.driver.quit()
	conn.close()
	scraper.driver.close()
	scraper.driver.quit()