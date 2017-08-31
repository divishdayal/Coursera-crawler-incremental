# Coursera-crawler-incremental

Enviroment setup

      1. Python 2.7 (We recommend to set it up with anaconda)
  
      2. Install the packages specified in requirements.txt
  
	      pip install -r requirements.txt
  
      3. Download Phantomjs <a href="https://bitbucket.org/ariya/phantomjs/downloads/phantomjs-2.1.1-windows.zip"> here</a> for Windows, <a href="https://bitbucket.org/ariya/phantomjs/downloads/phantomjs-2.1.1-macosx.zip"> here</a> for MacOS and <a href="https://bitbucket.org/ariya/phantomjs/downloads/phantomjs-2.1.1-linux-x86_64.tar.bz2"> here</a> for Linux-64 bit( <a href="http://phantomjs.org/download.html"> Other references</a>), install it and add the path to 'phantomjsPath' of config.yml.

# Setup :
Populate the config.yml file :

<1> "UserName" is the username for coursera account;
	
<2> "Password" is the password for coursera account;
	
<3> "UserId" is the ID for every account.
	    
	    First, you have to login using your coursera account on the website. Then press F12, select "Network" and choose XHR. 
	    
	    You can see an API link "https://www.coursera.org/api/openCourseMemberships.v1/?q=findByUser&userId=XXX", your userid is "XXX".
	
<4> "filePath" is the path to save the data you crawled. Make a 'data' folder in the same directory and add the relative path to it here. eg. './data/'
  
        When you login, you will see "My Courses", including "Last Active" and "Inactive".
	
<5> "activeCoursePageNum" is the maximum pages of your "Last Active" courses you want to crawl.
	
<6> "inactiveCoursePageNum" is the maximum pages of your "Inactive" courses you want to crawl.
	
<7> "phantomjsPath" is the executable file path of phantomjs.

<8> "dbPath" is the location of the sqlite3 database file on disk. For fist run - make a blank database.

<9> "debug" : set it to one to see debug messages in the terminal on running the code, otherwise set it to 0.
  
<10> "cookie" :  First sign in to your coursera account on any web browser. Add the cookie after logging into the coursera account using the browser.Instructions to get the cookie variable - http://www.whatarecookies.com/view.asp
  
# Run 
	python incremental_crawler.py

The crawled data will be saved in folders. Every course has a folder named as courseName_courseID_crawlTime(%Y_%m_%d)
