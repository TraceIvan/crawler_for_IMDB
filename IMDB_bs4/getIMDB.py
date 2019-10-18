# -*- coding: utf-8 -*-
import re
import urllib.request as urllib2
from bs4 import BeautifulSoup
from myLog.myLog import MyLog as mylog
import os
import requests
import time
from multiprocessing.dummy import Pool as ThreadPool
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

PER_PAGE_OF_IMG=48
PER_PAGE_OF_VIDEO=30
THREADS=8
IS_ORIGINAL_PICTURE=False
MORE_REVIEW,MAX_CLICK=False,3
class Item(object):
    movieName=None
    id=None
    time=None
    genres=None
    imgs=None
    videos=None
    directors=None
    writers=None
    stars=None
    IMDB_Rating=None
    Metascore=None
    Summaries=None
    Synopsis=None
    reviews=None
    releaseinfo=None


class GetIMDB(object):
    def __init__(self,url):
        self.cnt_urls = 0
        self.cnt_pics=0
        self.cnt_videos=0

        self.start_time=time.time()
        self.end_time = self.start_time
        self.url=url
        self.log=mylog('IMDB.log')
        self.st_time='2019-09'
        self.ed_time='2019-09'
        self.urls=self.getUrls(self.st_time,self.ed_time)
        self.spider(self.urls)
        self.pipelines(self.items)




    def getUrls(self,st_time,ed_time):
        urls=[]
        names=self.getNames(st_time,ed_time)
        for name in names:
            urls.append(self.url+name)
        self.log.info("获取urls成功")
        return urls

    def getNames(self,st_time,ed_time):
        names = [st_time]
        st_time=st_time.split('-')
        ed_time=ed_time.split('-')
        st_time=list(map(int,st_time))
        ed_time=list(map(int,ed_time))
        st_year,st_month=st_time[0],st_time[1]
        while st_year<=ed_time[0]:
            st_month=st_month%12+1
            if st_year<ed_time[0] or (st_year==ed_time[0] and st_month<=ed_time[1]):
                names.append('-'.join([str(st_year),str(st_month).zfill(2)]))
            else:
                break
        return names

    def spider(self,urls):
        self.items=[]
        for url in urls:
            htmlContent=self.getResponseContent(url)
            soup=BeautifulSoup(htmlContent,'lxml')
            tagList = soup.find('div', attrs={"class": "list detail"})
            tags = tagList.find_all('div', class_="list_item")

            pool=ThreadPool(processes=THREADS)
            pool.map(self.get_per_movie,tags)
            pool.close()
            pool.join()
            """
            for tag in tags:
                item=Item()
                item.movieName=tag.find('h4').a.get_text().strip()
                id_pattern=re.compile("/title/(.+)/")
                temp=tag.find('h4').a.attrs['href'].strip()
                item.id=id_pattern.search(temp).group(1)
                self.findCurMovie(item)
                self.items.append(item)
                self.log.info("获取%s成功"%item.movieName)
            """

    def get_per_movie(self,tag):
        item = Item()
        item.movieName = tag.find('h4').a.get_text().strip()
        id_pattern = re.compile("/title/(.+)/")
        temp = tag.find('h4').a.attrs['href'].strip()
        item.id = id_pattern.search(temp).group(1)
        self.findCurMovie(item)
        self.items.append(item)
        self.log.info("获取%s成功" % item.movieName)


    def findCurMovie(self,item):
        urlPre=re.search('([a-zA-z]+://\S+?)/',self.url).group(1)
        curUrl='/'.join([urlPre,'title',item.id])
        #imgUrl='/'.join([urlPre,'titile',item.id,'mediaindex'])
        htmlContent=self.getResponseContent(curUrl)
        soup = BeautifulSoup(htmlContent, 'lxml')
        subtext=soup.find('div', attrs={"class":"subtext"})
        item.time = subtext.find('time').get_text().strip()
        item.genres=[]
        genres=subtext.find_all('a')[:-1]
        for genre in genres:
            item.genres.append(genre.get_text().strip())
        item.releaseinfo=subtext.find_all('a')[-1].get_text().strip()

        #1、海报
        #item.imgs.append(soup.find('div',attrs={"class":"poster"}).find('img').attrs['src'])
        imgs=soup.find('div',attrs={"id":"titleImageStrip"})
        imgs=imgs.find('div',attrs={"class":"combined-see-more see-more"})
        if imgs!=None:
            item.imgs=[]
            img_all=imgs.get_text().strip()
            img_nums=int(re.search("(\d+)",img_all).group(1))
            self.getAllimgs(item,curUrl+'/mediaindex',img_nums)
        #2、video
        videos = soup.find('div', attrs={"id": "titleVideoStrip"})
        videos = videos.find('div', attrs={"class": "combined-see-more see-more"})
        if videos!=None:
            item.videos=[]
            video_all = videos.get_text().strip()
            video_nums = int(re.search("(\d+)", video_all).group(1))
            self.getAllvideos(item, curUrl + '/videogallery', video_nums)

        #3、director、writer、stars
        credit_summary_items=soup.find_all('div',attrs={"class":"credit_summary_item"})
        item.directors,item.stars={},{}
        dirs=credit_summary_items[0].find_all('a')
        for i in dirs:
            id=re.search('/name/(.+)/',i.attrs['href'])
            if id == None:
                continue
            else:
                id = id.group(1)
            name=i.get_text().strip()
            item.directors[id]=name
        sts=credit_summary_items[-1].find_all('a')
        for i in sts:
            id = re.search('/name/(.+)/', i.attrs['href'])
            if id == None:
                continue
            else:
                id = id.group(1)
            name = i.get_text().strip()
            item.stars[id]=name
        if len(credit_summary_items)==3:
            wts=credit_summary_items[1].find_all('a')
            item.writers={}
            for i in wts:
                id = re.search('/name/(.+)/', i.attrs['href'])
                if id ==None:
                    continue
                else:
                    id=id.group(1)
                name = i.get_text().strip()
                item.writers[id]=name
        #4、IMDB rating、Metascore
        rating=soup.find('span',attrs={"class":"rating"})
        item.IMDB_Rating=rating.get_text().strip()
        metascore=soup.find('div',class_="metacriticScore")
        if metascore!=None:
            item.Metascore=metascore.get_text().strip()

        #5、Summaries、Synopsis
        self.getSum_Syn(item,'/'.join([curUrl,"plotsummary"]))
        #6、reviews
        self.getReviews(item,'/'.join([curUrl,"reviews"]))

    def getSum_Syn(self,item,url):
        htmlContent = self.getResponseContent(url)
        soup = BeautifulSoup(htmlContent, 'lxml')
        sums=soup.find('ul',attrs={"class":"ipl-zebra-list","id":"plot-summaries-content"})
        if sums!=None:
            sums=sums.find_all('li',attrs={"class":"ipl-zebra-list__item"})
            item.Summaries=[]
            for sum in sums:
                text=sum.p.get_text().strip()
                author=sum.find('div',attrs={"class":"author-container"})
                if author!=None:
                    text+=author.get_text().strip()
                item.Summaries.append(text)
        syns=soup.find('ul',attrs={"class":"ipl-zebra-list","id":"plot-synopsis-content"})
        if syns!=None:
            syns=syns.find_all('li',attrs={"class":"ipl-zebra-list__item"})
            item.Synopsis=[]
            for syn in syns:
                text = syn.get_text().strip()
                item.Synopsis.append(text)

    def getReviews(self,item,url):
        htmlContent = self.getResponseContent(url)
        soup = BeautifulSoup(htmlContent, 'lxml')
        button=soup.find('button',attrs={"id":"load-more-trigger"})

        if MORE_REVIEW and button!=None:
            self.chrome_selenium(item,url)
            return

        reviews=soup.find_all('div',class_="lister-item")
        if reviews!=None:
            item.reviews=[]
            for review in reviews:
                text=""
                rating=review.find('span',attrs={"class":"rating-other-user-rating"})
                if rating!=None:
                    text+=rating.get_text().strip()
                title=review.find('a',attrs={"class":"title"})
                if title!=None:
                    text+=' "'+title.get_text().strip()+'" '
                name_date=review.find('div',attrs={"class":"display-name-date"})
                if name_date!=None:
                    text+='--'+name_date.a.get_text().strip()+','+name_date.find('span',attrs={"class":"review-date"}).get_text().strip()
                text+='\n:'
                content=review.find('div',attrs={"class":"text show-more__control"})
                if content!=None:
                    text+=content.get_text().strip()
                item.reviews.append(text)

    def chrome_selenium(self,item,url):
        self.log.info("selenium:%s"%url)
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        #chrome_options.add_argument('--disable-gpu')
        driver = webdriver.Chrome(executable_path='./chromedriver', chrome_options=chrome_options)
        driver.get(url)
        driver.implicitly_wait(2)
        next = driver.find_elements_by_id('load-more-trigger')
        max_click=MAX_CLICK
        try:
            while max_click > 0:
                if len(next)<1:
                    break
                next=next[0]
                next.click()
                # driver.implicitly_wait(2)
                next = driver.find_elements_by_id('load-more-trigger')
                max_click -= 1
        except:
            self.log.info("selenium over!")
        htmlcontent = driver.page_source
        soup = BeautifulSoup(htmlcontent, 'lxml')
        reviews = soup.find_all('div', class_="lister-item")
        if reviews != None:
            self.reviews=[]
            pool=ThreadPool(processes=THREADS)
            pool.map(self.getEachReview,reviews)
            pool.close()
            pool.join()
            item.reviews=[]
            item.reviews.extend(self.reviews)

    def getEachReview(self,review):
        text = ""
        rating = review.find('span', attrs={"class": "rating-other-user-rating"})
        if rating != None:
            text += rating.get_text().strip()
        title = review.find('a', attrs={"class": "title"})
        if title != None:
            text += ' "' + title.get_text().strip() + '" '
        name_date = review.find('div', attrs={"class": "display-name-date"})
        if name_date != None:
            text += '--' + name_date.a.get_text().strip() + ',' + name_date.find('span', attrs={
                "class": "review-date"}).get_text().strip()
        text += ':\n'
        content = review.find('div', attrs={"class": "text show-more__control"})
        if content != None:
            text += content.get_text().strip()
        self.reviews.append(text)

    def getAllimgs(self,item,url,nums):
        pages=nums//PER_PAGE_OF_IMG
        if nums%PER_PAGE_OF_IMG:
            pages+=1
        page_url=[]
        for i in range(1,pages+1):
            page_url.append(url+'?page='+str(i))
        self.list=[]
        pool=ThreadPool(processes=THREADS)
        pool.map(self.get_curUrl_img,page_url)
        pool.close()
        pool.join()
        item.imgs=[]
        item.imgs.extend(self.list)

        """
        for cur_url in page_url:
            htmlContent = self.getResponseContent(cur_url)
            soup = BeautifulSoup(htmlContent, 'lxml')
            imgs = soup.find('div', attrs={"class": "media_index_thumb_list", "id": "media_index_thumbnail_grid"})
            imgs = imgs.find_all('img')
            for img in imgs:
                img_url = img.attrs['src']
                item.imgs.append(img_url)
        """

    def get_curUrl_img(self, cur_url):
        htmlContent = self.getResponseContent(cur_url)
        soup = BeautifulSoup(htmlContent, 'lxml')
        imgs = soup.find('div', attrs={"class": "media_index_thumb_list", "id": "media_index_thumbnail_grid"})
        imgs = imgs.find_all('img')
        if IS_ORIGINAL_PICTURE:
            orlList=[]
            for img in imgs:
                img_ori_url = "https://www.imdb.com" + img.parent.attrs['href']
                orlList.append(img_ori_url)
            pool=ThreadPool(processes=THREADS)
            pool.map(self.get_ori_imgs,orlList)
            pool.close()
            pool.join()
        else:
            for img in imgs:
                img_url = img.attrs['src']
                self.list.append(img_url)

    def get_ori_imgs(self,img_ori_url):
        htmlContent2 = self.getResponseContent(img_ori_url)
        soup2 = BeautifulSoup(htmlContent2, 'lxml')
        img_ori_url = soup2.find('meta', attrs={"itemprop": "image"}).attrs['content']
        self.list.append(img_ori_url)

    def getAllvideos(self,item,url,nums):
        pages = nums // PER_PAGE_OF_VIDEO
        if nums % PER_PAGE_OF_VIDEO:
            pages += 1
        page_url = []
        for i in range(1, pages + 1):
            page_url.append(url + '?page=' + str(i))
        for cur_url in page_url:
            htmlContent = self.getResponseContent(cur_url)
            soup = BeautifulSoup(htmlContent, 'lxml')
            videos=soup.find('div',attrs={"class":"search-results"})
            videos=videos.find_all('div',attrs={"class":"results-item slate"})
            self.cur_list=[]
            pool=ThreadPool(processes=THREADS)
            pool.map(self.get_cur_video,videos)
            pool.close()
            pool.join()
            item.videos.extend(self.cur_list)
            """
            for video in videos:
                video_url = video.a.attrs['href']
                video_url = "https://www.imdb.com" + video_url
                video_html = self.getResponseContent(video_url)
                video_soup = BeautifulSoup(video_html, "lxml")
                scripts = video_soup.find_all('script')
                text = scripts[-3].get_text()
                urls = re.findall('"videoUrl":"(\S+?)"', text)
                mp4Url = ''
                for url in urls:
                    type = re.search('\.mp4\?', url)
                    if type != None:
                        mp4Url = url
                        break
                realUrl = re.search('(.+)u002Fvi(.+)\\\\u002F(.+?)\Z', mp4Url).groups()
                realUrl = 'https://imdb-video.media-imdb.com/vi' + realUrl[1] + '/' + realUrl[2]
                self.cur_list.append(realUrl)
            """
    def get_cur_video(self,video):
        video_url = video.a.attrs['href']
        video_url = "https://www.imdb.com" + video_url
        video_html = self.getResponseContent(video_url)
        video_soup = BeautifulSoup(video_html, "lxml")
        scripts = video_soup.find_all('script')
        text = scripts[-3].get_text()
        urls = re.findall('"videoUrl":"(\S+?)"', text)
        mp4Url = ''
        for url in urls:
            type = re.search('\.mp4\?', url)
            if type != None:
                mp4Url = url
                break
        realUrl = re.search('(.+)u002Fvi(.+)\\\\u002F(.+?)\Z', mp4Url).groups()
        realUrl = 'https://imdb-video.media-imdb.com/vi' + realUrl[1] + '/' + realUrl[2]
        self.cur_list.append(realUrl)


    def download_videofile(self,video_name, video_link):
        print("视频下载:%s" % video_name)
        cnt=0
        r = requests.get(video_link, stream=True)
        with open(video_name, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
                    cnt+=1
        print("视频下载:%s成功" % video_name)
        return cnt

    def pipelines(self,items):
        self.log.info("total movies:%d"%len(items))
        self.log.info("total urls:%d"%self.cnt_urls)
        self.end_time = time.time()
        total_time = self.end_time - self.start_time
        self.log.info('爬取数据信息及链接完成，总用时： {:.0f}m {:.0f}s'.format(total_time // 60, total_time % 60))
        for item in items:
            route=item.id
            self.route = route
            if not os.path.exists(route):
                os.makedirs(route)
            infoFile='/'.join([route,'info.txt'])
            #1、电影相关信息
            with open(infoFile,'w',encoding='utf8') as fi:
                fi.write('Name:%s\tTime:%s\tGenres:%s\tIMDB_rating:%s'%
                         (item.movieName,item.time,','.join(item.genres),item.IMDB_Rating))
                if item.Metascore!=None:
                    fi.write("\tMetascore:%s"%item.Metascore)
                fi.write("\treleaseInfo:%s\n"%item.releaseinfo)
                tmps=[]
                for id in item.directors:
                    tmps.append(id + '(' + item.directors[id] + ')')
                fi.write("director:%s" % (','.join(tmps)))
                tmps=[]
                if item.writers!=None:
                    for id in item.writers:
                        tmps.append(id + '(' + item.writers[id] + ')')
                    fi.write("\twriters:%s" % (','.join(tmps)))
                    tmps=[]
                for id in item.stars:
                    tmps.append(id+'('+item.stars[id]+')')
                fi.write("\tstars:%s"%(','.join(tmps)))
            img_pre='/'.join([self.route, 'imgs'])
            if not os.path.exists(img_pre):
                os.makedirs(img_pre)
            video_pre='/'.join([self.route, 'videos'])
            if not os.path.exists(video_pre):
                os.makedirs(video_pre)
            self.img_pre=img_pre
            self.video_pre=video_pre
            """
            #2、图片
            if item.imgs!=None:
                ids=list(range(len(item.imgs)))
                pool=ThreadPool(processes=THREADS)
                tmp=list(zip(item.imgs,ids))
                pool.map(self.download_img,tmp)
                pool.close()
                pool.join()
                self.log.info("%s下载img nums(100*100):%d" % (item.movieName, len(item.imgs)))
                self.cnt_pics+=len(item.imgs)
            #3、视频
            if item.videos!=None:
                self.cnt=0
                ids = list(range(len(item.videos)))
                pool = ThreadPool(processes=THREADS)
                tmp = list(zip(item.videos, ids))
                pool.map(self.download_video, tmp)
                pool.close()
                pool.join()
                self.log.info("%s下载video chunk(1024*1024):%d" % (item.movieName, self.cnt))
                self.cnt_videos+=len(item.videos)
            """
            #4、summary
            if item.Summaries!=None:
                file='/'.join([route,'summary.txt'])
                with open(file, 'w', encoding='utf8') as fi:
                    for sum in item.Summaries:
                        fi.write("%s\n\n"%sum)
            #5、Synopsis
            if item.Synopsis!=None:
                file='/'.join([route,'synopsis.txt'])
                with open(file, 'w', encoding='utf8') as fi:
                    for syn in item.Synopsis:
                        fi.write("%s\n\n"%syn)
            #6、reviews
            if item.reviews!=None:
                file='/'.join([route,'review.txt'])
                with open(file, 'w', encoding="utf8") as fi:
                    for review in item.reviews:
                        fi.write("%s\n\n"%review)


            self.log.info("%s写入文件成功"%(item.movieName))
        self.end_time=time.time()
        total_time=self.end_time-self.start_time
        self.log.info("下载图片总数：%d，下载视频总数：%d"%(self.cnt_pics,self.cnt_videos))
        self.log.info('爬取和下载数据完成，总用时： {:.0f}m {:.0f}s'.format(total_time // 60, total_time % 60))

    def download_img(self,data):
        cur_pic_name = '/'.join([self.img_pre, str(data[1] + 1) + '.jpg'])
        urllib2.urlretrieve(data[0], cur_pic_name)
        time.sleep(0.1)
    def download_video(self,data):
        cur_video_name = '/'.join([self.video_pre, str(data[1]+ 1) + '.mp4'])
        self.cnt += self.download_videofile(cur_video_name,data[0])


    def getResponseContent(self,url):
        try:
            response=urllib2.urlopen(url)
        except:
            self.log.error("返回url:%s 失败"%url)
        else:
            self.log.info("返回url:%s 成功"%url)
            self.cnt_urls+=1
            return response.read()

if __name__=="__main__":
    url="https://www.imdb.com/movies-coming-soon/"
    getIMDBinfo=GetIMDB(url)