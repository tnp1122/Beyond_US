import requests
from bs4 import BeautifulSoup
from requests_html import HTMLSession
import pandas as pd
import time
import sys


class Ppom:
    def __init__(self, id='info_ico'):
        self.id = id
        self.FileName = 'data\Ppom_' + self.id[0].upper() + self.id[1:] + '.csv'
        self.url = 'https://www.ppomppu.co.kr/zboard/zboard.php?id='
        self.aurl = 'http://m.ppomppu.co.kr/new/bbs_view.php?id='
        self.base_url = self.url + self.id + '&no='
        #self.header_m = {'User-Agent': 'Mozilla/5.0 (Linux; U; Android 4.0.3; ko-kr; LG-L160L Build/IML74K) AppleWebkit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30'}
        self.size = 100     # size of chunk
        self.usize = 10     # 실시간 업데이트용 청크 사이즈
        self.html = requests.get(self.base_url)
        self.bs = BeautifulSoup(self.html.content, 'lxml')
        self.first0 = self.bs.find('tr', class_='list0').find('td', class_='eng list_vspace')
        self.first1 = self.bs.find('tr', class_='list1').find('td', class_='eng list_vspace')
        if self.first0.text > self.first1.text:
            self.from_num = int(self.first0.text)
        else:
            self.from_num = int(self.first1.text)
        self.data = pd.DataFrame()
        self.end_check = False
        try:
            self.data = pd.read_csv(self.FileName, index_col=['Unnamed: 0'])
        except:
            pass

    def run(self):
        self.base_url = self.aurl + self.id
        if not self.data.empty:
            self.update_data(self.size)
            print("최신 데이터 업데이트 완료")
        print("crawling to end page")
        self.save_end_data()
        self.update_data(self.size)

        self.base_url = self.url + self.id
        print("실시간 업데이트 작동")
        while self.end_check:
            time.sleep(30)
            self.first0 = self.bs.find('tr', class_='list0').find('td', class_='eng list_vspace')
            self.first1 = self.bs.find('tr', class_='list1').find('td', class_='eng list_vspace')
            if self.first0.text > self.first1.text:
                self.from_num = int(self.first0.text)
            else:
                self.from_num = int(self.first1.text)
            self.update_data(self.usize)

    def search_data(self, from_num, size):
        contents = []
        index = []
        for n in range(from_num - size + 1, from_num+1):
            html = requests.get(self.base_url + '&no=' + str(n))
            if html.status_code == 200:
                bs = BeautifulSoup(html.content, 'lxml')
                if bs.find('script').text != 'alert("존재하지 않는 글입니다.")':
                    view = bs.find('div', class_='bbs view')
                    if view:
                        article = view.find('div', class_='cont')
                        if article:
                            title = view.find('h4').text.split('\n')[1].strip()
                            nick = view.find('div', class_='info').find_all('a')[0].text.strip()
                            date = view.find('span', class_='hi').text[-16:]
                            cr = view.find('div', class_='info').text.replace(' ', '').split(':')
                            count = cr[2][:-3]
                            recommend = cr[3][:2].strip()
                            href = article.find_all('a', class_='noeffect')
                            comment = len(href)
                            text = article.text.replace('\n', '')
                            if len(href) > 0:
                                text += '본문 내 하이퍼 링크: '
                                for m in range(comment):
                                    text += href[m]['href'] + ', '

                            comnames = []
                            comments = []
                            if bs.find('div', class_='cmAr'):
                                comnames = bs.find('div', class_='cmAr').find_all('h6', class_='com_name')
                                comments = bs.find('div', class_='cmAr').find_all('div', class_='comment_memo')
                            com = []
                            for m in range(len(comments)):
                                com.append(comnames[m].find('span').text.strip() + ' : ' + comments[m].text.strip())

                            '''print(self.base_url + '&no=' + str(n))
                            print(title)
                            print(nick)
                            print(date)
                            print(count)
                            print(recommend)
                            print(comment)
                            print(text)
                            print(com)'''

                            contents.append([self.base_url + str(n), title, nick, date, int(count), int(recommend), int(comment), text, com])
                            index.append(n)
            sys.stdout.write(f"\rcrawl... {n} to {from_num}")
            time.sleep(0.3)

        sys.stdout.write('\r')
        sys.stdout.flush()

        df = pd.DataFrame(contents, index=index, columns=['주소', '제목', '닉네임', '작성시간', '조회수', '추천수', '댓글수', '내용', '댓글'])
        return df

    def update_data(self, size):  # 마지막으로 저장된 글을 size 개수 만큼 업데이트 후 최신글까지 업데이트
        index = self.data.index.tolist()
        gap = self.from_num - index[0]
        chunk_n = int((gap - gap % size) / size)
        for n in range(chunk_n):
            self.data = pd.concat([self.data, self.search_data(index[0] + n * size, size)]).drop_duplicates(subset=['주소'], keep='last').sort_index(ascending=False)
            self.data.to_csv(self.FileName, encoding='utf-8-sig')
        self.data = pd.concat([self.data, self.search_data(self.from_num, 2 * size)]).drop_duplicates(subset=['주소'], keep='last').sort_index(ascending=False)
        self.data.to_csv(self.FileName, encoding='utf-8-sig')
        self.from_num = self.data.index.tolist()[-1]

    def save_end_data(self):
        n = 0
        while (self.from_num - n * self.size) > self.size:
            self.data = pd.concat([self.data, self.search_data(self.from_num - n * self.size, self.size)]).drop_duplicates(subset=['주소'], keep='last').sort_index(ascending=False)
            self.data.to_csv(self.FileName, encoding='utf-8-sig')
            n += 1
        if self.from_num - n * self.size < self.size:
            num = self.from_num - n * self.size
            self.data = pd.concat([self.data, self.search_data(num, num)]).drop_duplicates(subset=['주소'], keep='last').sort_index(ascending=False)
            self.data.to_csv(self.FileName, encoding='utf-8-sig')
            self.end_check = True
            print("마지막 데이터 업데이트 완료")

    def get_data(self, prt=False):
        if prt:
            print(self.data)
        return self.data


if __name__ == '__main__':
    process = Ppom()
    process.run()