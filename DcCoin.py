import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import sys

FileName = 'data.csv'

class DcCoin:
    def __init__(self):
        self.base_url_m = "https://m.dcinside.com/board/coin/"
        self.header_m = {
            'User-Agent': 'Mozilla/5.0 (Linux; U; Android 4.0.3; ko-kr; LG-L160L Build/IML74K) AppleWebkit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30'}
        self.size = 10  # size of chunk
        self.html = requests.get(self.base_url_m, headers=self.header_m)
        self.bs = BeautifulSoup(self.html.content, 'lxml')
        self.first = self.bs.find('div', class_='gall-detail-lnktb').find('a', class_='lt')['href']
        self.from_num = int(self.first[len(self.base_url_m):])
        self.data = pd.DataFrame()
        try:
            self.data = pd.read_csv(FileName, index_col=['Unnamed: 0'])
        except:
            pass

    def run(self):
        if not self.data.empty:
            self.update_data()
            print("최신 데이터 업데이트 완료")

        print("crawling to end page")
        self.save_end_data()

    def search_data(self, from_num, size):
        contents = []
        index = []
        for n in range(from_num - size + 1, from_num+1):
            html = requests.get(self.base_url_m + str(n), headers=self.header_m)
            if html.status_code == 200:
                bs = BeautifulSoup(html.content, 'lxml')
                title = bs.find('span', class_='tit').text.strip()
                nick = bs.find('ul', class_='ginfo2').find_all('li')[0].text
                date = bs.find('ul', class_='ginfo2').find_all('li')[1].text
                article = bs.find('div', class_='gall-thum-btm-inner')
                count = article.find('ul', class_='ginfo2').find_all('li')[0].text[4:]
                recommend = article.find('ul', class_='ginfo2').find_all('li')[1].text[3:]
                comment = article.find('ul', class_='ginfo2').find_all('li')[2].text[3:]
                text = article.find('div', class_='thum-txtin').text.replace('\n', '')
                comments = bs.find('ul', class_='all-comment-lst')
                com = ''
                if comments:
                    for tag in comments.find_all('li'):
                        if tag.find('a'):   # 댓글 삭제된경우 'a'(작성자) 태그가 없음
                            com = f"{com}{tag.find('a').text} : {tag.find('p').text}\t"
                contents.append([self.base_url_m + str(n), title, nick, date, int(count), int(recommend), int(comment), text, com])
                index.append(n)
            sys.stdout.write(f"\rcrawl... {n} to {from_num}")
            time.sleep(0.3)

        sys.stdout.write('\r')
        sys.stdout.flush()

        df = pd.DataFrame(contents, index=index, columns=['주소', '제목', '닉네임', '작성시간', '조회수', '추천수', '댓글수', '내용', '댓글'])
        return df

    def update_data(self):  # 마지막으로 저장된 글을 size 개수 만큼 업데이트 후 최신글까지 업데이트
        index = self.data.index.tolist()
        gap = self.from_num - index[0]
        chunk_n = int((gap - gap % self.size) / self.size)
        for n in range(chunk_n):
            self.data = pd.concat([self.data, self.search_data(index[0] + n * self.size, self.size)]).drop_duplicates(subset=['주소'], keep='last').sort_index(ascending=False)
            self.data.to_csv('data.csv', encoding='utf-8-sig')
        self.data = pd.concat([self.data, self.search_data(self.from_num, 2 * self.size)]).drop_duplicates(subset=['주소'], keep='last').sort_index(ascending=False)
        self.data.to_csv('data.csv', encoding='utf-8-sig')
        self.from_num = self.data.index.tolist()[-1]

    def save_end_data(self):
        n = 0
        while (self.from_num - n * self.size) > self.size:
            self.data = pd.concat([self.data, self.search_data(self.from_num - n * self.size, self.size)]).drop_duplicates(subset=['주소'], keep='last').sort_index(ascending=False)
            self.data.to_csv('data.csv', encoding='utf-8-sig')
            n += 1
        if self.from_num - n * self.size < self.size:
            num = self.from_num - n * self.size
            self.data = pd.concat([self.data, self.search_data(num, num)]).drop_duplicates(subset=['주소'], keep='last').sort_index(ascending=False)
        self.data.to_csv('data.csv', encoding='utf-8-sig')

    def get_data(self, prt=False):
        if prt:
            print(self.data)
        return self.data


if __name__ == '__main__':
    dccoin = DcCoin()
    dccoin.run()