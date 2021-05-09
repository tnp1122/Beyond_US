import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import sys

base_url_m = "https://m.dcinside.com/board/coin/"
header_m = {'User-Agent': 'Mozilla/5.0 (Linux; U; Android 4.0.3; ko-kr; LG-L160L Build/IML74K) AppleWebkit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30'}
size = 100  # size of chunk


def search_data(from_num, size=size):
    contents = []
    index = []
    for n in range(from_num - size + 1, from_num+1):
        html = requests.get(base_url_m + str(n), headers=header_m)
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
            contents.append([n, base_url_m + str(n), title, nick, date, int(count), int(recommend), int(comment), text, com])
            index.append(n)
        sys.stdout.write(f"\rcrawl... {n} to {from_num}")
        time.sleep(0.3)

    sys.stdout.write('\r')
    sys.stdout.flush()

    df = pd.DataFrame(contents, index=index, columns=['순번', '주소', '제목', '닉네임', '작성시간', '조회수', '추천수', '댓글수', '내용', '댓글'])
    return df


def update_data(data):
    index = data.index.tolist()
    gap = from_num - index[0]
    data = pd.concat([data, search_data(from_num, gap + 10)]).drop_duplicates(['순번'], keep='last').sort_values('순번', ascending=False)
    data.to_csv('data.csv', encoding='utf-8-sig')
    return data


def get_end_data(data):
    n = 0
    while (from_num - n * size) > 100:
        data = pd.concat([data, search_data(from_num - n * size)]).drop_duplicates(['순번'], keep='last').sort_values('순번',ascending=False)
        data.to_csv('data.csv', encoding='utf-8-sig')
        n += 1
    if from_num - n * size < size:
        num = from_num - n * size
        data = pd.concat([data, search_data(num, num)]).drop_duplicates(['순번'], keep='last').sort_values('순번',ascending=False)
        data.to_csv('data.csv', encoding='utf-8-sig')
    return data


if __name__ == '__main__':
    html = requests.get(base_url_m, headers=header_m)
    bs = BeautifulSoup(html.content, 'lxml')
    first = bs.find('div', class_='gall-detail-lnktb').find('a', class_='lt')['href']
    from_num = int(first[len(base_url_m):])

    data = pd.DataFrame()
    try:
        data = pd.read_csv('data.csv', index_col=['Unnamed: 0'])
    except:
        pass

    if not data.empty:
        data = update_data(data)
        from_num = data.index.tolist()[-1]
        print("최신 데이터 업데이트 완료")

    print("crawling to end page")
    data = get_end_data(data)

# print(data)