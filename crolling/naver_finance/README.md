# 재무제표 크롤링
네이버 증권 사이트를 기준으로 재무제표를 추출하는 코드입니다.

코드 실행시 나오는 csv 파일들은 다음과 같은 폴더 안에 압축하여 저장되어 있습니다.
whole_df : 전체(요약) 재무제표
yearly_df : 연간 재무제표

# Requirements
## 라이브러리
다음과 같은 라이브러리가 설치되어 있어야 합니다.
설치하기 전에 
``` 
pip install --upgrade pip 
``` 
를 통해 pip 를 업데이트하는 것을 추천합니다.

* selenium
	```
	pip install selenium
	```
* beautifulsoup
	```
	pip install bs4
	```
* html_table_parser
	```
	pip install html_table_parser
	```
## Chrome
최신 버전의 Chrome 이 필요합니다.
<img width="80%" src="https://bakey-api.codeit.kr/api/files/resource?root=static&seqId=3955&directory=Screenshot%202020-11-24%20at%2010.08.35%20PM.png&name=Screenshot+2020-11-24+at+10.08.35+PM.png"/>


크롬 브라우저 우측 상단에 메뉴 버튼을 클릭한 다음 Help → About Google Chrome (도움말 → Chrome 정보)을 클릭하시면 크롬 버전을 확인하실 수 있습니다.
<img width="80%" src="https://bakey-api.codeit.kr/api/files/resource?root=static&seqId=3955&directory=Screenshot%202020-11-24%20at%2010.10.34%20PM.png&name=Screenshot+2020-11-24+at+10.10.34+PM.png"/>

## Webdriver
https://chromedriver.chromium.org/downloads 
위 사이트에서 크롬 버전에 맞는 웹드라이버를 설치해야합니다.
본인 환경에 맞는 웹드라이버를 설치한 뒤에 압축을 풀고 chromedriver 라는 파일의 파일 경로를 복사하여 main.py 에서
```
# Selenium 작업
driver = webdriver.Chrome('') # 괄호 안에 webdriver.exe 파일 위치를 넣는다.
```
이 부분에서 괄호 안에 파일 경로를 넣어줍니다. ex) driver = webdriver.Chrome('/Users/me/Downloads/chromedriver')
### Mac은 chromedriver를 열면 아래와 같은 오류가 날 수 있습니다.
<img width="80%" src="https://bakey-api.codeit.kr/api/files/resource?root=static&seqId=3955&directory=Screenshot%202020-11-25%20at%201.00.14%20PM.png&name=Screenshot+2020-11-25+at+1.00.14+PM.png"/>


그렇다면 설정 메뉴로 가셔서 Security & Privacy (보안 및 개인 정보 보호) 아이콘을 클릭해 주세요.
<img width="80%" src="https://bakey-api.codeit.kr/api/files/resource?root=static&seqId=3955&directory=Screenshot%202020-11-25%20at%201.01.29%20PM.png&name=Screenshot+2020-11-25+at+1.01.29+PM.png"/>


그런 다음 아래 Open Anyway (확인 없이 열기) 버튼을 눌러주세요.
<img width="80%" src="https://bakey-api.codeit.kr/api/files/resource?root=static&seqId=3955&directory=Screenshot%202020-11-25%20at%201.03.52%20PM.png&name=Screenshot+2020-11-25+at+1.03.52+PM.png"/>

아래와 같이 chromedriver가 잘 실행되면 셋업이 끝난 겁니다.
<img width="80%" src="https://bakey-api.codeit.kr/api/files/resource?root=static&seqId=3955&directory=Screenshot%202020-11-25%20at%201.08.50%20PM.png&name=Screenshot+2020-11-25+at+1.08.50+PM.png"/>

# Usage
## main.py 에서 아래와 같은 작업들을 해준다.

std 변수에서 원하는 재무제표 기준을 입력한다.
0 : 전체(요약) 정보
1 : 연간 정보
2 : 분기 정보

```
# csv 파일로 부터 종목코드 추출
df = pd.read_csv('cur_comp_info.csv 파일 경로 입력', encoding='cp949')
df = df['Symbol']
```
위의 read_csv 에 괄호 안에 cur_comp_info.csv 파일의 경로를 넣어준다. 
ex) /Users/choewonjun/Downloads/cur_comp_info.csv

```
# 저장 경로 입력
financial_df.to_csv(f"저장경로/{code}.csv", encoding="utf-8-sig", index=False, header=True)
```
위의 괄호 안에 저장경로를 입력한다. 즉, 종목코드 csv 파일들이 저장될 위치를 지정한다. csv 파일들은 종목 코드 이름으로 저장되고 저장경로/000020.csv 와 같이 저장이 된다.
ex) financial_df.to_csv(f"/Users/choewonjun/Documents/coding/crolling/yearly_df/{code}.csv", encoding="utf-8-sig", index=False, header=True)

위와 같이 입력이 끝냈으면 main.py 에서 코드를 실행하면 된다.