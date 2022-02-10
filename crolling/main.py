from urllib import request
from bs4 import BeautifulSoup
from selenium import webdriver
from html_table_parser import parser_functions
import pandas as pd
from wics import change_wics


# Selenium 작업
driver = webdriver.Chrome('/Users/choewonjun/Downloads/chromedriver') # 괄호 안에 webdriver.exe 파일 위치를 넣는다.


# csv 파일로 부터 종목코드 추출
df = pd.read_csv('/Users/choewonjun/Documents/coding/crolling/code_num.csv')
df = df['code']

for code in df:
	code = '{:0>6}'.format(code)

	# 종목코드로 해당 주식 정보 페이지 이동
	driver.get(f'https://finance.naver.com/item/coinfo.naver?code={code}&target=finsum_more')
	driver.implicitly_wait(3)
	
	driver.switch_to.frame("coinfo_cp") # iframe 이동

	# Beautiful soup 작업
	req = driver.page_source
	soup = BeautifulSoup(req, 'html.parser')
	tables = soup.find_all('table', {'class' : 'gHead01'}) 
	financial_table = parser_functions.make2d(tables[3])  # 재무제표 표 추출
	financial_df = pd.DataFrame(data=financial_table[1:]) # 필요없는 행 지우기
	name = soup.find('span', 'name').text # 주식 이름
	wics = (soup.find_all('dt', 'line-left')[9].text)[7:] # 주식 WICS(소분류)
	wics = change_wics(wics) # WICS 대분류로 바꾸기
	# print(name, wics)
	financial_df.to_csv(f"/Users/choewonjun/Documents/coding/crolling/financial_df/{code}.csv", encoding="utf-8-sig", index=False, header=True)
	
driver.quit()
