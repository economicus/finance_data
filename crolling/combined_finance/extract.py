import pandas as pd
import time
from exception import *

def read_code_csv(path):
	df = pd.read_csv(path)
	codes = df['Symbol']
	comp_id = df['ID']
	return (codes, comp_id)


def id_to_code(codes, path):
	code_list = []
	df = pd.read_csv(path)
	for i in codes:
		code = df[df['ID'] == int(i)]['Symbol']
		code_list.append(int(code))
	return code_list
	

def for_make_yyyy_mm_dd(x):
	if pd.isna(x) or x == 'nan' or x == None:
		return None
	if '.0' in x:
		x.replace('.0', '')
	return x + '-12-31'


def union_structure(save_path):
	""" csv 구조를 통일하는 함수 """
	codes = read_code_csv('/Users/choewonjun/Documents/coding/crolling')
	for code in codes:
		code = '{:0>6}'.format(code)
		print(code)
		save_path = '/Users/choewonjun/Documents/coding/crolling/finished'
		df = pd.read_csv(f'{save_path}/{code}.csv', encoding='cp949')
		if 'Unnamed: 0' in df.columns:
			l = list(df.columns)
			splice_idx = l.index('Unnamed: 0')
			new_df = df.iloc[:,splice_idx + 1:]
			new_df.to_csv(f'{save_path}/{code}.csv', encoding='cp949', index=False)


def convert_from_naver_to_git(naver_path, code):
	""" 크롤링된 네이버 csv 파일을 git 재무제표 형식으로 변환하는 함수 """
	
	n_df = pd.read_csv(f'{naver_path}/{code}.csv')
	no_december = ['169330', '190650' ,'357120', '950210', '338100', '365550', '334890'] # 실적발표가 12월이 아닌 회사
	datas = []
	count = 0
	for i in range(1, len(n_df.columns)):
		for j in range(1, len(n_df)):
			if pd.isna(n_df.iat[j, i]):
				n_df.iat[j, i] = '0'
	for i in n_df.columns[1:]:
		if pd.isna(n_df.loc[0, i]):
			data = [0] * 50
			datas.append(data)
			count += 1
			continue
		if int(n_df.loc[0, i].split('/')[1][:2]) != 12 or 'E' in n_df.loc[0, i]:
			if code in no_december and 'E' not in n_df.loc[0, i]: # 예외처리 (실적이 12월에 안나는 회사)
				pass
			else:
				continue
		# print(len(extract_from_naver(i, n_df)))
		datas.append(extract_from_naver(i, n_df))
		count += 1
	cols = ['구분', '자산', '유동자산', '비유동자산', '기타자산', '부채', '유동부채',
		'비유동부채', '자본', '주당액면가액', '발행한주식총수', '보통주', '우선주', '매출액', '매출원가',
		'매출총이익', '판매비와관리비', '영업이익', '법인세차감전순이익', '당기순이익', '영업활동현금흐름',
		'투자활동현금흐름', '재무활동현금흐름', 'DPS(보통주,현금+주식)', '배당금(현금+주식)', '배당성향(현금+주식)',
		'영업이익률', '당기순이익률', '매출액증가율(전년동기)', '영업이익증가율(전년동기)', '당기순이익증가율(전년동기)',
		'이자보상배율', '유동비율', '부채비율', '배당수익률(보통주,현금+주식)', '매출총이익률', '총자산회전율',
		'ROE(영업이익)', 'ROE(당기순이익)', 'ROA(영업이익)', 'ROA(당기순이익)', 'EPS', 'BPS',
		'PER', 'PBR', 'PSR', 'PCR', 'ROIC', 'EV/EBIT', 'EV/EBITDA']
	csv_df = pd.DataFrame(data=datas, columns=cols)
	return csv_df



def combine_git_naver(naver_path, git_path, comp_id_path, save_path):
	""" 네이버 재무제표와 깃 재무제표를 합치는 함수"""
	codes, _  = read_code_csv(comp_id_path)
	count = 0
	KONEX_list = except_KONEX_list(comp_id_path)
	no_december = ['169330', '190650' ,'357120', '950210', '338100', '365550', '334890'] # 실적발표가 12월이 아닌 회사
	no_report = ['402340', '383310', '378850', '383800'] # 재무제표가 없는 회사
	# 코넥스가 들어오는 경우 예외처리 필요
	for code in codes:
		code = '{:0>6}'.format(code)
		# if code != '000050':
		# 	continue

		if code in no_report or code in KONEX_list: # 예외 케이스
			print('Exception case')
			continue
		
		datas = []
		try:
			n_df = pd.read_csv(f'{naver_path}/{code}.csv')
			g_df = pd.read_csv(f'{git_path}/{code}.csv', encoding='cp949')
			print(f'{code} : git ok')
			g_df = except_for_git_make_same_columns(g_df)
			for i in range(1, len(n_df.columns)):
				for j in range(1, len(n_df)):
					if pd.isna(n_df.iat[j, i]):
						n_df.iat[j, i] = 0
			g_df = except_kor(g_df)
			converted_csv = convert_from_naver_to_git(naver_path, code)
			# 깃 재무제표에 있던 년도는 지우기
			drop_list = [] 
			for idx, year in enumerate(converted_csv['구분']):
				if str(year) in str(g_df['구분']):
					drop_list.append(idx)
			converted_csv = converted_csv.drop(drop_list)
			csv_df = pd.concat([g_df, converted_csv])

		except:
			print(f'{code} : except')
			count = 0
			csv_df = convert_from_naver_to_git(naver_path, code)

		csv_df.to_csv(f'{save_path}/{code}.csv', encoding='cp949', index=False)


def update_finance_from_naver(comp_id_path, naver_path, save_path):
	""" 이전에 combined 되어 있던 파일이 있다는 가정하에 새롭게 네이버 재무제표에서 데이터를 추가 """
	codes, _  = read_code_csv(comp_id_path)
	
	for code in codes:
		code = '{:0>6}'.format(code)
		new_df = pd.read_csv(f'{naver_path}/{code}.csv', encoding='cp949')
		datas = []
		try:
			saved_df = pd.read_csv(f'{save_path}/{code}.csv', encoding='cp949')
			new_df = convert_from_naver_to_git(naver_path, code)
			done_years = [i for i in saved_df['구분']]
			years = new_df['구분']
			for year in years:
				if year not in done_years:
					break
		
			new_df = new_df[new_df['구분'] >= year]
			combined_df = pd.concat([saved_df,new_df])
		except:
			print("Exception : No saved finance")
			continue
		combined_df.to_csv(f'{save_path}/{code}.csv', encoding='cp949', index=False)


def make_one_csv(comp_id_path, save_path):
	""" 모든 csv 파일을 하나의 csv 로 만드는 함수 """
	count = 0
	flag = 0
	return_date = 0
	codes, comp_ids = read_code_csv(comp_id_path)

	for i in range(len(codes)):
		code = codes[i]
		code = '{:0>6}'.format(code)
		id = comp_ids[i]
		try:
			before_df = pd.read_csv(f'{save_path}/{code}.csv', encoding='cp949', index_col=False)
			if code == '334890': # 이상하게 이 항목만 Unnamed: 0 가 생성되어서 따로 예외 처리해줌
				before_df.drop(['Unnamed: 0'], axis = 1, inplace = True)
			print(code, id)
		except:
			print(f'{code} is excepted')
			continue
		remove_list = [] # 어떠한 정보도 없는 행 지우기
	
		for i in range(len(before_df)):
			if before_df.loc[i, '자산'] == 0:
				remove_list.append(i)

		df = before_df.drop(remove_list, axis = 0)

		df.insert(0, "Code", [code] * len(df))
		df.insert(0, "ID", [i for i in range(count, count + len(df))])
		df.insert(0, "COMP_ID", [id] * len(df))
		
		if flag == 0:
			return_date = df
		else:
			return_date = pd.concat([return_date, df])
		count += len(df)
		flag = 1

	# dypes 수정
	col = ['구분', '자산', '유동자산', '비유동자산', '기타자산', '부채', '유동부채', '비유동부채', '자본',
		'주당액면가액', '발행한주식총수', '보통주', '우선주', '매출액', '매출원가', '매출총이익', '판매비와관리비',
		'영업이익', '법인세차감전순이익', '당기순이익', 'DPS(보통주,현금+주식)', '배당금(현금+주식)',
		'배당성향(현금+주식)', '영업이익률', '당기순이익률', '매출액증가율(전년동기)', '영업이익증가율(전년동기)',
		'당기순이익증가율(전년동기)', '이자보상배율', '유동비율', '부채비율', '배당수익률(보통주,현금+주식)',
		'매출총이익률', '총자산회전율', 'ROE(영업이익)', 'ROE(당기순이익)', 'ROA(영업이익)',
		'ROA(당기순이익)', 'EPS', 'BPS', 'PER', 'PBR', 'PSR', 'PCR', 'ROIC',
		'EV/EBIT', 'EV/EBITDA', '영업활동현금흐름', '투자활동현금흐름', '재무활동현금흐름']
	types = ['str'] + ['float64'] * 24 + ['object'] * 2 + ['float64'] * 10 + ['int64'] * 2 +  ['float64'] * 10
	dic = { name:value for name, value in zip(col, types)}
	# dic['구분'] = 'str'
	# dic['매출액증가율(전년동기)'] = 'float64'
	# dic['부채비율'] = 'float64'
	# dic['ROE(영업이익)'] = 'float64'
	# dic['ROE(당기순이익)'] = 'float64'
	return_date = return_date.astype(dic)
	return_date['구분'] = return_date['구분'].apply(for_make_yyyy_mm_dd)
	return_date['구분'] = return_date['구분'].apply(lambda x : x.replace('.0', ''))
	return_date['구분'] = return_date['구분'].apply(lambda x : x.replace('.', '-'))
	# 단위 수정
	change_list = ['자산', '유동자산', '비유동자산', '기타자산', '부채', '유동부채', '비유동부채', '자본','매출액', '매출원가', '매출총이익', '판매비와관리비','영업이익', '법인세차감전순이익', '당기순이익']
	for col in return_date.columns:
		if col in change_list:
			return_date[col] = return_date[col].apply(lambda x : round(x / 100000, 2))
	# # print(return_date)
	return_date.to_csv(f'{save_path}/combinded.csv', index=False, encoding='cp949')


if __name__ == "__main__":
	naver_path = '/Users/choewonjun/Documents/coding/finance_data/crolling/naver_finance/csv_files/yearly_df/yearly' 
	git_path = '/Users/choewonjun/Documents/coding/finance_data/crolling/git_finance/git_finance' # 2050
	save_path = '/Users/choewonjun/Documents/coding/finance_data/crolling/combined_finance/done_csv' 
	comp_id_path = '/Users/choewonjun/Documents/coding/finance_data/crolling/combined_finance/company.csv'

	# start_tiem = time.time()

	# union_structure(save_path)

	# combine_git_naver(naver_path, git_path, comp_id_path, save_path)
	make_one_csv(comp_id_path, save_path)
	# df = convert_from_naver_to_git(naver_path, '000020')
	# df.to_csv('~/Desktop/ck.csv', index=False, encoding='cp949')
	# end_time = time.time()
	# print(end_time - start_tiem)
