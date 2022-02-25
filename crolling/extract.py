import pandas as pd

def read_code_csv(path):
	df = pd.read_csv(path)
	codes = df['Symbol']
	comp_id = df['ID']
	return (codes, comp_id)


def except_kor(g_df):
	""" 예외 케이스 처리 함수 """
	kor = ['흑전', '적전', '적지']
	g_df['영업이익증가율(전년동기)'] = g_df['영업이익증가율(전년동기)'].apply(lambda x : 0 if x in kor else x)
	g_df['당기순이익증가율(전년동기)'] = g_df['당기순이익증가율(전년동기)'].apply(lambda x : 0 if x in kor else x)
	return g_df

def except_KONEX_list(comp_id_path):
	df = pd.read_csv(comp_id_path)
	KONEX_list = []
	for i in df[df['Market'] == 'KONEX']['Symbol']:
		KONEX_list.append('{:0>6}'.format(i))
	return KONEX_list


def extract_from_naver(i, n_df):
	""" 네이버 재무제표에서 깃 재무제표 컬럼에 있는 요소들을 추출하는 함수 """
	data = []
	data.append(n_df.loc[0, i].split('/')[0]) # 구분
	data.append(int(n_df.loc[8, i].replace(',', '') + '00000')) # 자산
	data.append(None) # 유동자산
	data.append(None) # 비유동자산
	data.append(None) # 기타자산
	data.append(int(n_df.loc[9, i].replace(',', '') + '00000')) # 부채
	data.append(None) # 유동부채
	data.append(None) # 비유동부채
	data.append(int(n_df.loc[10, i].replace(',', '') + '00000')) # 자본
	data.append(None) # 주당액면가액
	data.append(int(n_df.loc[33, i].replace(',', ''))) # 발행주식수
	data.append(None) # 보통주
	data.append(None) # 우선주
	data.append(int(n_df.loc[1, i].replace(',', '') + '00000')) # 매출액
	data.append(None) # 매출원가
	data.append(None) # 매출총이익
	data.append(None) # 판매비와관리비
	data.append(int(n_df.loc[2, i].replace(',', '') + '00000')) # 영업이익
	data.append(None) # 법인세차감전순이익
	data.append(int(n_df.loc[5, i].replace(',', '') + '00000')) # 당기순이익
	data.append(int(n_df.loc[14, i].replace(',', '') + '00000')) # 영업활동현금흐름
	data.append(int(n_df.loc[15, i].replace(',', '') + '00000')) # 투자활동현금흐름
	data.append(int(n_df.loc[16, i].replace(',', '') + '00000')) # 재무활동현금흐름
	data.append(int(n_df.loc[30, i].replace(',', ''))) # DPS
	data.append(None) # 배당금
	data.append(float(n_df.loc[32, i].replace(',', ''))) # 배당성향
	data.append(round(int(n_df.loc[2, i].replace(',', '') + '00000') / int(n_df.loc[1, i].replace(',', '') + '00000') * 100, 2) if int(n_df.loc[1, i].replace(',', '') + '00000') != 0 else 0 ) # 영업이익률
	data.append(round(int(n_df.loc[5, i].replace(',', '') + '00000') / int(n_df.loc[1, i].replace(',', '') + '00000') * 100, 2) if int(n_df.loc[1, i].replace(',', '') + '00000') != 0 else 0 ) # 당기순이익률
	data.append(round(int(n_df.loc[1, i].replace(',', '') + '00000') / int(n_df.loc[1, str(int(i) - 1)].replace(',', '') + '00000') * 100 - 100, 2) if int(i) != 1 and int(n_df.loc[1, str(int(i) - 1)].replace(',', '') + '00000') != 0 else 0) # 매출액증가율
	data.append(round(int(n_df.loc[2, i].replace(',', '') + '00000') / int(n_df.loc[2, str(int(i) - 1)].replace(',', '') + '00000') * 100 - 100, 2) if int(i) != 1 and  int(n_df.loc[2, str(int(i) - 1)].replace(',', '') + '00000') != 0 else 0) # 영업이익증가율
	data.append(round(int(n_df.loc[5, i].replace(',', '') + '00000') / int(n_df.loc[5, str(int(i) - 1)].replace(',', '') + '00000') * 100 - 100, 2) if int(i) != 1 and  int(n_df.loc[5, str(int(i) - 1)].replace(',', '') + '00000') != 0 else 0) # 당기순이익증가율
	data.append(None) # 이자보상배율
	data.append(None) # 유동비율
	data.append(round(int(n_df.loc[9, i].replace(',', '') + '00000') / int(n_df.loc[8, i].replace(',', '') + '00000') * 100, 2) if int(n_df.loc[8, i].replace(',', '') + '00000') != 0 else 0) # 부채비율
	data.append(n_df.loc[31, i]) # 배당수익률
	data.append(None) # 매출총이익률
	data.append(round(int(n_df.loc[1, i].replace(',', '') + '00000') / int(n_df.loc[8, i].replace(',', '') + '00000'), 2) if int(n_df.loc[8, i].replace(',', '') + '00000') != 0 else 0) # 총자산회전율
	data.append(round(int(n_df.loc[2, i].replace(',', '') + '00000') / int(n_df.loc[10, i].replace(',', '') + '00000') * 100, 2) if int(n_df.loc[10, i].replace(',', '') + '00000') != 0 else 0) # ROE 영업이익
	data.append(round(int(n_df.loc[5, i].replace(',', '') + '00000') / int(n_df.loc[10, i].replace(',', '') + '00000') * 100, 2) if int(n_df.loc[10, i].replace(',', '') + '00000') != 0 else 0) # ROE 당기순이익
	data.append(round(int(n_df.loc[2, i].replace(',', '') + '00000') / int(n_df.loc[8, i].replace(',', '') + '00000') * 100, 2) if int(n_df.loc[8, i].replace(',', '') + '00000') != 0 else 0) # ROA 영업이익
	data.append(round(int(n_df.loc[5, i].replace(',', '') + '00000') / int(n_df.loc[8, i].replace(',', '') + '00000') * 100, 2) if int(n_df.loc[8, i].replace(',', '') + '00000') != 0 else 0) # ROA 당기순이익
	data.append(int(n_df.loc[26, i].replace(',', ''))) # EPS
	data.append(int(n_df.loc[28, i].replace(',', ''))) # BPS
	data.append(float(n_df.loc[27, i].replace(',', ''))) # PER
	data.append(float(n_df.loc[29, i].replace(',', ''))) # PBR
	data.append(None) # PSR
	data.append(None) # PCR
	data.append(None) # ROIC
	data.append(None) # EV/EBIT
	data.append(None) # EV/EBITDA
	return (data)


def combine_git_naver(naver_path, git_path, comp_id_path, save_path):
	""" 네이버 재무제표와 깃 재무제표를 합치는 함수"""
	codes, _  = read_code_csv(comp_id_path)
	count = 0
	KONEX_list = except_KONEX_list(comp_id_path)
	no_december = ['169330', '190650' ,'357120', '950210', '338100']
	# 코넥스가 들어오는 경우 예외처리 필요
	for code in codes:
		code = '{:0>6}'.format(code)
		if code not in no_december:
			continue

		if code == '334890' or code == '402340' or code in KONEX_list: # 예외 케이스
			continue

		try:
			n_df = pd.read_csv(f'{naver_path}/{code}.csv')
			g_df = pd.read_csv(f'{git_path}/{code}.csv', encoding='cp949')
			print(f'{code} : git ok')

			for i in range(1, len(n_df.columns)):
				for j in range(1, len(n_df)):
					if pd.isna(n_df.iat[j, i]):
						n_df.iat[j, i] = '0'
			g_df = except_kor(g_df)
			for i in n_df.columns[1:]:
				if pd.isna(n_df.loc[0, i]):
					data = [0] * 50
					datas.append(data)
					count += 1
					continue			
				# print(n_df.loc[0, i])

				if int(n_df.loc[0, i].split('/')[0]) <= 2018 or int(n_df.loc[0, i].split('/')[1][:2]) != 12 or 'E' in n_df.loc[0, i]:
					if code in no_december: # 예외처리 (실적이 12월에 안나는 회사)
						pass
					else:
						continue

				g_df.loc[len(g_df)] = extract_from_naver(i, n_df)
			csv_df = g_df

		except:
			print(f'{code} : except')
			# n_df = pd.read_csv(f'{naver_path}/{code}.csv')
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
				if int(n_df.loc[0, i].split('/')[0]) <= 2018 or int(n_df.loc[0, i].split('/')[1][:2]) != 12 or 'E' in n_df.loc[0, i]:
					if code in no_december: # 예외처리 (실적이 12월에 안나는 회사)
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
			
		# print(csv_df)
		# return
		csv_df.to_csv(f'{save_path}/{code}.csv', encoding='cp949', index=False)
		# csv_df.to_csv(f'/Users/choewonjun/Desktop/ck/{code}.csv', encoding='cp949')


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


def for_make_yyyy_mm_dd(x):
	if pd.isna(x) or x == 'nan' or x == None:
		return None
	if '.0' in x:
		x.replace('.0', '')
	return x + '-12-31'


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
			print(code, id)
		except:
			print(f'{code} is excepted')
			continue

		remove_list = [] # 어떠한 정보도 없는 행 지우기
		for i in range(len(before_df)):
			if before_df.iat[i, 0] == 0:
				remove_list.append(i)
		df = before_df.drop(remove_list, axis = 0)

		df.insert(0, "Code", [code] * len(df))
		df.insert(0, "ID", [i for i in range(count, count + len(df))])
		df.insert(0, "COMP_ID", [id] * len(df))

		df = df.astype({'구분':'str'})
		if flag == 0:
			return_date = df
		else:
			return_date = pd.concat([return_date, df])
		count += len(df)
		flag = 1
	return_date['구분'] = return_date['구분'].apply(for_make_yyyy_mm_dd)
	return_date['구분'] = return_date['구분'].apply(lambda x : x.replace('.0', ''))
	return_date['구분'] = return_date['구분'].apply(lambda x : x.replace('.', '-'))
	# # print(return_date)
	return_date.to_csv(f'{save_path}/combinded.csv', index=False, encoding='cp949')


if __name__ == "__main__":
	naver_path = '/Users/choewonjun/Documents/coding/crolling/combine_24/crolling/naver_finance/yearly_df/yearly' 
	git_path = '/Users/choewonjun/Documents/coding/crolling/combine_24/crolling/git_finannce' # 2050
	save_path = '/Users/choewonjun/Documents/coding/crolling/combine_24/crolling/done_finance' 
	comp_id_path = 'company.csv'

	# union_structure(save_path)
	# combine_git_naver(naver_path, git_path, comp_id_path, save_path)
	make_one_csv(comp_id_path, save_path)
	