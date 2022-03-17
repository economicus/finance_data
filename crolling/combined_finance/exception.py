import pandas as pd


def except_for_git_make_same_columns(g_df):
	""" 칼럼 순서가 바뀐 경우의 예외처리 함수 """
	if g_df.iloc[:,1].name == 'DPS(보통주,현금+주식)':
		col_0 = g_df.columns[:1].to_list()
		col_1 = g_df.columns[38:].to_list()
		col_2 = g_df.columns[28:38].to_list()
		col_3 = g_df.columns[1:28].to_list()
		new_col = col_0 + col_1 + col_2 + col_3
		g_df = g_df[new_col]

	elif g_df.iloc[:,13].name == '영업활동현금흐름':
		if g_df.iloc[:,16].name == 'DPS(보통주,현금+주식)':
			col_0 = g_df.columns[:13].to_list()
			col_1 = g_df.columns[43:].to_list()
			col_2 = g_df.columns[13:43].to_list()
			new_col = col_0 + col_1 + col_2
		else:
			col_0 = g_df.columns[:13].to_list()
			col_1 = g_df.columns[16:23].to_list()
			col_2 = g_df.columns[13:16].to_list()
			col_3 = g_df.columns[23:].to_list()
			new_col = col_0 + col_1 + col_2 + col_3
		g_df = g_df[new_col]

	elif  g_df.iloc[:,13].name == 'DPS(보통주,현금+주식)':
		col_1 = g_df.columns[:13].to_list()
		col_2 =g_df.columns[40:].to_list()
		col_3 =g_df.columns[13:40].to_list()
		new_col=col_1+col_2 + col_3
		g_df = g_df[new_col]

	elif g_df.iloc[:,13].name != '매출액':
		col_1 = g_df.columns[:13].to_list()
		col_2 = g_df.columns[43:].to_list()
		col_3 = g_df.columns[13:43].to_list()
		new_col=col_1+col_2 + col_3
		g_df = g_df[new_col]
		
	elif g_df.iloc[:,43].name == 'PCR':
		col_1 = g_df.columns[:20].to_list()
		col_2 = g_df.columns[47:].to_list()
		col_3 = g_df.columns[20:47].to_list()
		new_col=col_1+col_2 + col_3
		g_df = g_df[new_col]
		
	return g_df

def except_kor(g_df):
	""" 예외 케이스 처리 함수 """
	kor = ['흑전', '적전', '적지', '완전잠식']
	g_df['매출액증가율(전년동기)'] = g_df['매출액증가율(전년동기)'].apply(lambda x : 0 if x in kor else x)
	g_df['영업이익증가율(전년동기)'] = g_df['영업이익증가율(전년동기)'].apply(lambda x : 0 if x in kor else x)
	g_df['당기순이익증가율(전년동기)'] = g_df['당기순이익증가율(전년동기)'].apply(lambda x : 0 if x in kor else x)
	g_df['부채비율'] = g_df['부채비율'].apply(lambda x : 0 if x in kor else x)
	g_df['ROE(영업이익)'] = g_df['ROE(영업이익)'].apply(lambda x : 0 if x in kor else x)
	g_df['ROE(당기순이익)'] = g_df['ROE(당기순이익)'].apply(lambda x : 0 if x in kor else x)
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
	data.append(float(n_df.loc[27, i].replace(',', '')) if n_df.loc[27, i] != 0 else 0) # PER
	data.append(float(n_df.loc[29, i].replace(',', '')) if n_df.loc[27, i] != 0 else 0) # PBR
	data.append(None) # PSR
	data.append(None) # PCR
	data.append(None) # ROIC
	data.append(None) # EV/EBIT
	data.append(None) # EV/EBITDA
	return (data)