# finance_data

## data_collector

### 필요 데이터 -> db 저장

### db 설정 및 실행방법

- connect.py애서 db.create_engine('mysql+pymysql://{db 정보 입력}')

- data_collector.py에서 함수 차례로 실행

### 함수설명

- get_company_table()
	- 현재 상장 종목 정보
	
	- columns
	```
		ID	아이디
		Symbol 종목코드
		Market 거래소
		Name 종목한글명
		MainSector 대분류섹터(WICS)
		Sector 소분류섹터
		Industry 상품
		ListingDate 상장일
		HomePage 회사홈페이지
	```

- get_price_table()
	- 가격 정보(수정주가 기준)

	- columns
	```
		ID 아이디
		Date 날짜
		Open 시가
		High 고가
		Low 저가
		AdjClose 종가
		Volume 거래량
		PVolume 거래대금
		Changes 등락률
		Marcap 시가총액
		Stocks 주식수
		Ranks 당일 시총기준 순위
	```

- get_finance_table()
	- 재무 정보

	- columns
	```
		COMP_IT company테이블아이디
		ID 아이디
		Code 종목코드
		Quarter 분기
		NetRevenue 총매출
		NetRevenueRate 총매출 증가율
		NetProfitMargin 당기순이익(순수익률)
		NetProfitMarginRate 당기순이익 증가율
		DERatio 부채비율
		PER 
		PSR 
		PBR 
		PCR
		OperationActivities 영업 현금흐름
		InvestingActivities 투자 현금흐름
		FinancingActivities 재무 현금흐름
		DividendYield 현금배당수익률
		DividendPayoutRatio 현금배당성향
		ROA 
		REO 
	```



- get_price_average_info()
	- 메년 12월 평균가격 정보

	- columns
	```
		ID 아이디
		Date 날짜
		AdjClose 12월 평균가격
	```
	
- add_main_sector_tocompany()
	- main_sector를 company table에 업데이트 시켜준다.
	
	
- get_market_open_info()
	- 종목별 주식시장 거래일
