# finance_data

## data_collector

### 함수설명

- get_raw_price_info()
	- 1995 ~ 2021 모든 종목(상폐종목 포함) !수정주가 아님!
	```
		Date 날짜,
		Code 종목코드,
		Name 한글 종목 이름,
		Market 거래소,
		MarketId 거래소 id,
		Open 시가,
		High 고가,
		Low 저가,
		Close 종가,
		ChangeCode 등락 기호,
		Changes 전일대비, 
		ChagesRatio 등락률,
		Volume 거래량,
		Amount 거래대금,
		Marcap 시가총액(백만원),
		Stocks 상장주식수,
		Ranks 시가총액 순위 (당일)
	```

- get_cur_comp_info()
	- 현재 상장 종목 정보
	```
		Symbol 종목코드,
		Market 거래소,
		Name 하나글 종목 이름,
		Sector 섹터,
		Industry 사업 분야,
		ListingDate 상장일,
		HomePage 홈페이지
	```

- get_market_open_info()
	- 1995년부터 당일 거래가능 종목 리스트
	```
		Date 날짜,
		Code 종목코드
	```

- get_price_info()
	- 현재 상장된 종목중 1995년부터의 수정주가 데이터
	```
		Code 종목코드,
		Date 날짜,
		Open 시가,
		High 고가,
		Low 저가,
		AdjClose 종가,
		Volume 거래량,
		Changes 등락률
	```