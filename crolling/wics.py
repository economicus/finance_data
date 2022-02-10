# WICS 소분류 -> 대분류 함수
def change_wics(wics):
	if wics == "에너지장비및서비스" or wics == '석유와가스':
		return "에너지"
	elif wics == "화학" or wics == '포장재' or wics == '비철금속'or wics == '철강'or wics == '종이와목재':
		return "소재"
	elif (wics == "우주항공과국방" or wics == '건축제품' or wics == '건축자재'or wics == '건설'or wics == '가구' or wics == '운송인프라' or
			wics == "전기장비" or wics == '복합기업' or wics == '기계'or wics == '조선'or wics == '무역회사와판매업체' or
			wics == "상업서비스와공급품" or wics == '항공화물운송과물류' or wics == '항공사'or wics == '해운사'or wics == '도로와철도운송'):
		return "산업재"
	elif (wics == "자동차부품" or wics == '자동차' or wics == '가정용기기와용품'or wics == '레저용장비와제품'or wics == '섬유,의류,신발,호화품' or wics == '화장품' or
			wics == "문구류" or wics == '호텔,레스토랑,레저' or wics == '다각화된소비자서비스'or wics == '판매업체'or wics == '인터넷과카탈로그소매' or
			wics == "백화점과일반상점" or wics == '전문소매' or wics == '교육서비스'):
		return "경기관련소비재"
	elif (wics == "식품과기본식료품소매" or wics == '음료' or wics == '식품'or wics == '담배'or wics == '가정용품'):
		return "필수소비재"
	elif (wics == "건강관리장비와용품" or wics == '건강관리업체및서비스' or wics == '건강관리기술'or wics == '생물공학'or wics == '제약' or wics == '생명과학도구및서비스'):
		return "건강관리"
	elif (wics == "은행" or wics == '증권' or wics == '창업투자'or wics == '카드'or wics == '기타금융' or wics == '손해보험'or wics == '생명보험'or wics == '부동산'):
		return "금융"
	elif (wics == "IT서비스" or wics == '소프트웨어' or wics == '통신장비'or wics == '핸드셋'or wics == '컴퓨터와주변기기' or wics == '전자장비와기기'or 
			wics == '사무용전자제품'or wics == '반도체와반도체장비'or wics == '전자제품'or wics == '전기제품'or wics == '디스플레이 패널'or wics == '디스플레이 장비 및 부품'):
		return "IT"
	elif (wics == "다각화된통신서비스" or wics == '무선통신서비스' or wics == '광고'or wics == '방송과엔터테인먼트'or wics == '출판' or wics == '게임엔터테인먼트'or wics == '양방향미디어와서비스'):
		return "커뮤니케이션서비스"
	elif (wics == "전기유틸리티" or wics == '가스유틸리티' or wics == '복합유틸리티'or wics == '수도유틸리티'or wics == '독립전력생산및에너지거래'):
		return "유틸리티"