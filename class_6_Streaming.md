<스파크를 다루는 기술>(길벗, 2018)을 학습하고 개인 학습용으로 정리한 내용입니다.

출처 - Petar Zecevic 외 1명. <스파크를 다루는 기술(Spak in Action)>. 이춘오. 길벗(2018)

# 6장 스파크 스트리밍

* 실시간으로 데이터가 흐르도록

## 6.1 애플리케이션 작성

* 일괄 처리를 지향하는 스파크에 실시간 데이터를 어떻게 지향할 수 있을까?
  * 미니 배치(mini-batch)
  * 특정 시간 간격 내에 유입된 데이터 블록을 RDD로 구성
  * 입력 데이터 스트림을 미니 배치 RDD로 시분할하고, 다른 스파크 컴포넌트는 이 미니 배치 RDD를 일반 RDD처럼 처리

### 6.1.1 예제

* 초당 거래 주문 건수
* 누적 거래액이 가장 맣은 고객 1~5위
* 지난 1시간 동안 거래량이 가장 많았던 유가 증권 1~5위

### 6.1.2 스트리밍 컨텍스트 생성

```python
from __future__ import print_function
from pyspark.streaming import StreamingContext

ssc = StreamingContext(sc, 5)
```

* 미니 배치 RDD를 생성할 시간 간격 지정(5초)
* Minute(), Millisecond()도 가능

### 6.1.3 이산 스트림 생성

* **SteamingContext**의 **textFileStream** 메서드를 사용해 파일의 텍스트 데이터를 스트리밍으로 직접 전달
* 지정된 디렉터리를 모니터링하고, 디렉터리에 새로생성된 파일을 개별적으로 읽음
* 즉, **SteamingContext**를 시작할 시점에 이미 폴더에 있던 파일은 처리하지 않는다.

```python
filestream = ssc.textFileStream("/home/gyeol/ch06input")
```

* **textFileStream** 메서드에 전달해 스트리밍 애플리케이션의 입력 폴더로 설정
* 분할된 파일을 복사할 폴더

#### 데이터 설명

* 주문 시각 : yyyy-MM-dd HH:mm:ss 형식
* 주문 ID : 순차적으로 증가시킨 정수
* 고객 ID : 1에서 100사이 무작위 정수
* 주식 종목 코드 : 80개의 주식 종목 코드 목록에서 무작위로 선택한 값
* 주문 수량 : 1에서 1000 사이 무작위 저웃
* 매매 가격 : 1에서 100 사이 무작위 정수
* 주문 유형 : 매수 주문(B) 또는 매도 주문(S)

#### 데이터 파싱

```python
from datetime import datetime
def parseOrder(line):
  s = line.split(",")
  try:
      if s[6] != "B" and s[6] != "S":
        raise Exception('Wrong format')
      return [{"time": datetime.strptime(s[0], "%Y-%m-%d %H:%M:%S"), "orderId": long(s[1]), "clientId": long(s[2]), "symbol": s[3],
      "amount": int(s[4]), "price": float(s[5]), "buy": s[6] == "B"}]
  except Exception as err:
      print("Wrong line format (%s): " % line)
      return []

orders = filestream.flatMap(parseOrder)
```



