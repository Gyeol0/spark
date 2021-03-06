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