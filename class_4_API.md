# 4장 스파크 API 깊이 파헤치기

## 1. Pari RDD 다루기

### 1.1 Pair RDD 생성

* `key` : `value`
* 각 요소를 (f(요소), 요소) 쌍의 튜플로 매핑



### 2.2 기본 Pair RDD 함수

> 규칙 1 구매 횟수가 가장 많은 고객에게는 곰 인형을 보낸다.
>
> 규칙 2 바비 쇼핑몰 놀이 세트를 두 개 이상 구매하면 청구 금액을 5% 할인해 준다.
>
> 규칙 3 사전을 다섯 권 이상 구매한 고객에게는 칫솔을 보낸다.
>
> 규칙 4 가장 많은 금액을 지출한 고객에게는 커플 잠옷 세트를 보낸다.

* 구매날자#시간#고객 ID#상품 ID#구매 수량#구매 금액

```scala
val tranFile = sc.textFile("edition/ch04/ch04_data_transactions.txt")
val tranData = tranFile.map(_.split("#"))
var transByCust = tranData.map(tran => (tran(2).toInt, tran))
```

```python
tranFile = sc.textFile("edition/ch04/ch04_data_transactions.txt")
tranData = tranFile.map(lambda line: line.split("#"))
transByCust = tranData.map(lambda t: (int(t[2]), t))
```

* `sc.textFile` 데이터 로드
* `map` `split` 데이터 파싱
* `map(x[2], x)` Pari RDD 생성
* Key = 고객 ID, Value = 데이터 배열

