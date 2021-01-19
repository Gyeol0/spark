<스파크를 다루는 기술>(길벗, 2018)을 학습하고 개인 학습용으로 정리한 내용입니다.

출처 - Petar Zecevic 외 1명. <스파크를 다루는 기술(Spak in Action)>. 이춘오. 길벗(2018)

# 4장 스파크 API

## 1. Pari RDD

### 1.1 Pair RDD

* `key` : `value`
* 각 요소를 (f(요소), 요소) 쌍의 튜플로 매핑



### 1.2 기본 Pair RDD 함수

> 규칙 1 구매 횟수가 가장 많은 고객에게는 곰 인형을 보낸다.
>
> 규칙 2 바비 쇼핑몰 놀이 세트를 두 개 이상 구매하면 청구 금액을 5% 할인해 준다.
>
> 규칙 3 사전을 다섯 권 이상 구매한 고객에게는 칫솔을 보낸다.
>
> 규칙 4 가장 많은 금액을 지출한 고객에게는 커플 잠옷 세트를 보낸다.

* 구매날자#시간#고객 ID#상품 ID#구매 수량#구매 금액

#### Pair RDD 생성

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

#### 키 값 개수 세기

```scala
transByCust.keys.distinct().count()
```

```python
transByCust.keys.distinct().count()
```

* 고객 ID별 중복제거 후 count
* `transByCust.map(_._1).distinct.count()` 같은 결과

#### 키별 개수 세기

``` scala
transByCust.countByKey()
```

```python
import operator
transByCust.countByKey()
```

* 고객 ID별 구매 횟수

```scala
val (cid, purch) = transByCust.countByKey().toSeq.sortBy(_._2).last
```

```python
(cid, purch) = sorted(transByCust.countByKey().items(), key=operator.itemgetter(1))[-1]
```

* 구매 횟수를 기준으로 sort
* `last` `[-1]`가장 많은 상품을 구매한 고객 ID(cid), 횟수(purch)

```scala
var complTrans = Array(Array("2015-03-30", "11:59 PM", "53", "4", "1", "0.00"))
```

```python
complTrans = [["2015-03-30", "11:59 PM", "53", "4", "1", "0.00"]]
```

* 규칙 1에 의해서 가장 많은 상품을 구매한 53번 고객에게 사은품 추가 구매 기록 저장 (구매 금액 0)

#### 단일 키로 값 찾기

```scala
transByCust.lookup(53)
transByCust.lookup(53).foreach(tran => println(tran.mkString(", ")))
```

```python
transByCust.lookup(53)
for t in transByCust.lookup(53):
    print(", ".join(t))
```

* `lookup`특정 고객이 어떠한 상품을 구매했는지 파악

#### mapValues 변환 연산자로 Pair RDD 값 바꾸기(규칙 2)

```scala
transByCust = transByCust.mapValues(tran => {
     if(tran(3).toInt == 25 && tran(4).toDouble > 1)
         tran(5) = (tran(5).toDouble * 0.95).toString
     tran })
```

```python
def applyDiscount(tran):
    if(int(tran[3])==25 and float(tran[4])>1):
        tran[5] = str(float(tran[5])*0.95)
    return tran
transByCust = transByCust.mapValues(lambda t: applyDiscount(t))
```

* 상품 ID가 25(바비 쇼핑몰 놀이 세트)인지 확인
* 25번 상품을 2개 이상 구매했는지 확인
* 위 조건을 모두 만족하면 구매 금액을 5% 할인(0.95)

#### flatMapValues 변환 연산자로 키에 값 추가(규칙 3)

* 사은품 구매 기록을 추가
* `flatMapValues` - 각 키 값을 0개 또는 한 개 이상 값으로 매핑해 RDD에 포함된 요소 개수를 변경할 수 있다.
* 키에 새로운 값을 추가하거나 키 값을 모두 제거할 수 있다.

```scala
transByCust = transByCust.flatMapValues(tran => {
    if(tran(3).toInt == 81 && tran(4).toInt >= 5) {
       val cloned = tran.clone()
       cloned(5) = "0.00"; cloned(3) = "70"; cloned(4) = "1";
       List(tran, cloned)
    }
    else
       List(tran)
    })
```

```python
def addToothbrush(tran):
    if(int(tran[3]) == 81 and int(tran[4])>4):
        from copy import copy
        cloned = copy(tran)
        cloned[5] = "0.00"
        cloned[3] = "70"
        cloned[4] = "1"
        return [tran, cloned]
    else:
        return [tran]
transByCust = transByCust.flatMapValues(lambda t: addToothbrush(t))
```

* 상품 ID가 81번(사전)인지 확인
* 81번 상품을 5개 이상 구매했느지 확인
* `tran.clone()`복제(복사)
* `cloned`사은품 칫솔(70번), 1개, 구매금액 0으로 수정
* 원래 요소와 추가한 요소를 반환
* 조건에 만족하지 않을 때에는 원래 요소만 반환

```python
transByCust.count()
```

* 1006개로 칫솔 사은품 = 6개

#### reduceByKey 변환 연산자로 키의 모든 값 병합

* `reduceByKey`
* 각 키의 모든 값을 동일한 타입의 단일 값으로 병합
* 두 값을 하나로 병합하는 merge 함수를 전달해야한다.
* 각 키별로 값 하나만 남을 때까지 merge 함수를 계속 호출한다.
* `foldBykeY`와 유사
* `foldByKey` 기능은 같지만, merge 함수의 인자 목록 바로 앞에 `zeroValue`인자를 담은 또 다른 인자 목록을 추가로 전달

```scala
val amounts = transByCust.mapValues(t => t(5).toDouble)
val totals = amounts.foldByKey(0)((p1, p2) => p1 + p2).collect()
totals.toSeq.sortBy(_._2).last
amounts.foldByKey(100000)((p1, p2) => p1 + p2).collect()
```

```python
amounts = transByCust.mapValues(lambda t: float(t[5]))
totals = amounts.foldByKey(0, lambda p1, p2: p1 + p2).collect()
sorted(totals, key=operator.itemgetter(1))[-1]
totals_1 = amounts.foldByKey(100000, lambda p1, p2: p1 + p2).collect()
sorted(totals_1, key=operator.itemgetter(1))[-1]
```

* zeroValue는 반드시 항등원
* 덧셈 0, 곱셈 1, 리스트 연산 Nil
* `amounts`구매 금액만을 Double형으로 변환 후 매핑
* `totals`키를 기준으로 차례로 합계
* `totals.toSeq.sortBy(_._2).last`오름차순 정렬 후 마지막
* `amounts.foldByKey(100000...` RDD의 파티션 개수만큼 100000 더함
* 결과 1 : 100049, 결과 2 : 300049

```scala
complTrans = complTrans :+ Array("2015-03-30", "11:59 PM", "76", "63", "1", "0.00")
transByCust = transByCust.union(sc.parallelize(complTrans).map(t => (t(2).toInt, t)))
transByCust.map(t => t._2.mkString("#")).saveAsTextFile("ch04output-transByCust")
```

```pthon
complTrans += [["2015-03-30", "11:59 PM", "76", "63", "1", "0.00"]]
transByCust = transByCust.union(sc.parallelize(complTrans).map(lambda t: (int(t[2]), t)))
transByCust.map(lambda t: "#".join(t[1])).saveAsTextFile("ch04output-transByCust")
```

* 커플 잠옷 세트를 보내기 위한 구매 기록에 76번 고객 추가
* `sc.parallelize(complTrans).map(lambda x:(int(x[2]),x)).collect()` 결과
* [(53, ['2015-03-30', '11:59 PM', '53', '4', '1', '0.00']), (76, ['2015-03-30', '11:59 PM', '76', '63', '1', '0.00'])]
* 새로운 구매기록 transByCust에 추가
* 새로운 파일로 저장

#### aggregateByKey로 키의 모든 값 그루핑

* `zeroValue`를 받아 RDD 값을 병합한다는 점에서 `foldByKey`나 `reduceByKey`와 유사
* 값 타입을 바꿀 수 있다.
* 타입을 변환하는 `변환 함수`와 변환한 값을 두 개씩 하나로 병합하는 `병합 함수`를 인자로

```python
prods = transByCust.aggregateByKey([], lambda prods, tran: prods + [tran[3]],
    lambda prods1, prods2: prods1 + prods2)
prods.collect()
```

* `zeroValue`에 빈 리스트 저장
* 제품 리스트를 추가, RDD의 각 파티션별로 요소를 병합
* 키가 같은 두 제품 리스트를 이어 붙인다.



## 2. 데이터 파티셔닝과 데이터 셔플링

* 데이터 파티셔닝은 데이터를 여러 클러스터 노드로 분할하는 메커니즘
* RDD별로 RDD의 파티션 목록을 보관하며, 각 파티션의 데이터를 처리할 최적 위치를 추가로 저장
* `partitions` 필드는 Array 타입으로, `partiotions.size`피드로 파티션 개수를 알아낼 수 있다.

### 2.1 데이터 partitioner

* `HashPartitioner`
* `RangePartitioner`

* `사용자 정의 Partitioner`는 Pair RDD에만 사용 가능
* `rdd.foldByKey(afunc, new HashPartitioner(200))`

### 2.2 데이터 셔플링

* 파티션 간의 물리적인 데이터 이동

* `aggregateByKey`에서 변환 함수는 각 파티션별로 수행, 병합 함수에서 여러 파티션의 값을 최종 병합하며 셔플링 수행

* 셔플링 시작 전에 수행한 태스크를 맵 태스크

* 바로 다음에 수행한 태스크를 리듀스 태스크

* Partitioner를 명시적으로 변경하면 셔플링이 발생

* 객체가 다르더라도 동일한 파티션 개수를 지정하면 동일한 파티션 번호 사용

* `map`과 `flatMap`은 RDD의 partitioner를 제거

* 연산자의 결과 RDD에 다른 변환 연산자(`aggregateByKey` `foldByKey`)를 사용하면 기본 partitioner를 사용했더라도 셔플링 발생

  ```python
  rdd = sc.parallelize(range(10000))
  rdd.map(lambda x: (x, x*x)).map(lambda (x, y): (y, x)).collect()
  rdd.map(lambda x: (x, x*x)).reduceByKey(lambda v1, v2: v1+v2).collect()
  ```

  * 2번째 줄을 셔플링이 발생하지 않는다.
  * 3번째 줄은 셔플링 발생
  * 2번째 줄에서 map으로 Pair RDD를 생성하며 partitioner 제거 후 map 연산
  * 3번째 줄은 map 변환 후, `reduceByKey` 변환 연산자 사용으로 셔플링 유발

##### RDD의 partitioner를 변경하는 Pair RDD 변환 연산자

* `aggregateByKey` `foldByKey` `reduceByKey` `groupByKey` `join` `leftOuterJoin` `rightOuterJoin` `fullOuterJoin` `subtractByKey`

##### 일부 RDD 변환 연산자

* `subtract` `intersection` `groupWith`

##### `sortByKey` 변환 연산자(셔플링이 무조건 발생)

##### shuffle = True로 설정한 coalesce 연산자

### 2.3 RDD 파티션 변경

#### partitionBy

* Pair RDD에서만 사용 가능
* 파티셔닝에 사용할 Paritioner 객체만 인자로 전달
* Paritioner가 기존과 다르면 셔플링 작업을 스케줄링하고 새로운 RDD 생성

#### coalesce, repartition

* `coalesce`연산자는 파티션 개수를 줄이거나 늘리는 데 사용
  * shuffle = True : coalesce 이전의 변환 연산자들은 원래 파티션 개수 그대로
  * shuffle = False : 모든 변환 연산자는 새롭게 지정된 파티션 개수 사용
* `repartition` 변환 연산자는 단순히 shuffle을 True로 설정해 coalesce를 호출한 결과를 반환

#### repartitionAndSortWithinPartition

* 새로운 Partitioner 객체를 받아 각 파티션 내에서 요소를 정렬
* 셔플링 단계에서 정렬 수행
* repartition을 호출한 후 직접 정렬보다 성능이 좋음
* 셔플링을 항상 수행

### 2.4 파티션 단위로 데이터 매핑

#### mapPartitions, mapPartitionsWithIndex

* `mapPartitions`

  * 매핑 함수를 인수로 받음
  * 매핑 함수는 각 파티션의 모든 요소를 반복문으로 처리하고 새로운 RDD 파티션 생성

* `mapPartitionsWithIndex`

  * 매핑 함수에 파티션 변호가 함께 전달

* `glom`

  * 각 파티션의 모든 요소를 배열 하나로 모으고, 이 배열들을 요소로 포함하는 새로운 RDD 반환

  * 새로운 RDD에 포함된 요소 개수는 이 RDD의 파티션 개수와 동일

  * 기존의 Partitioner를 제거

  * ```python
    import random
    l = [random.randrange(100) for x in range(500)]
    rdd = sc.parallelize(l, 30).glom()
    rdd.collect()
    rdd.count()
    ```

    * l에 들어있는 500개의 요소를 30개의 파티션으로 나눔
    
    

## 3. 데이터 조인, 정렬, 그루핑

```python
transByProd = transByCust.map(lambda ct: (int(ct[1][3]), ct[1]))
totalsByProd = transByProd.mapValues(lambda t: float(t[5])).reduceByKey(lambda tot1, tot2: tot1 + tot2)
```

* 구매 상품 ID와 구매 내역으로 Pair RDD 생성
* `reduceByKey`변환 연산자를 통해 상품 ID별 매출액 합계

```python
products = sc.textFile("edition/ch04/ch04_data_products.txt").map(lambda line: line.split("#")).map(lambda p: (int(p[0]), p))
```

* 상품 정보 파일 불러오기
* (상품 ID, 상품 정보) Pair RDD 생성
* 조인 연산은 Pair RDD에서만 사용 가능
* 조인할 두 RDD의 요소 중에서 키가 중복된 요소는 여러 번 조인한다.
* 파티션 개수 지정 가능
* 지정하지 않으면 첫 번째 RDD 파티션 개수 사용
* 명시적으로 정의하지 않으면 join

* RDBMS `inner join`과 동일
* 키가 동일한 모든 값의 조합이 포함된 Pair RDD를 생성
* 명시적으로 정의하지 않으면 스파크는 새로운 `HashPartitioner`생성

#### leftOuterJoin

* 첫 번째 RDD에만 있는 키의 요소는 결과 RDD에 (key, (None, W)) 타입으로 저장
* 두 번째 RDD에만 있는 키의 요소는 제외

#### rightOuterJoin

* 두 번째 RDD에만 잇는 키의 요소는 결과 RDD에 (key, (None, W)) 타입으로 저장
* 첫 번째 RDD에만 있는 키의 요소는 제외

#### fullOuterJoin

* 두 RDD 중 어느 한쪽에만 잇는 키의 요소는 (key, (None, W)) 또는 (key, (W, None))

```python
totalsAndProds = totalsByProd.join(products)
totalsAndProds.first()
```

* 상품 ID 기준으로 조인
* (상품 ID, (판매액, [상품정보]))

```python
totalsWithMissingProds = products.leftOuterJoin(totalsByProd)
missingProds = totalsWithMissingProds.filter(lambda x: x[1][1] is None).map(lambda x: x[1][0])
from __future__ import print_function
missingProds.foreach(lambda p: print(", ".join(p)))
```

* `leftOuterJoin`으로 상품 정보 RDD에만 있는 상품 ID의 value는 None

* `missingProds`에 판매 정보가 없는 상품 ID 저장

* `rightOuterJoin`

  * ```python
    toalsByProd.leftOuterJoin(products).filter(lambda x: x[1][0] is None).map(lambda x: x[1][1])
    ```

#### subtract, subtractByKey 변환 연산자로 공통 값 제거

* `subtract`
  * 첫 번째 RDD에서 두 번째 RDD의 요소를 제거한 여집합을 반환
  * 일반 RDD에서 사용 가능
  * 요소 전체를 비교해 제거 여부를 판단
* `subractByKey`
  * Pair RDD에서만 사용 가능
  * 첫 번째 RDD의 쌍 중에서 두 번째 RDD에 포함되지 않은 키의 요소들로 RDD 반환
  * 같은 타입의 값을 가질 필요 없음

#### cogroup 변환 연산자로 RDD 조인

* 여러 RDD 값을 키로 그루핑

* 각 RDD의 키별 값을 담은 Iterable 객체를 생성한 후, Iterable 객체 배열을 Pair RDD로 반환

* 최대 세 개까지 조인 가능

* 동일 타입만 가능

* 한 쪽에만 등장한 키의 경우 다른 쪽 RDD Iterator는 비어 있다

* ```python
  prodTotCogroup = totalsByProd.cogroup(products)
  prodTotCogroup.filter(lambda x: len(x[1][0].data) == 0).foreach(lambda x: print(", ".join(x[1][1].data[0])))
  ```

  * Iterator.data로 확인 가능
  * 어제 판매하지 않은 상품들
  * `len(x[1][0].data)==0`상품 정보에는 있지만 판매 상품 데이터에는 없는 상품

* ```python
  totalsAndProds = prodTotCogroup.filter(lambda x: len(x[1][0].data)>0).\
  map(lambda x: (int(x[1][1].data[0][0]),(x[1][0].data[0], x[1][1].data[0])))
  ```

  * 위와 반대
  * 이전 join 연산과 같음

#### intersection 변환 연산자 사용

* 타입이 동일한 두 RDD에서 양쪽 모두에 포함된 공통 요소

* 교집합 RDD 반환

* ```python
  totalsByProd.map(lambda t: t[0]).intersection(products.map(lambda p: p[0]))
  ```

* ```python
  rdd1 = sc.parallelize([1,2,3])
  rdd2 = sc.parallelize([2,3,4])
  rdd1.intersection(rdd2).collect()
  ```

#### cartesian 변환 연산자로 RDD 두 개 결합

* 두 RDD의 카테시안 곱을 계산

* 모든 조합이 튜플 형태로 구성

* ```python
  rdd1 = sc.parallelize([7,8,9])
  rdd2 = sc.parallelize([1,2,3])
  rdd1.cartesian(rdd2).collect()
  rdd1.cartesian(rdd2).filter(lambda el: el[0] % el[1] == 0).collect()
  ```

  * (x1,x2)에서 x1을 x2로 나눈 나머지가 0인 튜플 출력

#### zip 변환 연산자로 RDD 조인

* 모든 RDD에 사용 가능

* 두 RDD의 각 요소를 순서대ㅐ로 짝을 지은 새로운 RDD[(T, U)] 반환

* 동일한 파티션 개수와 모든 파티션이 동일한 개수의 요소를 포함할 때만 가능

* 차례로 결합

* ```python
  rdd1 = sc.parallelize([1,2,3])
  rdd2 = sc.parallelize(["n4","n5","n6"])
  rdd1.zip(rdd2).collect()
  ```

  *  [(1, 'n4'), (2, 'n5'), (3, 'n6')]

* `zipPartiotions`변환 연산자는 모든 파티션이 서로 동일한 개수의 요소가 아니어도 가능
  * 파티션에 포함된 요소를 반복문으로 처리
  * 두 개의 인자를 받아서 사용
    * 첫 번째 인자는 결합할 RDD
    * 두 번째 인자는 조인 함수를 정의해 전달
      * 각 파티션의 요소를 담은 Iterator 객체를 받고 결과 RDD 값을 담은 새로운 Iterator를 반환해야 한다
  * 첫 번째 인자에 `preservesPartitioning`선택 인수 추가 전달 가능 default = False
    * False = 결과 RDD의 Partitioner를 제거, 셔플링 발생
    * True = 조인 함수가 데이터의 파티션을 보존
  * Python에는 아직 적용 불가

### 3.2 데이터 정렬

* `repartitionAndSortWithinPartition`

  * 리파티셔닝과 정렬 작업을 동시에 수행

* `sortByKey`

* `sortBy`

  * ```python
    sortedProds = totalsAndProds.sortBy(lambda t: t[1][1][1])
    sortedProds.collect()
    ```

  * 상품 이름 기준으로 오름차순 정렬

* `groupByKeyAndSortValues`

  * 이차 정렬
  * 그루핑 후, 키별로 정렬
  * (Key, Iterable(Value))로 반환
  * RDD[(K, V)] => RDD[((K, V), Null )] 매핑 (rdd.map())
  * 사용자 정의 Partitioner를 이용해 K를 이용해 파티션을 나눔
  * `repartitionAndSortWithinPartition` 변환 연산자에 사용자 정의 Partitioner를 인수로 전달해 연산자를 호출, 전체 복합 키(K, V)를 기준으로 각 파티션 내 요소들을 정렬
  * 키로 먼저 정렬, 그 다음 Value로 정렬

### 3.3 데이터 그루핑

#### groupByKey나 groupBy 변환 연산자로 데이터 그루핑

* `groupByKey`
  * 동일한 키를 가진 모든 요소를 단일 키-값 쌍으로 모은 Pair RDD를 반환
  * (키1, (V1,V2)), (키2, (V1,V2))
  * (K, Iterable[v]) 타입으로 구성
  * Pair RDD에서 사용
* `groupBy`
  * Pair RDD 뿐만 아니라 일반 RDD에도 사용 가능
  * 일반 RDD를 pair RDD로 변환하고 groupByKey 호출과 같은 결과

#### combineByKey 변환 연산자로 데이터 그루핑

* 각 파티션별로 키의 첫 번째 값에서 최초 결합을 생성

* Pair RDD에 저장된 값들을 결합 값으로 병합
* 결합 값을 최종 결과로 병합
* Partitioner을 명시적으로 지정해야 한다
* mapSideCombine
  * 선택 인수
  * 각 파티션의 결합 값을 계산하는 작업을 셔플링 단계 이전에 수행할지
* Serializer
  * 선택 인수

```python
def createComb(t):
    total = float(t[5])
    q = int(t[4])
    return (total/q, total/q, q, total)
```

* 결합 값을 생성
* total = 구매 금액
* q = 구매 개수
* (1개 구매 금액, 1개 구매 금액, 개수, 총 구매 금액) 반환

```python
def mergeVal(arg,t):
    total = float(t[5])
    q = int(t[4])
    return (min(arg[0],total/q),max(arg[1],total/q),arg[2]+q,arg[3]+total)
```

* 결합 값과 값을 병합
* (최소 구매 금액, 최대 구매 금액, 개수 합, 총 구매 금액 합) 반환

```python
def mergeComb(arg1, arg2):
    return (min(arg1[0],arg2[0]),max(arg1[1],arg2[1]),arg1[2]+arg2[2],arg1[3]+arg2[3])
```

* 결합 값을 서로 병합

```python
avgByCust = transByCust.combineByKey(createComb, mergeVal, mergeComb).\
mapValues(lambda arg: (arg[0],arg[1],arg[2],arg[3],arg[3]/arg[2]))
avgByCust.first()
```

* `combineByKey`변환 연산자 실행
* `lambda (mn,mx,cnt,tot): (mn,mx,cnt,tot,tot/cnt)`
  * 튜플에 상품의 평균 가격 추가

## 4. RDD 의존 관계

#### RDD 의존 관계와 스파크 동작 메커니즘

* 스파크의 실행 모델은 `방향성 비순환 그래프`에 기반

* `DAG`
  * RDD의 의존 관계를 간선으로 정의한 그래프
  * RDD의 변환 연산자를 호출할 때마다 새로운 정점(RDD)과 새로운 간선(의존 관계)이 생성
  * 방향은 자식 RDD(새로운)에서 부모RDD(이전)
  * 계보라고 한다.
  
* 의존 관계 유형에 따라 셔플링 실행 여부 결정
  
  * 조인할 때는 항상 셔플링 발생
  
* 좁은 의존 관계
  * 1-대-1 의존 관계
  
    * 셔플링이 필요하지 않은 모든 변환 연산자
  
  * 범위형 의존 관계
    * 여러 부모 RDD에 대한 의존 관계를 하나로 결합
    * `union` 변환 연산자
    
  * ```python
    import random
    r_list = [random.randrange(10) for x in range(500)]
    rdd1 = sc.parallelize(r_list, 5)
    pairs_rdd1 = rdd1.map(lambda x: (x, x*x))
    reduced = pairs_rdd1.reduceByKey(lambda x1, x2: x1+x2)
    rdd2 = reduced.mapPartitions(lambda x: ["K="+str(k)+",V="+str(v) for (k,v) in x])
    rdd2.collect()
    print(rdd2.toDebugString)
    ```
  
    * 랜덤함수를 통해 5개의 파티션으로 구성된 rdd1 생성하고 Pair RDD 매핑
    * `reduceByKey`를 통해 Key별로 Value를 합산하고 
    * 각 파티션을 매핑하여 파티션을 제거하고 문자열로 구성
  
* 넓은 의존 관계(셔플)

#### 스테이지 & 태스크

* 셔플링이 발생하는 지점을 기준으로 스테이지로 나눈다.
* 스테이지 결과를 중간 파일 형태로 저장
* 각 스테이지와 파티션별로 태스크를 생성해 실행자에 전달
  * 스테이지가 셔플리으로 끝나는 경우, 태스크를 `셔플 - 맵`태스크라고 한다.
  * 스테이지의 모든 태스크가 완료되면 다음 스테이지의 태스크 생성
* 마지막 스테이지의 태스트가 `결과 태스크`

#### 체크포인트

* 장애 발생 이전에 저장된 스냅샷을 사용해 이 지점부터 다시 계산
* 체크포인팅을 실행하면 RDD의 데이터와 계보를 모드 저장한 후, 의존 관계와 부모 RDD 정보 삭제
* `checkpoint`
* `SparkContextsetCheckpointDir` 데이터를 저장할 디렉터리 지정
* `DAG`가ㅏ 길어질 때 사용

## 5. 누적 변수와 공유 변수

* 누적 변수는 스파크 프로그램의 전역 상태를 유지
* 공유 변수로 태그크 및 파티션이 공통으로 사용하는 데이터를 공유

### 5.1 누적 변수

* 더하는 연산만 가능

* `SparkContext.accumulator(initialValue)` 생성

* `sc.accumulator(initialValue, "accumulatorName")` 이름 지정 (파이썬은 불가)

* ```python
  acc = sc.accumulator(0)
  l = sc.parallelize(range(1000000))
  l.foreach(lambda x: acc.add(1))
  acc.value
  ```

  * `acc.value` 100000

* ```python
  l.foreach(lambda x: acc.value)
  ```

  * 오류 발생
  * 실행자가 실제로 더하는 값의 타입과 누적 변수 값의 타입이 다를 경우 `Accumulable` 객체 생성, 스파크 2.0,0 부터는 사용 불가

### 5.2 공유 변수

* 실행자가 수정 불가, 드라이버에서만 생성
* 실해자는 읽기 연산만 가능
* `SparkContext.broadcast(value)` 생성
* `spark.broadcast.compress`공유 변수를 전송하기 전 데이터 압축 여부
* `spark.broadcast.blockSize`공유 변수를 전송하는 데 사용하는 데이터 청크의 크기
* `spark.pythin.worker.reuse`공유 변수 성능에 큰 영향, 워커 재사용 여부(True)
* 공유 변수 값에 접근할 때는 항상 `value` 메서드 사용