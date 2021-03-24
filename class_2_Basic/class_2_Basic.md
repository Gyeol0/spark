<스파크를 다루는 기술>(길벗, 2018)을 학습하고 개인 학습용으로 정리한 내용입니다.

출처 - Petar Zecevic 외 1명. <스파크를 다루는 기술(Spak in Action)>. 이춘오. 길벗(2018)

# 2장 스파크 기초

## 2.1 Basic

### 2.1.1 예제

```python
licLines = sc.textFile("/user/local/spark/LICENSE")
lineCnt = licLines.count()
```

* 스파크가 사용하는 모든 라이브러리와 각 라이브러리가 제공한 라이선스 정보 불러오기
* 줄의 개수 count

```python
bsdLines = licLines.filter(lambda line: "BSD" in line)
bsdLines.count()
```

* **BSD**가 포함된 줄의 개수 count
* **filter**함수는 익명(내부) 함수가 True로 판별한 요소만으로 구성된 새로운 컬렉션을 반환

```python
from __future__ import print_function
bsdLines.foreach(lambda bLine: print(bLine))
```

* bsdLines에 저장된 요소 출력

```python
def isBSD(line):
    return "BSD" in line

bsdLines1 = licLines.filter(isBSD)
bsdLines1.count()
bsdLines.foreach(lambda bLine: print(bLine))
```

* **lambda**를 사용한 익명 함수가 아닌 기명 함수를 정의해 **filter** 가능

* 위와 같은 결과

### 2.1.2 RDD의 개념

* RDD : 스파크의 기본 추상화 객체
* 분산 컬렉션의 성질가 장애 내성으 추상화
* 직관적인 방식으로 대규모 데이터셋에 병렬 연산
* 출력시 순서가 없음

#### 성질

* 불변성(immutable) : 읽기 전용
  * 데이터를 조작하는 다양한 변환 연산자를 제공하지만, 항상 새로운 RDD 객체 생성
  * 불변의 성질
* 복원성(resilient) : 장애 내성
  * 가변성은 대체로 시스템을 복잡하게
  * 불변을 통해 분산 시스템에서 가장 중요한 장애 내성을 보장
  * 유실된 RDD 복구 가능, 데이터 자체 복원이 아님
  * 데이터를 만드는데 사용된 변환 연산자의 로그를 남김
  * 변환 연산자 적용 순서 => **RDD 계보**
  * 장애 발생 시 다시 계산
* 분산(deisributed) : 노드 한 개 이상에 저장된 데이터셋
  * 사용자에게 투명



## RDD 연산자

* 변환 연산자
  * RDD 데이터를 조작하여 새로운 RDD 생성
  * filter, map
* 행동 연산자
  * 연산자를 호출한 프로그램으로 계산 결과 반환
  * 또는 RDD 요소에 특정 작업을 수행하려고 실제 계산을 시작하는 역할
  * count, foreach

### map

* RDD의 모든 요소에 임의의 함수를 적용할 수 있는 변환 연산자
* RDD 타입이 바뀔 수 있음(map 함수가 인자로 받는 함수에 따라)

```python
from __future__ import print_function
numbers = sc.parallelize(range(10, 51, 10))
numbersSquared = numbers.map(lambda num: num * num)
```



