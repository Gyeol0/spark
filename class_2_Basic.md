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





*

*

