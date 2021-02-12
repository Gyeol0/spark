<스파크를 다루는 기술>(길벗, 2018)을 학습하고 개인 학습용으로 정리한 내용입니다.

출처 - Petar Zecevic 외 1명. <스파크를 다루는 기술(Spak in Action)>. 이춘오. 길벗(2018)

# 5장 스파크 SQL

## 5.1 DataFrame

* 모든 칼럼의 타입을 미리 지정
* DataFrame 생성 방법
  * 기존 RDD를 변환하는 방법
  * SQL 쿼리를 실행하는 방법
  * 외부 데이터에서 로드하는 방법

### RDD에서 DataFrame 생성

* 가장 많이 사용

* 비정형 데이터에 DataFrame API를 사용하려면 먼저 RDD로 로드하고 변환한 후, DataFrame 생성

* 생성 방법

  * 로우의 데이터를 튜플 형태로 저장한 RDD 사용
  * 케이스 클래스 사용
  * 스키마를 명시적으로 지정

* 첫 번째와 두 번째 방법은 스키마를 간접적으로 지정(추론) 한다.

* ```python
  Rows = sc.textFile("italianPosts.csv")
  PostSplit = Rows.map(lambda x: x.split("~"))
  ```

  * `commentCount` 포스트에 달린 댓글 개수
  * `lastActivityDate` 마지막 수정 날짜
  * `owenerUserId` 포스트를 게시한 사용자 ID
  * `body` 질문 및 답변 내용
  * `score` '좋아요', '싫어요' 계산한 총 점수
  * `creationDate` 생성 날짜
  * `viewCount` 조회 수
  * `title` 질문 제목
  * `tags` 질문에 달린 태그
  * `answerCount` 답변 개수
  * `acceptedAnswerId` 답변이 제출된 경우 채택된 답변의 ID
  * `postTypeId` 유형, 1: 질문, 2: 답변
  * `id` 포스트 고유 ID

### 첫 번째 생성 방법

* 로우의 데이터를 튜플 형태로 저장한 RDD 사용

* 배열을 튜플로 변환 후, DataFrame 생성

* ```python
  PostRDD = PostSplit.map(lambda x: (x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8],x[9],x[10],x[11],x[12]))
  PostDf = PostRDD.toDF(["commentCount", "lastActivityDate", "ownerUserId", "body", "score", "creationDate", "viewCount", "title", "tags", "answerCount", "acceptedAnswerId", "postTypeId", "id"]) # 컬럼 이름 설정
  # .toDF() 기본형
  PostDf.show(10)
  ```

* ```python
  PostDf.printSchema()
  ```

  * 스키마를 확인하여 컬럼 별 데이터 타입 확인 가능

* 컬럼 데이터 타입을 모두 수정해야함



### 두 번째 생성 방법 & 세 번째 생성 방법

* 케이스 클래스 사용

* ```python
  from pyspark.sql import Row
  from datetime import datetime
  ```

* ```python
  def toInt(val):
    try:
      return int(val)
    except ValueError:
      return None
  ```

  * int 타입으로 변환
  * NULL로 인한 에러는 `None`
  * int 타입에 long 타입 포함

* ```python
  def toTime(val):
    try:
      return datetime.strptime(val, "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
      return None
  ```

  * 타임스탬프 타입으로 변환
  * NULL로 인한 에러는 `None`

* ```python
  def ToPost(row):
    r = row.split("~")
    return Row(
      toInt(r[0]),
      toTime(r[1]),
      toInt(r[2]),
      r[3],
      toInt(r[4]),
      toTime(r[5]),
      toInt(r[6]),
      toInt(r[7]),
      r[8],
      toInt(r[9]),
      toInt(r[10]),
      toInt(r[11]),
      int(r[12]))
  rowRDD = Rows.map(lambda x: ToPost(x))
  ```

  * 데이터 타입 변환
  * `Id`는 NULL이 없으므로 `int()`사용
  * NULL이 있으면 `pyspark`에서는 `toDF()` 불가
    * 스키마를 지정해서 `DataFrame`을 생성해야 한다.

* ```python
  from pyspark.sql.types import *
  postSchema = StructType([
    StructField("commentCount", IntegerType(), True),
    StructField("lastActivityDate", TimestampType(), True),
    StructField("ownerUserId", LongType(), True),
    StructField("body", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("creationDate", TimestampType(), True),
    StructField("viewCount", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("tags", StringType(), True),
    StructField("answerCount", IntegerType(), True),
    StructField("acceptedAnswerId", LongType(), True),
    StructField("postTypeId", LongType(), True),
    StructField("id", LongType(), False)
    ])
  
  PostDfStruct = sqlContext.createDataFrame(rowRDD, postSchema)
  ```

  * 컬럼명, 스키마 정의 후 DataFrame 생성
  * `sqlContext`의 `createDataFrame`사용해서 생성
  * `nullable`  = `True`는 NULL 허용, `False`는 NULL 불가 

* ```python
  PostDfStruct.printSchema()
  PostDfStruct.columns
  PostDfStruct.dtypes
  ```

  * 스키마, 컬럼명, 데이터 타입 확인
  
  

### DataFrame API

* DataFrame의 `DSL`과 RDB의 `SQL`과 유사한 기능
* RDD와 마찬가지로 `불변성`과 `지연 실행`
  * 반드시 새로운 DataFrame으로 변환

#### select

```python
postsDf = PostDfStruct
postsIdBody = postsDf.select("id", "body")
postsIdBody.show(5)
```

* `id`와 `body` 컬럼 select

  * ```python
    postsIdBody = postsDf.select(postsDf["id"], postsDf["body"])
    ```

  * 같은 결과

#### drop

```python
postIds = postsIdBody.drop("body")
```

* 선택한 하나의 컬럼을 제외한 컬럼 반환

#### filter

* `where`과 동일하게 동작

```python
from pyspark.sql.functions import *
postsIdBody.filter(instr(postsIdBody["body"], "Italiano") > 0).count()
```

* `body`컬럼에 `Italiano`란 단어가 포함되어 있는 행의 count
* `instr()`특정 단어의 위치를 찾아주는 함수

```python
noAnswer = postsDf.filter((postsDf["postTypeId"] == 1) & isnull(postsDf["acceptedAnswerId"]))
```

* `postTypeId` 1 : 질문
* `acceptedAnswerId`가 `NULL`
* 채택된 답변이 없는 질문만 필터링

```python
TenQs = postsDf.filter(postsDf["postTypeId"] == 1).limit(10)
```

* `상위 10개`의 질문을 담은 `DataFrame`

#### withColumn

* `withColumnRenamed(변경할 컬럼 이름, 새로운 이름)`

  * ```python
    TenQsRn = TenQs.withColumnRenamed("ownerUserId", "owner")
    ```

  * `ownerUserId` 컬럼을 `owner`으로 변경

* `withColumn(추가할 컬럼 이름, 계산식)`

  * ```python
    postsDf.filter(postsDf.postTypeId == 1).withColumn("ratio", postsDf.viewCount / postsDf.score).where("ratio < 35").show()
    ```

  * `viewCount` / `score`를 계산해서 `ratio`생성

  * `ratio`가 35 미만인 질문

#### 정렬

* 한 개 이상으 컬럼 이름 또는 `Column` 표현식을 받고 이를 기준으로 데이터를 정렬
* `Column` 클래스에 asc나 desc 연산자를 통해 오름차순, 내림차순 지정 가능, default = asc

* `orderBy`
* `sort`

```python
postsDf.filter(postsDf.postTypeId == 1).orderBy(postsDf.lastActivityDate.desc()).limit(10).show()
```

```python
postsDf.filter(postsDf.postTypeId == 1).orderBy(postsDf.lastActivityDate.desc()).limit(10).show()
```

* 같은 결과
* 마지막 수정 날짜를 기준으로 내림차순 정렬
* 최근에 수정한 상위 열 개 질문 출력


