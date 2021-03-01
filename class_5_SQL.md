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



### 연산

* 스파크 SQL 함수는 DataFrame API나 SQL 표현식으로 사용 가능
* **스칼라 함수** : 각 로우의 단일 컬럼 또는 여러 컬럼 값을 계산해 단일 값을 반환하는 함수
  * `abs`, `exp`, `substring` 등
* **집계 함수** : 로우의 그룹에서 단일 값을 계산하는 함수
  * `max`, `min`, `avg` 등
  * 집계 함수는 보통 groupBy와 함께 쓰지만 select나 withColumn 메서드에 사용하면 전체 데이터셋을 대상으로 값을 집계할 수 있다.
* **윈도 함수** : 로우의 그룹에서 여러 결과 값을 계산하는 함수
* **사용자 정의 함수** : 커스텀 스칼라 함수 또는 커스텀 집계 함수



#### 스칼라 및 집계 함수

```python
from pyspark.sql.functions import *

postsDf.filter(postsDf.postTypeId == 1).withColumn("activePeriod", datediff(postsDf.lastActivityDate, postsDf.creationDate)).orderBy(desc("activePeriod")).head().body.replace("&lt;","<").replace("&gt;",">")
```

* `lastActivityDate`, `creationDate`두 날짜 간의 차이 계산
* `activePeriod`으로 내림차순 정렬
* 가장 오랜 기간 논의된 질문

```python
postsDf.select(avg(postsDf.score), max(postsDf.score), count(postsDf.score)).show()
```

* `score`의 평균, 최댓값, count

#### 윈도 함수

* 집계 함수와 유사하지만 단일 결과로만 그루핑하지 않는다.
* **프레임**을 정의할 수 있다.
* **프레임**은 윈도 함수가 현재 처리하는 로우와 관련된 다른 로우 집합으로 정의하며, 이 집합을 현재 로우 계산에 활용할 수 있다.
* 이동평균이나 누적 합 등을 계산
* 보통 서브쿼리를 사용하여 계산하지만 윈도 함수를 이용하여 간단하게 계산 가능
* 사용
  1. 집계 함수나 랭킹 함수, 분서 함수 중 하나를 사용해 Column 정의를 구성
  2. 윈도 사양(WindowSpec 객체)을 생성하고 Column의 over 함수에 인수로 전달
  3. over 함수는 이 윈도 사양을 사용하는 윈도 칼럼을 정의해 반환
* 윈도 사양을 생성하는 방법
  1. `partitionBy` 메서드 사용하여 단일 칼럼 또는 복수 칼럼을 파티션의 분할 기준으로 지정
  2. `orderBy` 메서드로 파티션의 로우를 정렬할 기준을 지정
  3. `partitionBy`와 `orderBy`를 모두 사용
* `rowsBetween(from, to)` `rangeBetween(from, to)` 함수로 프레임에 포함될 로우를 추가적으로 제한 가능
  * `rowsBetween`는 상대 순번, 현재 처리하고 있는 로우 기준

```python
from pyspark.sql.window import Window

winDf = postsDf.filter(postsDf.postTypeId == 1).select(postsDf.ownerUserId, postsDf.acceptedAnswerId, postsDf.score, max(postsDf.score).over(Window.partitionBy(postsDf.ownerUserId)).alias("maxPerUser"))

winDf.withColumn("toMax", winDf.maxPerUser - winDf.score).show(10)
```

```python
max(postsDf.score).over(Window.partitionBy(postsDf.ownerUserId)).alias("maxPerUser")
```

* `ownerUserId`을 기준으로`score`의 최대값, 별칭 `maxPerUser`
* `postTypeId`가 1인 사용자가 올린 질문 중 최고 점수를 계산하고, 해당 사용자가 게시한 다른 질문의 점수와 최고 점수 간 차이



```python
postsDf.filter(postsDf.postTypeId == 1).select(postsDf.ownerUserId, postsDf.id, postsDf.creationDate, lag(postsDf.id, 1).over(Window.partitionBy(postsDf.ownerUserId).orderBy(postsDf.creationDate)).alias("prev"), lead(postsDf.id, 1).over(Window.partitionBy(postsDf.ownerUserId).orderBy(postsDf.creationDate)).alias("next")).orderBy(postsDf.ownerUserId, postsDf.id).show()

```

* `postTypeId`가 1인 사용자가 올린 질문 중 생성 날짜를 기준으로 질문자가 한 바로 전 질문과 바로 다음 질문의 ID를 각 질문별로 출력

* `lag(column, offset, [default])` : 프레임에 포함된 로우 중에서 현재 로우를 기준으로 offset만큼 뒤에 있는 로우 값을 반환, 로우가 없을 시에는 default 반환
* `lead(column, offset, [default])` : 프레임에 포함된 로우 중에서 현재 로우를 기준으로 offset만큼 앞에 있는 로우 값을 반환, 로우가 없을 시에는 default 반환

* `creationDate`를 기준으로 정렬하고 `lag`를 통해 prev 질문, `lead`를 통해 next 질문



#### 사용자 정의 함수

* 스파크에서 지원하지 않는 특정 기능이 필요할 때 사용
* **function** 객체의 **udf** 함수로 생성
* **udf** 인자로 필요한 로직 전달, 최대 10개까지 가능

```python
countTags = udf(lambda tags: tags.count("&lt;"), IntegerType())
postsDf.filter(postsDf.postTypeId == 1).select("tags", countTags(postsDf.tags).alias("tagCnt")).show(10, False)
```

* `countTags` 정의 : &lt count, IntegerType
* `postTypeId`이 1인 사용자가 올린 질문 중에서 태그 개수
* `show` 메서드의 두 번째 인자를 **False**로 하면 출력된 문자열을 중략하지 않도록 한다. 지정하지 않으면 앞쪽 20개 문자까지만 출력하고 나머지는 중략



### 결측값

* N/A, unknown, null, 비어 있는 경우
* DataFrame의 DataFrameNaFunctions를 활용해 결측 값 인스턴스 처리
* 처리 방법
  1. 제거
  2.  다른 상수로 채워 넣기
  3. 다른 상수로 치환

#### 제거

* **drop** 메서드를 인수 없이 호출하면 최소 하나 이상의 칼럼이 na를 가지는 모든 로우 삭제

```python
cleanPosts = postsDf.na.drop()
cleanPosts.count()
```

* **drop** 인자로 `'any'`이면 칼럼 하나라도 null이면, `'all'`이면 모든 칼러이 null이면
* 특정 칼럼 지정 가능

#### 채워 넣기

```python
postsDf.na.fill({"viewCount": 0}).show()
```

* **fill** 함수를 사용해 다른 상수 또는 문자열 상수로 채울 수 있다.
* `viewCount` 칼럼의 null 값을 0으로 채움

#### 치환

```python
postsDf.na.replace(1177, 3000, ["id", "acceptedAnswerId"]).show()
```

* **replace** 함수를 사용해 특정 칼럼의 특정 값을 다른 값으로 치환

* `id`, `acceptedAnswerId` 1177번을 3000번으로 치환



### RDD 변환

* 반대로 DataFrame을 RDD로 변환

```python
postsRdd = postsDf.rdd
```

* 변환된 RDD는 org.apache.spark.sql.Row 타입의 요소로 구성

* DataFrame 데이터와 파티션을 map이나 flatMap, mapPartitions 변환 연산자 등으로 매핑하면 실제 매핑 작업은 하부 RDD에서 실행되어, 변환 연산의 결과 또한 새로운 DataFrame이 아니라 새로운 RDD가 된다.
* DataFrame의 변환 연산자는 DataFrame 스키마(즉, RDD 스키마)를 변경할 수 있다.
  * 칼럼 순서, 개수 타입 변경 가능
  * RDD의 Row 객체를 다른 타입으로 변환 가능
  * 그러나 타입을 변경한 RDD를 다시 DataFrame으로 변환하는 과정 자동화 불가
  * 변환 연산자가 DataFrame 스키마를 변경하지 않으면, DataFrame의 이전 스키마를 그대로 사용해 새 DataFrame 생성 가능

```python
def replaceLtGt(row):
	return Row(
	  commentCount = row.commentCount,
    lastActivityDate = row.lastActivityDate,
    ownerUserId = row.ownerUserId,
    body = row.body.replace("&lt;","<").replace("&gt;",">"),
    score = row.score,
    creationDate = row.creationDate,
    viewCount = row.viewCount,
    title = row.title,
    tags = row.tags.replace("&lt;","<").replace("&gt;",">"),
    answerCount = row.answerCount,
    acceptedAnswerId = row.acceptedAnswerId,
    postTypeId = row.postTypeId,
    id = row.id)

postsMapped = postsRdd.map(replaceLtGt)
```

* 각 로우를 매핑하여 **body**와 **tags** 문자열 변환
* 후에 다시 DataFrame으로 변환

```python
def sortSchema(schema):
	fields = {f.name: f for f in schema.fields}
	names = sorted(fields.keys())
	return StructType([fields[f] for f in names])

postsDfNew = sqlContext.createDataFrame(postsMapped, sortSchema(postsDf.schema))
```

* 그러나 DatafFrame API의 내장 함수, 사용자 정의 함수를 통해 대부분의 매핑 작업을 수행할 수 있기 때문에 RDD로 변환하지 않고 바로 적용할 수 있다.



### 데이터 그루핑

* SQL의 **GROUP BY**와 비슷
* 칼럼 이름 또는 **Column** 객체의 목록을 받고 **GroupedData** 객체를 반환
* **GroupedData** 는 groupBy에 지정한 칼럼들의 값이 모두 동일한 로우 그룹들을 표현한 객체
* 집계 함수를 통해 **groupBy**에 지정한 칼럼들과 집계 결과를 저장한 추가 칼럼으로 구성된 DataFrame 반환

```python
postsDfNew.groupBy(postsDfNew.ownerUserId, postsDfNew.tags, postsDfNew.postTypeId).count().orderBy(postsDfNew.ownerUserId.desc()).show(10)
```

* 작성자, 관련 태그, 포스트의 유형별로 포스트 개수 집계
* 작정자 ID 기준으로 내림차순 정렬

```python
postsDfNew.groupBy(postsDfNew.ownerUserId).agg(max(postsDfNew.lastActivityDate), max(postsDfNew.score)).show(10)
```

* **agg** 함수를 사용해 서로 다른 칼럼의 여러 집계 연산 수행
* Map 객체를 전달해도 가능
* 작성자별로 마지막 수정 날짜, 최고 점수 산출

```python
postsDfNew.groupBy(postsDfNew.ownerUserId).agg({"lastActivityDate": "max", "score": "max"}).show(10)
```

* 위 코드와 동일

```python
postsDfNew.groupBy(postsDfNew.ownerUserId).agg(max(postsDfNew.lastActivityDate), max(postsDfNew.score) > 5).show(10)
```

* 첫 번째 표현식이 더 유연하고 다른 표현식과 연결이 가능하다.

#### 사용자 정의 집계 함수

* 추상 클래스를 상속한 새로운 클래스를 선언한 후, 이 클래스에 입력 스키마와 버퍼 스키마를 정의
* **initialize**, **update**, **merge**, **evaluate** 함수를 구현하는 과정을 거친다.

#### rollup과 cube

* 지정된 칼럼의 부분 집합을 추가로 사용해 집계 연산을 수행
* **cube**는 칼럼의 모든 조합을 대상으로 계산
* **rollup**은 지정된 칼럼 순서를 고려한 순열 사용

```python
smplDf = postsDfNew.where((postsDfNew.ownerUserId >= 13) & (postsDfNew.ownerUserId <= 15))
smplDf.groupBy(smplDf.ownerUserId, smplDf.tags, smplDf.postTypeId).count().show()
```

```python
smplDf.rollup(smplDf.ownerUserId, smplDf.tags, smplDf.postTypeId).count().show()
```

* **rollup**은 groupBy가 반환한 결과에 작성자별 부분 집계 결과, 작성자 및 관련 태그별 부분 집계 결과, 전체 집계 결과를 추가

```python
smplDf.cube(smplDf.ownerUserId, smplDf.tags, smplDf.postTypeId).count().show()
```

* **cube**는 **rollup** 결과에 나머지 부분 집계 결과들을 추가



### 데이터 조인

* 칼럼 이름을 조인 기준으로 사용할 때는 해당 칼럼이 양쪽에 모두 있어야 한다.
* 조인 기준으로 사용할 칼럼 이름이 서로 다르면 **Column** 정의를 사용해야 한다.

```python
itVotesRaw = sc.textFile("edition/ch05/italianVotes.csv").map(lambda x: x.split("~"))
itVotesRows = itVotesRaw.map(lambda row: Row(id=int(row[0]), postId=int(row[1]), voteTypeId=int(row[2]), creationDate=datetime.strptime(row[3], "%Y-%m-%d %H:%M:%S.%f")))
votesSchema = StructType([
  StructField("id", IntegerType(), False),
  StructField("postId", IntegerType(), False),
  StructField("voteTypeId", IntegerType(), False),
  StructField("creationDate", TimestampType(), False)
  ])  

votesDf = sqlContext.createDataFrame(itVotesRows, votesSchema)
```

* 데이터 불러오기
* DataFrame으로 변환

```python
postsVotes = postsDf.join(votesDf, postsDf.id == votesDf.postId)
```

* **id**를 기준으로 조인
* 칼럼 이름이 다르기 때문에 `postsDf.id == votesDf.postId`

```python
postsVotesOuter = postsDf.join(votesDf, postsDf.id == votesDf.postId, "outer")
```

* outer 조인
* **id**, **postId**에 null 값이 포함되어 있음
* null 값도 포함하여 조인



## 5. 2 Dataset

* 도메인 객체에 대한 변환 연산을 손쉽게 표현할 수 있는 API 지원
* 스파크 SQL 실행 엔진의 빠른 성능과 높은 안정성 제공

* 메서드 호출 시 Encoder 객체 전달



## 5.3 SQL

* 스파크 전용 SQL
* 하이브 쿼리 언어(HQL)

### 테이블 카탈로그와 하이브 메타스토어

* 스파크는 사용ㅈ가 등록한 테이블 정보를 **테이블 카탈로그**에 저장
* 하이브 지원 기능이 없는 스파크에서는 테이블 카탈로그를 단순한 인-메모리 Map으로 구현하여, 등록된 테이블 정보를 드라이버의 메모리에만 저장하고 스파크 세션이 종료되면 사라진다.
* 하이브를 지원하는 **SparkSession**에서는 테이브 카탈로그가 하이브 메타스토어를 기반으로 구현
* 하이브 메타스토어는 영구적인 데이터 베이스, 새로 시작해도 DataFrame 정보 유지



#### 테이블 임시 등록

```python
postsDf.registerTempTable("posts_temp")
```

* **posts_temp**라는 이름으로 테이블 임시 등록

#### 테이블 영구 등록

```python
postsDf.write.saveAsTable("posts")
votesDf.write.saveAsTable("votes")
```

* DataFrame의 **write** 메서드로 영구 테이블 등록
* **HiveContext**는 **metastore_db** 폴더 아래 로컬 작업 디렉터리에 **Derby** 데이터베이스를 생성
* hive-site.xml 파일의 hive.metastore.warehouse.dir에 원하는 경로 지정 가능

#### 테이블 카탈로그

```python
spark.catalog.listTables()
```

* 등록되어 있는 테이블 목록
* **isTemporary** 칼럼 True = 임시 테이블, False= 영구 테이블

* **tableType**=MANAGED, 스파크가 해당 테이블의 데이터까지 관리
  * EXTERNAL은 다른 시스템(ex RDBMS)으로 관리하는 테이블

```python
spark.catalog.listColumns("votes")
```

* 특정 테이브르이 칼럼 정보 조회



### SQL 쿼리

* 스파크 SQL을 통해 ALTER TABLE, DROP TABLE 등 DDL 가능

```python
resultDf = sqlContext.sql("select * from posts")
```

* 결과 또한 DataFrame

```python
select substring(title, 0, 70) from posts where postTypeId = 1 order by creationDate desc limit 3;
```

* spark-sql 명령어로 SQL 셸 진입
* 가장 최근에 게시된 질문 제목 3개 출력

```
spark-sql -e "select substring(title, 0, 70) from posts where postTypeId = 1 order by creationDate desc limit 3"
```

* SQL 셸이 들어가지 않고 터미널(ubuntu)에서 바로 실행 가능



### 쓰리프트 서버로 스파크 SQL 접속

* JDBC, ODBC 서버를 이용해 원격지에서 SQL 명령 실행 가능
* 쓰리프트 서버를 이용해 관계형 데이터베이스와 통신할 수 있는 모든 애플리케이션 사용 가능
* 쓰리프트 서버로 전달된 SQL 쿼리는 DataFrame으로 변환 후, RDD 연산으로 변환해 실행하고 실행결과는 다시 JDBC 프로토콜로 반환
* 참조하는 DataFrame은 미리 하이브 메타스토에서 영구 등록해야 한다.



## DataFrame 저장하고 불러오기

### 기본 데이터 소스

#### JSON

* XML을 대체하는 경량 데이터 포맷
* 스파크는 JSON 스키마를 자동으로 유추하여 외부 시스템과 데이터를 주고 받는 포맷으로 적합
* 영구 저장에 사용하기에는 효율이 떨어짐
* 포맷이 간단하고 사용이 간편, 사람이 읽을 수 있는 형태로 데이터 저장

#### ORC

* 칼럼형 포맷
* 로우형 포맷은 각 로우의 데이터를 순차적으로 저장
* 칼럼형 포맷은 각 칼럼의 데이터를 물리적으로 가까운 위치에 저장하는 방식
* ORC는 로우 데이터를 **스트라이프(stripe)** 여러 개로 묶어서 저장하고, 파일 끝에는 **파일 푸터(file footer)**와 **포스트 스크립트(postscript)** 영역이 있다.
* 파일 푸터에는 파일의 스트라이프 목록과 스트라이프별 로우 개수, 각 칼럼의 데이터 타입을 저장
* 포스트 스크립트 영역에는 파일 압축과 관련된 매개변수들과 파일 푸터의 크기 저장

#### 스트라이프

* 기본 크기 250MB
* 각 스트라이프는 다시 인덱스 데이터, 로우 데이터, 스트라이프 푸터로 구성
* 인덱스 데이터는 각 칼럼의 최솟값 및 최댓값과 각 칼럼 내 로우들의 위치 저장
  * 블륨 필터 내장 가능
  * 블륨 필터는 스트라이프에 특정 값의 포함 여부를 빠르게 검사할 수 있는 확률적 자료 구조
  * 특정 스트라이프를 스캔 대상에서 완전히 배제할 수 있어, 테이블 스캔 속도 향상
* 각 데이터 타입에 특화된 Serializer를 사용하여 데이터를 효율적 저장
  * Zlib 또는 Snappy 형식의 압축

#### Parquet

* 불필요한 의존 관계를 가지지 않으며 특정 프레임워크에 종속되지 않음
* 독립성
* 칼럼형 포맷이며 데이터 압축 가능
  * 칼럼별로 압축 방식을 지정할 수 있음
* 중첩된 복합 데이터 구조에 중점을 두고 설게되어서 ORC 파일 포맷보다 이러한 중첩 구조의 데이터 셋을 더 효율적으로 다룰 수 있음
* 각 칼럼의 청크별로 최솟값과 최댓값의 통계ㅡㄹㄹ 저장해 쿼리를 실행할 때 일부 데이터를 건너뛸 수 있도록 연산을 최적화
* 스파크의 기본 데이터 소스



### 데이터 저장

#### Writer

* foramt : 데이터를 저장할 파일 포맷, 데이터 소스 이름 지정(jsom, parquet, orc)
  * default : parquet
* mode : 지정된 테이블 또는 파일이 이미 존재하면 이에 대응해 데이터를 저장할 방식을 지정
  * overwrite
  * append
  * ignore
  * error
  * default : error
* option, options : 데이터 소스를 설정할 매개변수 이름과 변수 값을 추가
  * options 메서드에서는 매개변수의 이름-값 쌍을 Map 형태로 전달
* partitionBy : 복수의 파티션 칼럼을 지정

#### saveAsTable

* DataFrame의 데이터는 write 필드로 제공되는 DataFrameWriter 객체를 사용해 저장

```python
postsDf.write.format("json").saveAsTable("postsjson")

sqlContext.sql("select * from postsjson")
```

* `json` 형태로 저장
* `save`나 `insertInto` 메서드로도 데이터 저장 가능
  * `save`는 데이터를 파일에 저장
  * 나머지는 하이브 테이블로 저장하며 메타스토어 활용

#### insertInto

* 이미 하이브 메타스토어에 존재하는 테이블을 지정해야 한다.
* 이 테이블과 새로 저장할 DataFrame 스키마가 서로 같아야 한다.
* 테이블의 스키마가 DataFrame과 다르면 예외가 발생
* 테이블이 이미 존재하고 포맷도 결정했기 때문에 format과 option에 전달한 매개변수들은 insertInto 메서드에 사용하지 않음

#### save

* 하이브 메타스토어 사용하지 않고 데이터를 직접 파일 시스템에 저장

#### jdbc 메서드로 관계형 데이터 베이스에 데이터 저장

```python
props = {"user": "user", "password": "password"}
postsDf.write.jdbc("jdbc:postgresql:#postgresrv/mydb", "posts", properties=props)
```

* mydb 데이터베이스 아래에 posts라는 PostgreSQL 테이블로 저장
* 데이터베이스 접속에 필요한 속성 `props`

* DataFrame의 파티션이 너무 많으면 부담
  * 모든 파티션이 관계형 데이터베이스와 연결해 각자 데이터를 저장하기 때문



### 데이터 불러오기

* 데이터 저장과 사용법 동일
* schema 함수로 DataFrame 스키마를 지정할 수 있다는 차이점
* 스키마를 직접 지정하면 그만큼 연산 속도 향상
* `read`, `load`
  * `save`와 `load` 비슷

```python
postsDf = sqlContext.read.table("posts")
postsDf = sqlContext.table("posts")
```

#### jdbc 메서드로 관계형 데이터베이스에서 데이터 불러오기

```python
result = sqlContext.read.jdbc("jdbc:postgresql:#postgresrv/mydb", "posts", predicates=["viewCount > 3"], properties=props)
```

* 데이터를 저장할 때와 비슷하지만, 조건절(**WHERE**)을 통해 불러올 데이터셋 범위 축소 가능
* 최소 조회 수를 세 번 이상 기록한 포스트

#### sql 메서드로 등록한 데이터 소스에서 데이터 불러오기

```python
sqlContext.sql("CREATE TEMPORARY TABLE postsjdbc "+
  "USING org.apache.spark.sql.jdbc "+
  "OPTIONS ("+
    "url 'jdbc:postgresql:#postgresrv/mydb',"+
    "dbtable 'posts',"+
    "user 'user',"+
    "password 'password')")
resParq = sql("select * from postsParquet")
```

```python
sqlContext.sql("CREATE TEMPORARY TABLE postsParquet "+
  "USING org.apache.spark.sql.parquet "+
  "OPTIONS (path '/path/to/parquet_file')")
resParq = sql("select * from postsParquet")
```

* 이 방법으로는 조건절 지정 불가능
