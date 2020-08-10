from pyspark import SparkContext
from pyspark.sql.functions import row_number, monotonically_increasing_id, col
from pyspark.sql import SparkSession, Window
import re

"""
Remove characters which is not alphabet and numbers and '-', the assumption is that the valid word should only be 
only alphabetic/numbers with '-' in the string (but not in the beginning and in the end)
"""
def stripNumAlphaNumCharsFromHeadAndTail(word):
    return re.sub(r'[^a-zA-Z0-9-]','', word).strip("-")

"""
Filter words that are not valid
"""
def cleanUp(word):
    strippedWord = stripNumAlphaNumCharsFromHeadAndTail(word)
    if not strippedWord:
        return False
    elif strippedWord[0].isnumeric():
        return False
    else:
        return True
 
"""
Build Inverted index with the dictionary
"""
def buildInvertedIndex(sc):
    spark = SparkSession(sc)
    
    # Load File
    wholeTextFiles = sc.wholeTextFiles("dataset/*")
    dictionary = sc.textFile("dictionary.csv")
    
    # Create python dictionary
    df = spark.read.csv("dictionary.csv", header=False).collect()
    dic = {}
    for row in df:
        dic[row[0]] = row[1]
    
    print ("Start Building Inverted Index")
    
    # Build invered index with dictionary
    invertedIndex = wholeTextFiles \
    .flatMap(lambda fileTuple: [(fileTuple[0].split('/')[-1], line) for line in fileTuple[1].splitlines()]) \
    .flatMap(lambda lineTuple: [(lineTuple[0], word) for word in lineTuple[1].split()]).filter(lambda wordTuple: cleanUp(wordTuple[1])) \
    .map(lambda wordTuple: (stripNumAlphaNumCharsFromHeadAndTail(wordTuple[1]),wordTuple[0])) \
    .map(lambda wordTuple: (dic[wordTuple[0]], wordTuple[1])).sortBy(lambda tup: (int(tup[0]), int(tup[1]))) \
    .map(lambda tup: (tup[0], set([tup[1]]))) \
    .reduceByKey(lambda v1,v2: v1 | v2).map(lambda t: (int(t[0]), sorted([int(v) for v in t[1]]))).sortByKey(True, 1)
      
    # Save file
    invertedIndex.saveAsTextFile("invertedIndex")
    print ("Finish Building Inverted Index")

def main(): 
    sc = SparkContext.getOrCreate()
    buildInvertedIndex(sc)
    sc.stop()
    
if __name__ == "__main__":
    main()