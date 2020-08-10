from pyspark import SparkContext
from pyspark.sql.functions import row_number, monotonically_increasing_id
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
    if not strippedWord: # Return false when the string is empty after removing invalid characters
        return False
    elif strippedWord[0].isnumeric(): # Return false when the stripped word starts with numbers
        return False
    else:
        return True
"""
Build Dictionary
"""
def buildDictionary(sc):
    # Load File
    textFiles = sc.textFile("dataset/*")
    spark = SparkSession(sc)
    
    
    print ("Start Building Dictionary")
    # Create distinct words
    sortedKeys = textFiles.flatMap(lambda line: line.split()).filter(cleanUp).map(lambda word: (stripNumAlphaNumCharsFromHeadAndTail(word), _)).groupByKey().sortByKey()
    
    # Create (word, wordId) pairs
    df = spark.createDataFrame(sortedKeys, ["word","dummy"]).drop("dummy").withColumn("wordId", row_number().over(Window.orderBy(monotonically_increasing_id())) - 1)
    
    # Write dictionary to a single csv file
    df.toPandas().to_csv('dictionary.csv', index=False, header=False)
    
    print ("Finish Building Dictionary")

def main(): 
    sc = SparkContext(master='local').getOrCreate()
    buildDictionary(sc)
    sc.stop()

if __name__ == "__main__":
    main()