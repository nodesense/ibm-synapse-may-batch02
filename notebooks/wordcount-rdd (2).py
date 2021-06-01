# Databricks notebook source
# /FileStore/tables/words.txt

fileRdd = sc.textFile("/FileStore/tables/words.txt")

print ("filedata", fileRdd.collect())

# COMMAND ----------

# strip down white space around
stripWhiteSpaceRdd = fileRdd.map (lambda line: line.strip()) # remove white space around
print ("stripWhiteSpaceRdd", stripWhiteSpaceRdd.collect())

# COMMAND ----------

# remove the empty lines
nonEmptyLinesRdd = stripWhiteSpaceRdd.filter (lambda line: line != "")
print ("nonEmptyLinesRdd", nonEmptyLinesRdd.collect())

# COMMAND ----------

# split the lines into   list of words
listOfWordsRdd = nonEmptyLinesRdd.map (lambda line: line.split(' '))
print ("listOfWordsRdd", listOfWordsRdd.collect())

# COMMAND ----------

# convert list of words into words , or flatten the list
wordsRdd = listOfWordsRdd.flatMap(lambda listOfWords: listOfWords) # returns individual words back
print ("wordsRdd", wordsRdd.collect())

# COMMAND ----------

# now bring a structure like (key,value) tuple/paired rdd
# key is the actual word, the value is occurance, 1
# convert word into tuple (apple, 1)
keyedRdd = wordsRdd.map (lambda word: (word, 1) )
print ("keyedRdd", keyedRdd.collect())
 
# TAB, Ctrl +SPACE, TAB KEY 3 times

# COMMAND ----------

# count the words by using reduceByKey
# each word is key
# for each word, on second time if the word appears, the lambda function shall be invoked.
# accumulator is word count of individual word
# value is  1 which is from the tuple (scala, 1)
wordsCountRdd = keyedRdd.reduceByKey(lambda accumulator, value: accumulator + value)
print ("wordsCountRdd", wordsCountRdd.collect())


# COMMAND ----------

# same like above code, break with print statement
def accumulatorSum(accumulator, value):
  print ("--begin accumulator %s, value %s" %  (accumulator, value))
  accumulator += value
  print ("--end accumulator %s, value %s" %  (accumulator, value))
  return accumulator


"""
(python, 1) <-- first time, doesn't call accumulator function, it takes value as initial accumulator value
scala <-- first time,  doesn't call accumulator function, it takes value as initial accumulator value
python <-- second time, repeats.. accumulator already has value 1, now it calls accumulatorSum(1, 1), 1 + 1 = 2, now value 2 is initialized in table
python <-- third time.. reapt..accumulator already has value 2, now it calls accumulatorSum(2, 1), 2 + 1 = 3, now value 3 is initialized in table

table accumulator/virtual 

word       accumulator
python       3
scala        1


Collect() 

[(python, 3), (scala, 1)]
"""


# is called when the word is repeatly coming 2nd time onwards
wordsCountRdd = keyedRdd.reduceByKey(accumulatorSum)
print ("wordsCountRdd", wordsCountRdd.collect())

# COMMAND ----------


