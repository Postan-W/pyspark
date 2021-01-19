import os
import shutil

from pyspark import SparkContext

inputpath = './tempfiles/origin.txt'
outputpath = './tempfiles/result.txt'

sc = SparkContext('local', 'wordcount')

# 读取文件
input = sc.textFile(inputpath)
# 切分单词。把每一行的值给line，再用" "分割每一行的单词
words = input.flatMap(lambda line: line.split(" "))
print(words)
# 转换成键值对并计数
# counts = words.map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y)
#
#
#
# # 将统计结果写入结果文件
# counts.saveAsTextFile(outputpath)