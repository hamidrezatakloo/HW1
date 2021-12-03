from pyspark import SparkContext 
from operator import add 
import re 
 
sc = SparkContext("local","justApp")  
  
file = sc.textFile("./ShamsDaftar5.txt") 
 
a =( 
file.flatMap(lambda x: re.split("\s+",x)) 
 
 .filter(lambda x : x and x[0].isalpha()) 
 
 .map(lambda x: (x,1)) 
 ) 
b = a.reduceByKey(add) 
 
b.saveAsTextFile("./out") 
