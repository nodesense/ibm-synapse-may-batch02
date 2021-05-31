# Databricks notebook source
print ("Hello World")

# COMMAND ----------

# variables

name = "Python"
age = 25
salary = 28899.65
graduate = True 

print (name)

# COMMAND ----------

# types, Python does't support static type, only dynamic typing supported

# duck, duck typing

name = "Python"
print ( type (name) ) # string
print ( type (True) ) # bool
print ( type (10) ) # Int
print ( type (12.343) ) # flat



# COMMAND ----------

name = "Python"
print (name, type(name)) # Python, string
print(name.upper())
name = 10
print (name, type(name)) # 10, int
# crash, due to type error
print(name.upper()) # crash

# COMMAND ----------

# block statement
# if and block statements
# in C/c++/java/C#, curly brace used, 
# in python , curly brace not used.
# will use colon (:) 
# BLOCKS are identified using Indentation / SPACE/TAB
# use SPACE, som editors converts TAB to space..
n = 11
if (n %  2 == 0):
  print("Even")
  print("N is", n)
else:
  print ("Odd")
  print ("N is ", n)
  
  

# COMMAND ----------

# if expression..
# if expression returns a value

n = 10
result = "Even" if (n % 2 == 0) else "Odd"
print(result)

# COMMAND ----------

# python functions/methods/reusable block of code
# Python functions are basically object, First class citizen
def add(a, b):
  print (a, b)
  return a + b

print(add) # print 
print(add.__name__) # add
# assign 
add2 = add



# COMMAND ----------

def add(a, b):
  print (a, b)
  return a + b

# function call, parameter called from left to right
print (add(10, 20)) # a = 10, b = 20
# by parameter name
print (add (a = 10, b= 20)) # a = 10, b = 20

print (add (b = 20, a= 10)) # a = 10, b = 20

# mixed type, argument list, named arguent list
print (add (10, b= 20)) # a = 10, b = 20

# COMMAND ----------

# variable number of arguments
# pass 0, 1, or N arguments, the arguments are passed as collection, tuple
def sum(*numbers):
  s = 0
  print("*numbers", numbers)
  for n in numbers:
    s += n
  
  return s

print(sum()) # 0
print(sum(10)) # 10
print(sum(10,20,30, 40))  # 100


# COMMAND ----------

# lambda, annoymous function
# technically, it is  a function, no name
# it can accept, return values
# one line function
# look at name power
def power(n):
  # multi line possible
  print ("N is ", n)
  return n * n

# lamnda function
# also a function with single line, no block, no name
pow = lambda n: n * n

print("def power ", power.__name__)
print("lambda pow ", pow.__name__)

print ("power ", power(5))
print ("pow", pow(5))

power2 = power
pow2 = pow

print ("power ", power2(5))
print ("pow", pow2(5))

# COMMAND ----------

# higher order function
# a function that accept another function as input is called higher order function

def square(n):
  print ('square', n)
  result = n * n
  return result

# *numbers, all input parameter
def sum(func, *numbers):
  s = 0
  for n in numbers:
    s += n
    
  return s

# are passing square func reference to sum function as first arg
# use case: return sum of square of the number
print (sum(square, 10, 20, 30))

# cube , lambda doesn't have name, we no need define a function with name, adding 10000 of function in code
# use case, return sum of cube of the number
r2 = sum (lambda x: x * x * x, 10, 20 ,30)
print (r2)
# sqrt of the number

# COMMAND ----------

# customer.csv
    Mary,28,    3456.54
Joe,35,6000              
Jhon,    45, 8000  

Venkat, 30, 9000
"""
lambda line: line.strip()
lambda line:  line.split(",")
lambda line: line.isEmpty() # true or false
"""

# COMMAND ----------

name = 'python'
name = "Python"

name = """
  Python 
  line 2
"""

# COMMAND ----------

# Line continuation \  Option 1
# Python thinks the EOL, end of hte line is statemetn completion
# NO SPACE After \
b = 20 *\
30 +\
10

print(b)

# COMMAND ----------

# Line continuation using Paranthesis ( ) Option 2
# one expression enclosed in paranthesis
b = (
20
   *
30   +
  10
)

print(b)

# COMMAND ----------

# List
# collection of elements
numbers = [10,20,30,40,50]
names = ["python", "spark", "java", "jvm", "pyspark"]
print (numbers)
print ("len", len(numbers))

# accessor by using index, starting from 0, left to right, positive index 
print (numbers[0]) # 10
print(names[2]) # java
# range of elements
print ( names[0:] ) # returns all the list members
# [lower bound: upper bound]m return elements from lower bound up to upper bound - 1
print (names[0:3])
print (names[3:5])

print ("-" * 20)
# access elements using negative indexes, right to left
print (numbers[-1]) # last element
print (numbers[0: -1])  #everything except the last number
print (numbers[-3: -1])

# COMMAND ----------

# list is mutable
# we can change, add, delete, update

numbers = [10,20,30,40,50]
names = ["python", "spark", "java", "jvm", "pyspark"]

names[2] = "Scala" # java text replaced with Scala
names.append("delta")
print (names)

names.remove("jvm")

print(names)

names.reverse()

print(names)

# member/element check if present or not
result = "python" in names
print(result) # true

print ("kafka" in names) # false


# COMMAND ----------

# Tuple, immutable
# sequence of elements
# CREATE, CANNOT UPDATE, DELETE, APPEND members

names = () # empty tuple
print (type(names))

names = ("Python") # string, NOT A TUPLE
print (type(names)) # str # NOT A TUPLE

# to define tuple with single element, tuple with  1 element
names = ("Python",)
print (type(names))

# all list member accesor can be used 0:, 0:-1, 3:5, positive and negative indexes
# we cannot change values

names = ("python", "spark", "java")
# won't work, since tuple is immutable
# names[1]= "pyspark"

# COMMAND ----------

# map/dict, key and value types
countries = { 'IN': 'India', 'UK': 'United Kingdom'   }

print(countries)

print(countries['UK'])

# add new counties
countries['USA'] = 'United States'

print(countries)

print ("keys ", countries.keys())
print ("values ", countries.values())

print ("items", countries.items() ) # list of (key,value) tuples

del countries['USA']

print (countries)

# COMMAND ----------

# classes 

# blueprint for objects [/chocholate mold instrument, 5 cm length, 3 cm width, 2 cm depth]
# object factory, create objects [chocholate made out of mold, 5 cm length, 3 cm width, 2 cm depth]

# a blueprint is known as class in python / blueprint = class
# a class/blueprint, has attributes [length, widht, depth, color, weight, etc]
# a class/blueprint may have behaviour [Car is class, start, stop, drive() are behaviour]
# blueprint/mold
class Product:
  # python, constructors are known as initializer
  # python doesn't have this ref implicitly assinged
  # instead it is passed as ALWAYS first parameter, conventionally known as self
  # No access specifier like private,protected
  def __init__(self, length, width, color):
    # self.length, etc known as attributes of object
    self.length = length
    self.width = width
    self.color = color
    
  def getColor(self):
    return self.color
  
  def setColor(self, color):
    self.color = color
  
# create an object p1, chocholate 1 is made, Product is object factory
p1 = Product(10, 5, "red") # new Product in C++/C#/Java/JS

# self shall be passed implicitly to getColor()
print (p1.length, p1.getColor() )

p1.setColor("green")
print (p1.length, p1.getColor() )

# COMMAND ----------


