BDAM_Diaosi
===========

Big Data Analysis and Management Project

Author: Xiao Kan (0464716), Zhe Zhou (0457661)

## How to run it

### Compile the Java code and put the jar to hadoop path

```
$ cd path/to/project
$ javac -classpath /usr/local/hadoop/hadoop-core-1.0.4.jar -d . InfoboxGetter.java 
$ javac -classpath /usr/local/hadoop/hadoop-core-1.0.4.jar:. -d . DiaoSiDriver.java 
$ jar -cvfe /usr/local/hadoop/BDAM.jar com.github.diaosi.BDAM.DiaoSiDriver com/
```

### Run hadoop job

```
$ cd /usr/local/hadoop
$ bin/hadoop jar BDAM.jar infoboxgetter -inputreader "StreamXmlRecordReader, begin=<page>,end=</page>"
```

### Check the result //TODO update later

```
$ bin/hadoop dfs -cat /user/hduser/hw1-a-output/part-00000
$ bin/hadoop dfs -cat /user/hduser/hw1-b-output/part-00000
$ bin/hadoop dfs -cat /user/hduser/hw1-c-output/part-00000
```

