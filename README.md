BDAM_Diaosi
===========

Big Data Analysis and Management Course Project

Author: Xiao Kan (0464716), Zhe Zhou (0457661)

# Task description
Identify pages that have infoboxes. You will scan the Wikipedia pages and generate a CSV file named infobox.csv with the following format for each line corresponding to a page that contains an infobox: <cade>page_id, infobox_text</code>
If a page does not have an infobox, it will not have a line in the CSV file. The infobox_text should contain all text for the infobox, including the template name, attribute names and values.
Note that the id for a Wikipedia page is its title.


## How to run it

1)Create a new Java Project in Eclipse
2)Import the src folder to the project
3)Add to build path dependency libraries where you can find them in the folder "libs"
4)Export runnable jars with com.github.diaosi.BDAM.mapreduce.InfoboxGetter

```
$ cd /usr/local/hadoop
$ bin/hadoop jar BDAM.jar infoboxgetter -inputreader "StreamXmlRecordReader, begin=<!-- <page>,end=</page> -->"
```

### Check the result //TODO update later

```
$ bin/hadoop dfs -cat /user/hduser/hw1-a-output/part-00000
$ bin/hadoop dfs -cat /user/hduser/hw1-b-output/part-00000
$ bin/hadoop dfs -cat /user/hduser/hw1-c-output/part-00000
```

