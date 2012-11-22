BDAM_Diaosi
===========

Big Data Analysis and Management Course Project

Author: floodfill, ConanChou

## Task description
Identify pages that have infoboxes. You will scan the Wikipedia pages and generate a CSV file named infobox.csv with the following format for each line corresponding to a page that contains an infobox: <cade>page_id, infobox_text</code>
If a page does not have an infobox, it will not have a line in the CSV file. The infobox_text should contain all text for the infobox, including the template name, attribute names and values.
Note that the id for a Wikipedia page is its title.

## How to run it

### The most simple way

Create a new job flow in Amazon Elastic Map/Reduce with following configurations

1. Hadoop version: 1.0.3
2. Custom jar file: `s3n://diaosi-mapreduce/pro1.jar`
3. Jar arguments: `s3n://diaosi-mapreduce/raw_data s3n://<your-bucket-name>/<your-output-folder>`

### More sophisticated

1. Create a new Java Project in Eclipse
2. Import the src folder to the project
3. Add to build path dependency libraries where you can find them in the folder `libs`
4. Export runnable jars with com.github.diaosi.BDAM.mapreduce.InfoboxGetter as the class to be launched
5. Upload the jar you just generated to Amazon S3
6. (Optional and we already have this done)
Extract Bzip2-compressed wikipedia dumps to raw xml files and upload them to Amazon S3,
or use `s3n://diaosi-mapreduce/raw_data` as the input
7. Go to Amazon Elastic Map/Reduce, create a new job flow, run the jar with the first parameter as the input path and second one as the output path
8. Keep your finger crossed while the job flow is running until results are generated

## Check the result

1. Switch to your S3 bucket and find results listed in the folder you set up before
2. They should be in CSV format that can be open by any spreedsheet softwares
3. Note that Microsoft Excel is not powerful enough to handle opening any UTF-8 encoded csv files
4. Also note that different line separators have been used in the generated csv files, we use `\r\n` as the real line separtor and `\n` is what we used inside the infobox
