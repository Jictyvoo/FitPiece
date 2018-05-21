# Fit_Piece
A mini-framework to use in your project of pure php, or with another frameworks. Totally rewritable and customizable

## How to Use

First, you need to copy all files into the directory that you want to use this code. After that, the first step is to edit the confi.json, and after this, edit pagesInformation.json in layout directory.

### Page Informations File
pageInformations file contains all needed informations about your page files, like page title, file location and others. This configuration file serves to facilitate to load all your pages more easily.

#### Pages
To add a page, you only need to write his informations at this file, and create the page's main file.

#### Dashboards
This part of JSON file indicates where is your page dashboard, to load it. So, the purpose of that is to write only one time the page header, and configurations of all pages, like a navbar, and load its in all your pages. You can put more than one dashboard into your project.

All dashboards are splited into two file with same name, only differing in it end. All dashboards need to be in a directory with the same name as the files. Here's a example:

* Default (directory)
* Default_TOP.php (layout of headers and other things in page's top)
* Default_BOT.php (layout of footers and other things in page's bottom)

### DAO Classes
First thing first you need to load DatabaseController module from modules loader. After that, use generateDAO function giving table name as parameter.

All DAO Classes have a predefined functions, there is:
* insert($valuesArray) => Used to insert a new line into the table
* update($valuesArray, $whereCommand) => Used to update a table line 
* delete($condition) => Used to delete a table line
* select($columns, $whereCommand) => Used to search lines in table
* selectAll($whereCommand) => used to search all columns in all lines of table

To use insert and update functions, can use them with associative arrays, or just values in a normal array. If use a normal array the columns used was all less first if the values's array length was less than columns existing into table. And for every value less, less columns will be used in the command.

## Next Features
* Friendly-URL
* Database Encrypt Function