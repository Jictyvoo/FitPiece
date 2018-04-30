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

## Next Features
* Friendly-URL
* Database Modules