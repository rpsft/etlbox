# Getting started

Welcome to the getting started pages of ETLBox. This artice will give you a brief overview how ETLBox is organized. 
See the Table Of Content if you already know what you are looking for. 

## ETLBox Components

ETLBox is split into two main components: **Data Flow Tasks** and **Control Flow Tasks** . Some tasks in the Control Flow part are for logging purposes only.
As there are advanced logging capabilities in ETLBox, logging itself is treated in separate articles.

## Clean coding

At first glance, it can look like that some of the taks are not really adding big value to your project. But in accordance to clean coding, 
having written everything in a format which is easy-to-read and easy-to-understand, you are able to produce code that is easier to maintain. Having your code "clean" 
allows you to focus on the important parts of your logic and not be distracted by code lines that your brain need to "unravel" every time you go over them.
If you are interested in clean coding, you should read the "bible" on that topic, written by 
[Robert C. Martin: Clean Code](https://www.amazon.de/gp/product/0132350882/ref=as_li_tl?ie=UTF8&camp=1638&creative=6742&creativeASIN=0132350882&linkCode=as2&tag=andreaslennar-21&linkId=CAKVL4PO6YCRW53L)

### Books written by me

If you are able to understand and read german, I recommend you mein own book: 
[Mehr als Clean Code - Gedanken zur Softwareentwicklung](https://www.amazon.de/gp/product/3735736513/ref=as_li_tl?ie=UTF8&camp=1638&creative=6742&creativeASIN=3735736513&linkCode=as2&tag=andreaslennar-21&linkId=D6HR6S6YAQ65Q3S6)

# Getting started

## Overview Data Flow Tasks

All components in the Data Flow allow you to create your ETL (Extract, Transform, Load) pipeline - 
where data is extracted from the source(s), asynchrounously transformed and then loaded into your destinations.
Plese read the [Overview Data Flow](overview_dataflow.md) to get started. [There is also an Example Data Flow](example_basics.md).
To understand the dataflow components, you can also visit the API reference and look at the description and details of each dataflow component.

## Overview Control Flow Tasks

You will find an introduction into the Control Flow Tasks [in the article Overview Control Flow](overview_controlflow.md).
This will give you all the basics you need to understand how the Control Flow tasks are designed.
If you want to dig deeper, please see the API reference for detailled information about the tasks. 
If you are in need of some examples of how to use Control Flow tasks, [see the Example Control Flow](example_controlflow.md)

## Overview Logging 

All Control Flow and Data Flow Tasks come with the ability to produce log. 
There are also some special task that enables you to create or query the log tables easily. 
To get an introduction into logging, [please have a look at the Overview Logging](overview_logging.md)
To see a simple and working example of ETL code producing some log information, [see the Example for Logging](example_logging.md).
All logging capabilites are based on nlog. You can [visit the NLog homepage](https://nlog-project.org) if you are interested in more details how to set up and configure NLog.

## See the video

To get a quick overview and a basic introduction into ETLBox, you can [watch the video on Youtube.](https://www.youtube.com/watch?v=CsWZuRpl6PA)
You'll find the [full code of the video in the basic example](example_basics.md).

# API Reference

If you are in doubt how to use a certain task, you can have a look at the API reference. All property and method names should be self explanatory and 
already give you a quite good understanding of the code.

## Creating your own task

It is possible to create your own tasks. If you are in need for further details, please give me some feedback via github (open an issue) and 
I will happily give you detailled instructions how to do so. 









