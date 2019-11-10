# Getting started

Welcome to the getting started pages of ETLBox. This artice will give you a brief overview how ETLBox is organized. 

## ETLBox Components

ETLBox is split into two main components: **Data Flow Tasks** and **Control Flow Tasks** . Some tasks in the Control Flow part are for logging purposes only.
As there are advanced logging capabilities in ETLBox, logging itself is treated in separate articles.

## Overview Data Flow Tasks

All components in the Data Flow allow you to create your ETL (Extract, Transform, Load) pipeline - 
where data is extracted from the source(s), asynchrounously transformed and then loaded into your destinations.
Plese read the [Overview Data Flow](overview_dataflow.md) to get started. [There is also an Example Data Flow](examples/example_basics.md).
To understand the dataflow components, you can also visit the API reference and look at the description and details of each dataflow component.

## Overview Control Flow Tasks

You will find an introduction into the Control Flow Tasks [in the article Overview Control Flow](overview_controlflow.md).
This will give you all the basics you need to understand how the Control Flow tasks are designed.
If you want to dig deeper, please see the API reference for detailled information about the tasks. 
If you are in need of some examples of how to use Control Flow tasks, [see the Example Control Flow](examples/example_controlflow.md)

## Overview Logging 

All Control Flow and Data Flow Tasks come with the ability to produce log. 
There are also some special task that enables you to create or query the log tables easily. 
To get an introduction into logging, [please have a look at the Overview Logging](overview_logging.md)
To see a simple and working example of ETL code producing some log information, [see the Example for Logging](examples/example_logging.md).
All logging capabilites are based on nlog. You can [visit the NLog homepage](https://nlog-project.org) if you are interested in more details how to set up and configure NLog.

## See the video

To get a quick overview and a basic introduction into ETLBox, you can [watch the video on Youtube.](https://www.youtube.com/watch?v=CsWZuRpl6PA)
You'll find the [full code of the video in the basic example](examples/example_basics.md).

# API Reference

If you are in doubt how to use a certain task, you can have a look at the API reference. All property and method names should be self explanatory and 
already give you a quite good understanding of the code.

## Creating your own task

It is possible to create your own tasks. If you are in need for further details, please give me some feedback via github (open an issue) and 
I will happily give you detailled instructions how to do so. 









