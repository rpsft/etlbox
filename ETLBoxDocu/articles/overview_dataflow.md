# Overview Data Flow

A big part of ETLBox is the Data Flow library. 

## What is a data flow?

You have some data somewhere - stored in some files, a table or somewhere else. 
Now you want to define a pipeline which takes this data, transforms it "on the fly" and writes it into a target 
(this could be again a database, a file or somewhere else). 
On an abstract level this can be seen as an ETL process (ETL = Extract, Transform, Load).
This article shows you how ETLBox can help you with this.

## Namespace

All Data Flow taks reside in the 'ALE.ETLBox.DataFlow' namespace.

## Source components

All dataflow pipelines will need at least one or more sources. Sources are basically everything that can read data from someplace 
(e.g. CSV file or a database table) and then post this data into the pipeline. All sources should be able to read data asynchronously. 
That means, while the component reads data from the source, it simultanously sends the already processed data to components that are connected to source.
There are different build-in data sources, e.g.: `CSVSource`, `DBSource` or `ExelSource`. If you are in need of another source component, you can either extend the 
`CustomSource` or you [open an issue in github](https://github.com/roadrunnerlenny/etlbox/issues) describing your needs. 

Once a source starts reading data, it will start sending data to its connected components. These could be either a Transoformation or Destination.

## Transformations

Transformations always have at least one input and one output. Inputs can be connected either to other transformations or sources, and the output can also connect to other transformations
or to destinations. 
The purpose of a transformation component is to take the data from its input(s) and post the transformed data to its outputs. This is done on a row-by-row basis.
As soon as there is any data in the input, the transformation will start and post the result to the output. 

## Destination components 

Destination components will have normally only one input. They define a target for your data, e.g. a database table or CSV file. Currently, there is `DBDestination` 
and `CSVDestination` implemented. If you are in need of another destination component, you can either extend the `CustomDestination` or you [open an 
issue in github](https://github.com/roadrunnerlenny/etlbox/issues).

Every Destination comes with an input buffer. 

While a Destination for csv target will open a file stream where data is written into it as soon as arrives, a DB target will do this batch-by-batch - therefore, 
it will wait until the input buffer reaches the batch size (or the data is the last batch) and then insert it into the database using a bulk insert. 




