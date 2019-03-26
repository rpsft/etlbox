# Release Notes

## Version 1.0.0 (Released)

Initial release working on .NET Core.

## Version 1.0.1 (Released)

Reorganization of namespaces.

## Version 1.1.0 (Releesed)

* `DropDatabaseTask`: static "convenience" method name changed from delete to drop 
* `ConnectionManager` (general) improved:verified that the underlying ADO.NET connection pooling is working (see Issue#1)
* `OdbcConnectionManager`: ETLBox now supports connection via Odbc. (64bit only)
* `AccessOdbcConnectionManager`: ETLBox can now connect to access databases via ODBC (64bit ODBC driver required)
* `DBSource`: now accepts table name (instead of full table definition)
* `ExcelSource`: ETLBox can now read from excel files. 
* `CSVSource<CSVData>`: Adding a generic implementation for the CSVSource.
* `RowTransformation`: Adding a non generic implementation (same as `RowTansformation<string[],string[]`>)
* `DBDestination`: Adding a non generic implemnetation. (same as `DBDestination<string[]`>)

## Version 1.1.1 (Released)
* `DBDestination`: Fixed issue (#4) when destination table has more columns than input type.

## Version 1.1.2 (Unreleased)
* `CSVSource`: Aligning configuration properties with CSVHelper