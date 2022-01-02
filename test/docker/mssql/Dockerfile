FROM mcr.microsoft.com/azure-sql-edge

USER root

RUN /opt/mssql/bin/mssql-conf traceflag 3979 on && \
        /opt/mssql/bin/mssql-conf set control.alternatewritethrough 0 && \
        /opt/mssql/bin/mssql-conf set control.writethrough 0

USER mssql
