using System;

namespace ALE.ETLBox.ConnectionManager {
    public interface IConnectionManager : IDisposable {
        IDbConnectionString ConnectionString { get; set; }
        void Open();
        void Close();

    }
}
