using System.Data.Common;
using ETLBox.Primitives;

namespace EtlBox.Database.Tests.Infrastructure.Containers
{
    public interface IContainerManager : IAsyncDisposable
    {
        /// <summary>
        /// Провайдер БД
        /// </summary>
        ConnectionManagerType ConnectionType { get; }

        /// <summary>
        /// Удалить БД
        /// </summary>
        /// <param name="database">БД</param>
        void DropDatabase(string database);

        /// <summary>
        /// Выполнить команду в контейнере
        /// </summary>
        /// <param name="sql"></param>
        void ExecuteCommand(string sql);

        /// <summary>
        /// Получение строки подключения с текущей БД
        /// </summary>
        /// <returns></returns>
        string GetConnectionString();

        /// <summary>
        /// Получение билдера соединения
        /// </summary>
        /// <returns></returns>
        DbConnectionStringBuilder GetConnectionBuilder();

        /// <summary>
        /// Получение IConnectionManager по контейнеру
        /// </summary>
        /// <returns></returns>
        IConnectionManager GetConnectionManager();

        /// <summary>
        /// Установка текущей БД
        /// </summary>
        /// <param name="database"></param>
        void SetDatabase(string database);

        /// <summary>
        /// Сбрасывает User и Password в исходное состояние (owner)
        /// </summary>
        void UseDefaults();

        /// <summary>
        /// Старт контейнера
        /// </summary>
        /// <returns></returns>
        public Task StartAsync();

        /// <summary>
        /// Создать БД
        /// </summary>
        /// <param name="database"></param>
        void CreateDatabase(string database);
    }
}
