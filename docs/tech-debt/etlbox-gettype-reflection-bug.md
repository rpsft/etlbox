# Баг в DataFlowXmlReader.GetType: отсутствует обработка ReflectionTypeLoadException

## Описание проблемы

В `ALE.ETLBox.Serialization.DataFlow.DataFlowXmlReader` метод `GetType` (статический) вызывает `Assembly.GetTypes()` для всех загруженных сборок без обработки `ReflectionTypeLoadException`. При этом аналогичный метод `GetDataFlowTypes` в том же классе корректно обрабатывает исключения через catch-all блок.

Когда тестовый хост загружает сборки, ссылающиеся на `Microsoft.Testing.Platform` (транзитивная зависимость `Microsoft.NET.Test.Sdk`), десериализация DataFlow-пакетов из XML падает с `ReflectionTypeLoadException`.

## Текущий обходной путь

В `EtlDataFlowStep.RecreateDataFlow` добавлен временный обработчик `AppDomain.CurrentDomain.AssemblyResolve`, который при неудачной загрузке сборки пытается найти уже загруженную сборку с совместимым именем (без учёта версии). Это предотвращает исключение при разрешении типов, но не решает корневую проблему в EtlBox.

## Необходимое исправление в EtlBox

В методе `DataFlowXmlReader.GetType` нужно добавить обработку `ReflectionTypeLoadException` аналогично `GetDataFlowTypes`:

```csharp
// Текущий код (проблемный):
IEnumerable<Type> source = from t in assemblies[i].GetTypes()
    where t.Name == typeName
    select t;

// Исправленный вариант:
Type[] assemblyTypes;
try
{
    assemblyTypes = assemblies[i].GetTypes();
}
catch (ReflectionTypeLoadException ex)
{
    assemblyTypes = ex.Types.Where(t => t is not null).ToArray()!;
}

IEnumerable<Type> source = from t in assemblyTypes
    where t.Name == typeName
    select t;
```

## Затронутый пакет

`ETLBox.Classic.Serialization` версии `1.16.1-RSSL-11572.1` (внутренний NuGet-пакет).
