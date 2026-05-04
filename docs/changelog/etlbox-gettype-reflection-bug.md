# Баг в DataFlowXmlReader.GetType: отсутствует обработка ReflectionTypeLoadException

> **Status: COMPLETED** (2026-04-12) — fixed in commit `5c3314e3 fix(RSSL-11572): DataFlowXmlReader reflection robustness`

## Описание проблемы

В `ALE.ETLBox.Serialization.DataFlow.DataFlowXmlReader` метод `GetType` (статический) вызывал `Assembly.GetTypes()` для всех загруженных сборок без обработки `ReflectionTypeLoadException`. При этом аналогичный метод `GetDataFlowTypes` в том же классе корректно обрабатывал исключения через `SafeGetTypes`.

Когда тестовый хост загружал сборки, ссылающиеся на `Microsoft.Testing.Platform` (транзитивная зависимость `Microsoft.NET.Test.Sdk`), десериализация DataFlow-пакетов из XML падала с `ReflectionTypeLoadException`.

## Обходной путь (устранён)

В `EtlDataFlowStep.RecreateDataFlow` был добавлен временный обработчик `AppDomain.CurrentDomain.AssemblyResolve`, который при неудачной загрузке сборки пытался найти уже загруженную сборку с совместимым именем (без учёта версии).

## Применённое исправление

В методе `DataFlowXmlReader.GetType` вызов `assembly.GetTypes()` заменён на `SafeGetTypes(assembly)`:

```csharp
foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
{
    var types = SafeGetTypes(assembly).Where(t => t.Name == typeName);
    // ...
}
```

`SafeGetTypes` обрабатывает `ReflectionTypeLoadException` и возвращает типы, которые удалось загрузить:

```csharp
private static Type[] SafeGetTypes(Assembly assembly)
{
    try { return assembly.GetTypes(); }
    catch (ReflectionTypeLoadException ex)
    {
        return ex.Types.Where(t => t is not null).ToArray()!;
    }
}
```

## Затронутый пакет

`ETLBox.Classic.Serialization`
