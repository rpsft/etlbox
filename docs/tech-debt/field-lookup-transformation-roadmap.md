# Roadmap: FieldLookupTransformation — декларативный lookup с поддержкой XML-сериализации

## Описание проблемы

`LookupTransformation<TInput, TSourceOutput>` (`ETLBox/src/Toolbox/DataFlow/LookupTransformation.cs`)
уже поддерживает загрузку словаря из произвольного `IDataFlowSource<TSourceOutput>`, однако конфигурация
сопоставления и обогащения **не поддаётся XML-сериализации**:

- **Атрибутный режим** (`[MatchColumn]` / `[RetrieveColumn]`) требует декорирования C#-типов на этапе
  компиляции — не применим в динамических или конфигурационных сценариях.
- **Func-режим** (`TransformationFunc: Func<TInput, TInput>`) — делегат, который нельзя сериализовать
  в XML или JSON.

Это делает `LookupTransformation` непригодным для использования в сериализованных DataFlow через
`DataFlowXmlReader` (`ETLBox.Serialization/DataFlow/DataFlowXmlReader.cs`).

---

## Предлагаемое решение

Новый компонент `FieldLookupTransformation`, у которого:

1. Конфигурация match/retrieve — обычные POCO-списки со строковыми именами полей (сериализуемы).
2. Свойство `DictionarySource` типа `IDataFlowSource<TSourceOutput>` — deseriализуется через существующий
   механизм `DataFlowXmlReader.SetInterfaceProperty` с атрибутом `type="ConcreteType"` в XML.
3. Опционально: Roslyn-скрипт в виде строки (`EnrichmentScript`) для сложного обогащения — в проекте
   `ETLBox.Scripting`.

---

## Архитектура

### Фаза 1 — Ядро (проект ETLBox)

**Новые файлы:**

- `ETLBox/src/Definitions/DataFlow/Type/LookupMatchColumn.cs`
- `ETLBox/src/Definitions/DataFlow/Type/LookupRetrieveColumn.cs`
- `ETLBox/src/Toolbox/DataFlow/FieldLookupTransformation.cs`

**Сигнатура ключевого класса:**

```csharp
public class FieldLookupTransformation<TInput, TSourceOutput>
    : DataFlowTransformation<TInput, TInput>
{
    // Конфигурация — сериализуемые POCO
    public List<LookupMatchColumn>    MatchColumns    { get; set; } = new();
    public List<LookupRetrieveColumn> RetrieveColumns { get; set; } = new();

    // Кэш загруженных строк словаря
    public List<TSourceOutput> LookupData { get; set; } = new();

    // Интерфейсное свойство — DataFlowXmlReader десериализует через type="MemorySource" и т.п.
    // setter вызывает DictionarySource.LinkTo(LookupBuffer) — как в LookupTransformation.Source
    public IDataFlowSource<TSourceOutput> DictionarySource { get; set; }
}

// Non-generic: ExpandoObject, ExpandoObject
public class FieldLookupTransformation : FieldLookupTransformation<ExpandoObject, ExpandoObject> { }
```

**POCO для конфигурации:**

```csharp
public class LookupMatchColumn
{
    public string InputField  { get; set; }  // поле на входном ряду (TInput)
    public string LookupField { get; set; }  // поле в строке словаря (TSourceOutput)
}

public class LookupRetrieveColumn
{
    public string LookupField { get; set; }  // поле словаря — источник значения
    public string OutputField { get; set; }  // поле входного ряда — цель записи
}
```

**Внутренняя механика** (зеркало `LookupTransformation`):
- `CustomDestination<TSourceOutput> LookupBuffer` — собирает строки из `DictionarySource` в `LookupData`.
- `RowTransformation<TInput, TInput>` с `InitAction = LoadLookupData` — ленивая загрузка при первой строке.
- `protected virtual TInput EnrichRow(TInput row)` — виртуальный для переопределения в scripted-варианте.
- Два пути: `EnrichTyped` (reflection, кэш `PropertyInfo`) и `EnrichDynamic` (`IDictionary<string,object?>`,
  сравнение через `.ToString()` — значения из XML всегда строки).
- `protected TSourceOutput? FindMatch(TInput row)` — вынесен как helper для переиспользования в subclass.

### Фаза 2 — Scripted вариант (проект ETLBox.Scripting)

**Новый файл:** `ETLBox.Scripting/ScriptedFieldLookupTransformation.cs`

```csharp
public class ScriptedFieldLookupTransformation
    : FieldLookupTransformation<ExpandoObject, ExpandoObject>
{
    // Строковый C# Roslyn-скрипт. Globals: dynamic row (мутабельный), dynamic? lookup (null если нет совпадения)
    public string? EnrichmentScript { get; set; }

    protected override ExpandoObject EnrichRow(ExpandoObject row)
    {
        if (string.IsNullOrWhiteSpace(EnrichmentScript))
            return base.EnrichRow(row);  // fallback на field-mapping
        // компиляция и выполнение через ScriptBuilder/ScriptRunner (как в ScriptedRowTransformation)
        ...
    }
}
```

---

## XML-сериализация

### Изменения в DataFlowXmlReader

**Не требуются.** Механизм `SetInterfaceProperty` (строки 437–468) уже обрабатывает интерфейсные
свойства: читает атрибут `type="..."`, резолвит конкретный тип через `GetTypeByName`, создаёт экземпляр
и вызывает setter свойства. `MatchColumns` / `RetrieveColumns` — конкретные `List<T>` — идут через
`CreateList` → `CreateInstance` без доп. логики.

### Пример XML

```xml
<FieldLookupTransformation>
  <DictionarySource type="MemorySource">
    <Data>
      <ExpandoObject>
        <CustomerId>1</CustomerId>
        <CustomerName>Acme Corp</CustomerName>
      </ExpandoObject>
    </Data>
  </DictionarySource>
  <MatchColumns>
    <LookupMatchColumn>
      <InputField>CustomerId</InputField>
      <LookupField>CustomerId</LookupField>
    </LookupMatchColumn>
  </MatchColumns>
  <RetrieveColumns>
    <LookupRetrieveColumn>
      <LookupField>CustomerName</LookupField>
      <OutputField>CustomerName</OutputField>
    </LookupRetrieveColumn>
  </RetrieveColumns>
</FieldLookupTransformation>
```

Для scripted-варианта:

```xml
<ScriptedFieldLookupTransformation>
  <DictionarySource type="CsvSource"><Uri>products.csv</Uri></DictionarySource>
  <MatchColumns>...</MatchColumns>
  <EnrichmentScript>row.Name = lookup?.Name ?? "Unknown";</EnrichmentScript>
</ScriptedFieldLookupTransformation>
```

---

## Известные ограничения

| Ситуация | Поведение |
|---|---|
| `<DictionarySource>` без атрибута `type` | `InvalidDataException` (то же, что и для других интерфейсных свойств) |
| Типизированный source (`MemorySource<MyPoco>`) из XML | Не поддерживается — reader резолвит к `ExpandoObject`; только из кода API |
| Сравнение в dynamic-режиме | `ToString()` на обеих сторонах — значения из XML всегда строки |
| Порядок элементов в XML | `<DictionarySource>` может стоять до `<MatchColumns>` — MatchColumns читаются только при выполнении потока |

---

## План тестирования

### Фаза 1 — `TestTransformations/src/FieldLookupTransformation/`

- `FieldLookupTypedPocoTests.cs` — typed POCO: одна колонка, составной ключ, нет совпадения (проход без изменений)
- `FieldLookupDynamicTests.cs` — ExpandoObject: базовый случай, строковое сравнение типов, нет `DictionarySource`
- `ETLBox.Serialization.Tests/FieldLookupSerializationTests.cs` — XML round-trip с MemorySource и CsvSource

### Фаза 2 — `ETLBox.Scripting.Tests/`

- `ScriptedFieldLookupTransformationTests.cs` — скрипт обогащения, fallback на field-mapping, null-lookup, XML round-trip

---

## Критические файлы для реализации

| Файл | Роль |
|---|---|
| `ETLBox/src/Toolbox/DataFlow/LookupTransformation.cs` | Образец для копирования паттернов |
| `ETLBox.Common/DataFlow/RowTransformation.cs` | Базовый класс для обёртки функции обогащения |
| `ETLBox.Common/DataFlow/CustomDestination.cs` | Паттерн LookupBuffer |
| `ETLBox.Serialization/DataFlow/DataFlowXmlReader.cs` строки 343–491 | Пути десериализации (изменений не требует) |
| `ETLBox.Scripting/ScriptedRowTransformation.cs` | Паттерн ScriptBuilder/Runner для Фазы 2 |
| `ETLBox.Scripting/ScriptBuilder.cs` | Roslyn-инфраструктура компиляции |
