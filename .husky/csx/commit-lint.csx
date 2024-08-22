#!/usr/bin/env dotnet-script

using System.Text.RegularExpressions;

const string CommitTypes = "build|feat|ci|chore|docs|fix|perf|refactor|revert|style|test";

if (Args.Count != 1)
{
    Console.WriteLine("Please provide the path to the commit message file.");
    return 1;
}
private var msg = File.ReadAllLines(Args[0])[0];

if (Regex.IsMatch(msg, "^(Revert|Merge branch|Merge remote-tracking branch)"))
{
    return 0;
}

bool CheckLength(string message) => message.Length >= 1 && message.Length <= 90;
bool CheckType(string message) => Regex.IsMatch(message, $"^({CommitTypes})[:\\(]");
bool CheckScopeAndColon(string message) => Regex.IsMatch(message, $"^({CommitTypes})(\\(.+\\))?:");
bool CheckSubject(string message) => Regex.IsMatch(message, $"^({CommitTypes})(\\(.+\\))?: .{{4,}}");
bool CheckEnding(string message) => !Regex.IsMatch(message, @"[.\s]$");

string errorMessage;
if (!CheckLength(msg))
{
    errorMessage = $"Commit message is {msg.Length}, must be between 1 and 90 characters.";
}
else if (!CheckType(msg))
{
    errorMessage = $"Commit type is invalid. Should be one of: {CommitTypes} e.g: 'feat(scope): subject' or 'fix: subject'";
}
else if (!CheckScopeAndColon(msg))
{
    errorMessage = "Scope format is incorrect, should be non-empty string e.g: 'feat(scope): subject'";
}
else if (!CheckSubject(msg))
{
    errorMessage = "Subject must be at least 4 characters long and follow the type/scope. e.g: 'feat(scope): subject' or 'fix: subject'";
}
else if (!CheckEnding(msg))
{
    errorMessage = "Commit message must not end with a period or whitespace.";
}
else
{
    return 0;
}

Console.ForegroundColor = ConsoleColor.Red;
Console.WriteLine($"Invalid commit message: {errorMessage}");
Console.ResetColor();
Console.ForegroundColor = ConsoleColor.Gray;
Console.WriteLine("more info: https://www.conventionalcommits.org/en/v1.0.0/");

return 1;
