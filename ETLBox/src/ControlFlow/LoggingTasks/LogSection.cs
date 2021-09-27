using ETLBox.ControlFlow;

/* Unmerged change from project 'ETLBox (netstandard2.1)'
Before:
using System;
using Microsoft.Extensions.Logging;
After:
using Microsoft.Extensions.Logging;
using System;
*/

/* Unmerged change from project 'ETLBox (net5.0)'
Before:
using System;
using Microsoft.Extensions.Logging;
After:
using Microsoft.Extensions.Logging;
using System;
*/
using System;

namespace ETLBox.Logging
{
    /// <summary>
    /// A log section will execute your code block, wrapping the action with log messages indicating start and end.
    /// </summary>
    public class LogSection : ControlFlowTask
    {
        /// <inheritdoc/>
        public override string TaskName { get; set; }

        public LogSection(string name) {
            this.TaskName = name;
        }

        public void Execute(Action task) {
            LogInfo("{action} log section.", "START");
            task.Invoke();
            LogInfo("{action} log section.", "END");
        }

        public void Execute<t1>(Action<t1> task, t1 param1) {
            LogInfo("{action} log section.", "START");
            task.Invoke(param1);
            LogInfo("{action} log section.", "END");
        }

        public void Execute<t1, t2>(Action<t1, t2> task, t1 param1, t2 param2) {
            LogInfo("{action} log section.", "START");
            task.Invoke(param1, param2);
            LogInfo("{action} log section.", "END");
        }

        public static void Execute(string name, Action task) =>
           new LogSection(name).Execute(task);

        public static void Execute<t1>(string name, Action<t1> task, t1 param1) =>
           new LogSection(name).Execute<t1>(task, param1);

        public static void Execute<t1, t2>(string name, Action<t1, t2> task, t1 param1, t2 param2) =>
            new LogSection(name).Execute<t1, t2>(task, param1, param2);

    }
}