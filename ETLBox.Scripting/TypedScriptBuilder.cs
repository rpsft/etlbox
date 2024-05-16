using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using JetBrains.Annotations;
using Microsoft.CodeAnalysis.CSharp.Scripting;
using Microsoft.CodeAnalysis.Scripting;
using Microsoft.CodeAnalysis.Scripting.Hosting;

namespace ALE.ETLBox.Scripting
{
    /// <summary>
    /// Helper class to create a script with a typed global type.
    /// </summary>
    [PublicAPI]
    public class TypedScriptBuilder
    {
        private readonly GlobalsTypeInfo _globalsTypeInfo;

        /// <summary>
        /// Default constructor.
        /// </summary>
        /// <param name="globalsTypeInfo">Script "Global" type information</param>
        public TypedScriptBuilder(GlobalsTypeInfo globalsTypeInfo)
        {
            _globalsTypeInfo = globalsTypeInfo;
        }

        /// <summary>
        /// Copy and add references to the script.
        /// </summary>
        /// <param name="assemblies">List of additional <see cref="Assembly"/> object to reference from script</param>
        /// <returns>Copy of original script builder with references added</returns>
        public TypedScriptBuilder WithReferences(IEnumerable<Assembly> assemblies)
        {
            return new TypedScriptBuilder(
                new GlobalsTypeInfo(
                    assembly: _globalsTypeInfo.Assembly,
                    reference: _globalsTypeInfo.Reference,
                    type: _globalsTypeInfo.Type,
                    referencedAssemblies: _globalsTypeInfo.ReferencedAssemblies.Concat(assemblies)
                )
            );
        }

        /// <summary>
        /// Create script with arguments of given type and return type of object.
        /// </summary>
        /// <param name="scriptContent">Script source</param>
        /// <returns></returns>
        public ScriptRunner<object> CreateRunner(string scriptContent) =>
            CreateRunner<object>(scriptContent);

        /// <summary>
        /// Create script with arguments of given type and return type of T.
        /// </summary>
        /// <param name="scriptContent">Script source</param>
        /// <typeparam name="TOutput">Return type</typeparam>
        /// <returns></returns>
        public ScriptRunner<TOutput> CreateRunner<TOutput>(string scriptContent)
        {
            //ref: https://github.com/dotnet/roslyn/blob/main/docs/wiki/Scripting-API-Samples.md
            var options = ScriptOptions.Default
                .AddImports("System")
                .AddImports("System.Text")
                .AddReferences(_globalsTypeInfo.ReferencedAssemblies)
                .AddReferences(_globalsTypeInfo.Reference);

            using var loader = new InteractiveAssemblyLoader();
            loader.RegisterDependency(_globalsTypeInfo.Assembly);

            var script = CSharpScript.Create<TOutput>(
                scriptContent,
                options,
                globalsType: _globalsTypeInfo.Type,
                assemblyLoader: loader
            );

            return new ScriptRunner<TOutput>(script, _globalsTypeInfo);
        }
    }
}
