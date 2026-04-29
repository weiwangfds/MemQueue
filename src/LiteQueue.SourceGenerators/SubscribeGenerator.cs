using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace LiteQueue.SourceGenerators;

[Generator]
public class SubscribeGenerator : IIncrementalGenerator
{
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var subscribeClasses = context.SyntaxProvider
            .ForAttributeWithMetadataName(
                "LiteQueue.SubscribeAttribute",
                static (node, _) => node is ClassDeclarationSyntax,
                static (ctx, _) => ExtractSubscribeInfo(ctx))
            .Where(static info => info != null)
            .Collect();

        context.RegisterSourceOutput(subscribeClasses, static (spc, infos) =>
        {
            GenerateRegistration(spc, infos);
        });
    }

    private static SubscribeInfo ExtractSubscribeInfo(GeneratorAttributeSyntaxContext context)
    {
        var classSyntax = (ClassDeclarationSyntax)context.TargetNode;
        var classSymbol = context.TargetSymbol as INamedTypeSymbol;
        if (classSymbol == null)
            return null;

        var attributeData = context.Attributes.FirstOrDefault();
        if (attributeData == null)
            return null;

        string topic = null;
        if (attributeData.ConstructorArguments.Length > 0)
            topic = attributeData.ConstructorArguments[0].Value as string;

        if (string.IsNullOrEmpty(topic))
            return null;

        string groupId = null;
        bool autoCommit = true;

        foreach (var namedArg in attributeData.NamedArguments)
        {
            switch (namedArg.Key)
            {
                case "GroupId":
                    groupId = namedArg.Value.Value as string;
                    break;
                case "AutoCommit":
                    autoCommit = namedArg.Value.Value is bool b && b;
                    break;
            }
        }

        string messageTypeFullName = null;
        string messageTypeName = null;
        string messageTypeNamespace = null;

        var handlerInterface = classSymbol.AllInterfaces
            .FirstOrDefault(i => i.IsGenericType
                && i.Name == "IMessageHandler"
                && i.ContainingNamespace.Name == "Abstractions"
                && i.ContainingNamespace.ContainingNamespace?.Name == "LiteQueue");

        if (handlerInterface != null)
        {
            var typeArg = handlerInterface.TypeArguments[0];
            messageTypeFullName = typeArg.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            messageTypeName = typeArg.Name;
            messageTypeNamespace = typeArg.ContainingNamespace?.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
        }

        var ns = classSymbol.ContainingNamespace?.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
        if (ns == "<global namespace>")
            ns = null;

        return new SubscribeInfo
        {
            ClassName = classSymbol.Name,
            Namespace = ns,
            FullyQualifiedName = classSymbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
            Topic = topic,
            GroupId = groupId,
            AutoCommit = autoCommit,
            MessageTypeFullName = messageTypeFullName,
            MessageTypeName = messageTypeName,
            MessageTypeNamespace = messageTypeNamespace,
            HasHandlerInterface = handlerInterface != null,
        };
    }

    private static void GenerateRegistration(SourceProductionContext context, IList<SubscribeInfo> infos)
    {
        if (infos.Count == 0)
            return;

        var sb = new StringBuilder();
        sb.AppendLine("using Microsoft.Extensions.DependencyInjection;");
        sb.AppendLine("using Microsoft.Extensions.Hosting;");
        sb.AppendLine("using LiteQueue.Abstractions;");
        sb.AppendLine("using LiteQueue.Models;");
        sb.AppendLine();
        sb.AppendLine("namespace LiteQueue;");
        sb.AppendLine();
        sb.AppendLine("public static partial class LiteQueueSubscriberExtensions");
        sb.AppendLine("{");
        sb.AppendLine("    public static IServiceCollection AddLiteQueueSubscribers(this IServiceCollection services)");
        sb.AppendLine("    {");

        int index = 0;
        foreach (var info in infos)
        {
            if (!info.HasHandlerInterface)
                continue;

            sb.AppendLine("        services.AddScoped<" + info.FullyQualifiedName + ", " + info.FullyQualifiedName + ">();");

            string safeName = info.ClassName.Replace("<", "_").Replace(">", "_").Replace(".", "_");
            string hostedServiceName = safeName + "_HostedSubscriber_" + index;

            sb.AppendLine("        services.AddSingleton<IHostedService>(sp => new " + hostedServiceName + "(sp));");
            index++;
        }

        sb.AppendLine("        return services;");
        sb.AppendLine("    }");
        sb.AppendLine("}");

        index = 0;
        foreach (var info in infos)
        {
            if (!info.HasHandlerInterface)
                continue;

            string safeName = info.ClassName.Replace("<", "_").Replace(">", "_").Replace(".", "_");
            string hostedServiceName = safeName + "_HostedSubscriber_" + index;

            sb.AppendLine();
            sb.AppendLine("file class " + hostedServiceName + " : IHostedService");
            sb.AppendLine("{");
            sb.AppendLine("    private readonly IServiceScopeFactory _scopeFactory;");
            sb.AppendLine("    private readonly IServiceProvider _serviceProvider;");
            sb.AppendLine("    private Task _subscribeTask;");
            sb.AppendLine("    private CancellationTokenSource _cts;");
            sb.AppendLine();
            sb.AppendLine("    public " + hostedServiceName + "(IServiceProvider serviceProvider)");
            sb.AppendLine("    {");
            sb.AppendLine("        _serviceProvider = serviceProvider;");
            sb.AppendLine("        _scopeFactory = serviceProvider.GetRequiredService<IServiceScopeFactory>();");
            sb.AppendLine("    }");
            sb.AppendLine();
            sb.AppendLine("    public Task StartAsync(CancellationToken cancellationToken)");
            sb.AppendLine("    {");
            sb.AppendLine("        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);");
            sb.AppendLine("        var bus = _serviceProvider.GetRequiredService<IBus>();");
            sb.AppendLine("        var topic = \"" + info.Topic + "\";");

            if (info.GroupId != null)
            {
                sb.AppendLine("        var groupId = \"" + info.GroupId + "\";");
                sb.AppendLine("        var autoCommit = " + info.AutoCommit.ToString().ToLowerInvariant() + ";");
                sb.AppendLine("        _subscribeTask = bus.SubscribeAsync<" + info.MessageTypeFullName + ">(topic, groupId, autoCommit, async (msg, ctx, ct) =>");
            }
            else
            {
                sb.AppendLine("        _subscribeTask = bus.SubscribeAsync<" + info.MessageTypeFullName + ">(topic, async (msg, ctx, ct) =>");
            }

            sb.AppendLine("        {");
            sb.AppendLine("            using var scope = _scopeFactory.CreateScope();");
            sb.AppendLine("            var handler = scope.ServiceProvider.GetRequiredService<" + info.FullyQualifiedName + ">();");
            sb.AppendLine("            await handler.HandleAsync(msg, ctx, ct);");
            sb.AppendLine("        }, _cts.Token);");
            sb.AppendLine("        return Task.CompletedTask;");
            sb.AppendLine("    }");
            sb.AppendLine();
            sb.AppendLine("    public async Task StopAsync(CancellationToken cancellationToken)");
            sb.AppendLine("    {");
            sb.AppendLine("        _cts?.Cancel();");
            sb.AppendLine("        if (_subscribeTask != null)");
            sb.AppendLine("        {");
            sb.AppendLine("            try { await _subscribeTask; } catch (Exception ex) { System.Diagnostics.Debug.WriteLine($\"LiteQueue subscriber error: {ex.Message}\"); }");
            sb.AppendLine("        }");
            sb.AppendLine("    }");
            sb.AppendLine("}");

            index++;
        }

        context.AddSource("LiteQueueSubscriberExtensions.g.cs", SourceText.From(sb.ToString(), Encoding.UTF8));
    }

    private sealed class SubscribeInfo
    {
        public string ClassName { get; set; } = "";
        public string Namespace { get; set; }
        public string FullyQualifiedName { get; set; } = "";
        public string Topic { get; set; } = "";
        public string GroupId { get; set; }
        public bool AutoCommit { get; set; } = true;
        public string MessageTypeFullName { get; set; }
        public string MessageTypeName { get; set; }
        public string MessageTypeNamespace { get; set; }
        public bool HasHandlerInterface { get; set; }
    }
}
