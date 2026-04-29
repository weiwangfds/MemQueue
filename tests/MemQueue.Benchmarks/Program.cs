using BenchmarkDotNet.Running;

// MemQueue 性能基准测试入口
// 运行所有基准测试: dotnet run -c Release
// 筛选特定基准测试: dotnet run -c Release -- --filter "*PartitionLog*"
// 列出所有基准测试: dotnet run -c Release -- --list flat
BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
