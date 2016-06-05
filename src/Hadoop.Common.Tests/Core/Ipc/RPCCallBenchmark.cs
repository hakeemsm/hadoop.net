using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using Com.Google.Common.Base;
using Com.Google.Protobuf;
using Org.Apache.Commons.Cli;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Ipc.Protobuf;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;

using Management;

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>Benchmark for protobuf RPC.</summary>
	/// <remarks>
	/// Benchmark for protobuf RPC.
	/// Run with --help option for usage.
	/// </remarks>
	public class RPCCallBenchmark : Tool
	{
		private Configuration conf;

		private AtomicLong callCount = new AtomicLong(0);

		private static ThreadMXBean threadBean = ManagementFactory.GetThreadMXBean();

		private class MyOptions
		{
			private bool failed = false;

			private int serverThreads = 0;

			private int serverReaderThreads = 1;

			private int clientThreads = 0;

			private string host = "0.0.0.0";

			private int port = 0;

			public int secondsToRun = 15;

			private int msgSize = 1024;

			public Type rpcEngine = typeof(WritableRpcEngine);

			private MyOptions(string[] args)
			{
				try
				{
					Options opts = BuildOptions();
					CommandLineParser parser = new GnuParser();
					CommandLine line = parser.Parse(opts, args, true);
					ProcessOptions(line, opts);
					ValidateOptions();
				}
				catch (ParseException e)
				{
					System.Console.Error.WriteLine(e.Message);
					System.Console.Error.WriteLine("Try \"--help\" option for details.");
					failed = true;
				}
			}

			/// <exception cref="Org.Apache.Commons.Cli.ParseException"/>
			private void ValidateOptions()
			{
				if (serverThreads <= 0 && clientThreads <= 0)
				{
					throw new ParseException("Must specify at least -c or -s");
				}
			}

			private Options BuildOptions()
			{
				Options opts = new Options();
				opts.AddOption(OptionBuilder.Create("s"));
				opts.AddOption(OptionBuilder.Create("r"));
				opts.AddOption(OptionBuilder.Create("c"));
				opts.AddOption(OptionBuilder.Create("m"));
				opts.AddOption(OptionBuilder.Create("t"));
				opts.AddOption(OptionBuilder.Create("p"));
				opts.AddOption(OptionBuilder.Create('h'));
				opts.AddOption(OptionBuilder.Create('e'));
				opts.AddOption(OptionBuilder.Create('?'));
				return opts;
			}

			/// <exception cref="Org.Apache.Commons.Cli.ParseException"/>
			private void ProcessOptions(CommandLine line, Options opts)
			{
				if (line.HasOption("help") || line.HasOption('?'))
				{
					HelpFormatter formatter = new HelpFormatter();
					System.Console.Out.WriteLine("Protobuf IPC benchmark.");
					System.Console.Out.WriteLine();
					formatter.PrintHelp(100, "java ... PBRPCBenchmark [options]", "\nSupported options:"
						, opts, string.Empty);
					return;
				}
				if (line.HasOption('s'))
				{
					serverThreads = System.Convert.ToInt32(line.GetOptionValue('s'));
				}
				if (line.HasOption('r'))
				{
					serverReaderThreads = System.Convert.ToInt32(line.GetOptionValue('r'));
				}
				if (line.HasOption('c'))
				{
					clientThreads = System.Convert.ToInt32(line.GetOptionValue('c'));
				}
				if (line.HasOption('t'))
				{
					secondsToRun = System.Convert.ToInt32(line.GetOptionValue('t'));
				}
				if (line.HasOption('m'))
				{
					msgSize = System.Convert.ToInt32(line.GetOptionValue('m'));
				}
				if (line.HasOption('p'))
				{
					port = System.Convert.ToInt32(line.GetOptionValue('p'));
				}
				if (line.HasOption('h'))
				{
					host = line.GetOptionValue('h');
				}
				if (line.HasOption('e'))
				{
					string eng = line.GetOptionValue('e');
					if ("protobuf".Equals(eng))
					{
						rpcEngine = typeof(ProtobufRpcEngine);
					}
					else
					{
						if ("writable".Equals(eng))
						{
							rpcEngine = typeof(WritableRpcEngine);
						}
						else
						{
							throw new ParseException("invalid engine: " + eng);
						}
					}
				}
				string[] remainingArgs = line.GetArgs();
				if (remainingArgs.Length != 0)
				{
					throw new ParseException("Extra arguments: " + Joiner.On(" ").Join(remainingArgs)
						);
				}
			}

			public virtual int GetPort()
			{
				if (port == 0)
				{
					port = NetUtils.GetFreeSocketPort();
					if (port == 0)
					{
						throw new RuntimeException("Could not find a free port");
					}
				}
				return port;
			}

			public override string ToString()
			{
				return "rpcEngine=" + rpcEngine + "\nserverThreads=" + serverThreads + "\nserverReaderThreads="
					 + serverReaderThreads + "\nclientThreads=" + clientThreads + "\nhost=" + host +
					 "\nport=" + GetPort() + "\nsecondsToRun=" + secondsToRun + "\nmsgSize=" + msgSize;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private RPC.Server StartServer(RPCCallBenchmark.MyOptions opts)
		{
			if (opts.serverThreads <= 0)
			{
				return null;
			}
			conf.SetInt(CommonConfigurationKeys.IpcServerRpcReadThreadsKey, opts.serverReaderThreads
				);
			RPC.Server server;
			// Get RPC server for server side implementation
			if (opts.rpcEngine == typeof(ProtobufRpcEngine))
			{
				// Create server side implementation
				TestProtoBufRpc.PBServerImpl serverImpl = new TestProtoBufRpc.PBServerImpl();
				BlockingService service = TestRpcServiceProtos.TestProtobufRpcProto.NewReflectiveBlockingService
					(serverImpl);
				server = new RPC.Builder(conf).SetProtocol(typeof(TestProtoBufRpc.TestRpcService)
					).SetInstance(service).SetBindAddress(opts.host).SetPort(opts.GetPort()).SetNumHandlers
					(opts.serverThreads).SetVerbose(false).Build();
			}
			else
			{
				if (opts.rpcEngine == typeof(WritableRpcEngine))
				{
					server = new RPC.Builder(conf).SetProtocol(typeof(TestRPC.TestProtocol)).SetInstance
						(new TestRPC.TestImpl()).SetBindAddress(opts.host).SetPort(opts.GetPort()).SetNumHandlers
						(opts.serverThreads).SetVerbose(false).Build();
				}
				else
				{
					throw new RuntimeException("Bad engine: " + opts.rpcEngine);
				}
			}
			server.Start();
			return server;
		}

		private long GetTotalCpuTime<_T0>(IEnumerable<_T0> threads)
			where _T0 : Thread
		{
			long total = 0;
			foreach (Thread t in threads)
			{
				long tid = t.GetId();
				total += threadBean.GetThreadCpuTime(tid);
			}
			return total;
		}

		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			RPCCallBenchmark.MyOptions opts = new RPCCallBenchmark.MyOptions(args);
			if (opts.failed)
			{
				return -1;
			}
			// Set RPC engine to the configured RPC engine
			RPC.SetProtocolEngine(conf, typeof(TestProtoBufRpc.TestRpcService), opts.rpcEngine
				);
			RPC.Server server = StartServer(opts);
			try
			{
				MultithreadedTestUtil.TestContext ctx = SetupClientTestContext(opts);
				if (ctx != null)
				{
					long totalCalls = 0;
					ctx.StartThreads();
					long veryStart = Runtime.NanoTime();
					// Loop printing results every second until the specified
					// time has elapsed
					for (int i = 0; i < opts.secondsToRun; i++)
					{
						long st = Runtime.NanoTime();
						ctx.WaitFor(1000);
						long et = Runtime.NanoTime();
						long ct = callCount.GetAndSet(0);
						totalCalls += ct;
						double callsPerSec = (ct * 1000000000) / (et - st);
						System.Console.Out.WriteLine("Calls per second: " + callsPerSec);
					}
					// Print results
					if (totalCalls > 0)
					{
						long veryEnd = Runtime.NanoTime();
						double callsPerSec = (totalCalls * 1000000000) / (veryEnd - veryStart);
						long cpuNanosClient = GetTotalCpuTime(ctx.GetTestThreads());
						long cpuNanosServer = -1;
						if (server != null)
						{
							cpuNanosServer = GetTotalCpuTime(server.GetHandlers());
						}
						System.Console.Out.WriteLine("====== Results ======");
						System.Console.Out.WriteLine("Options:\n" + opts);
						System.Console.Out.WriteLine("Total calls per second: " + callsPerSec);
						System.Console.Out.WriteLine("CPU time per call on client: " + (cpuNanosClient / 
							totalCalls) + " ns");
						if (server != null)
						{
							System.Console.Out.WriteLine("CPU time per call on server: " + (cpuNanosServer / 
								totalCalls) + " ns");
						}
					}
					else
					{
						System.Console.Out.WriteLine("No calls!");
					}
					ctx.Stop();
				}
				else
				{
					while (true)
					{
						Thread.Sleep(10000);
					}
				}
			}
			finally
			{
				if (server != null)
				{
					server.Stop();
				}
			}
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private MultithreadedTestUtil.TestContext SetupClientTestContext(RPCCallBenchmark.MyOptions
			 opts)
		{
			if (opts.clientThreads <= 0)
			{
				return null;
			}
			// Set up a separate proxy for each client thread,
			// rather than making them share TCP pipes.
			int numProxies = opts.clientThreads;
			RPCCallBenchmark.RpcServiceWrapper[] proxies = new RPCCallBenchmark.RpcServiceWrapper
				[numProxies];
			for (int i = 0; i < numProxies; i++)
			{
				proxies[i] = UserGroupInformation.CreateUserForTesting("proxy-" + i, new string[]
					 {  }).DoAs(new _PrivilegedExceptionAction_347(this, opts));
			}
			// Create an echo message of the desired length
			StringBuilder msgBuilder = new StringBuilder(opts.msgSize);
			for (int c = 0; c < opts.msgSize; c++)
			{
				msgBuilder.Append('x');
			}
			string echoMessage = msgBuilder.ToString();
			// Create the clients in a test context
			MultithreadedTestUtil.TestContext ctx = new MultithreadedTestUtil.TestContext();
			for (int i_1 = 0; i_1 < opts.clientThreads; i_1++)
			{
				RPCCallBenchmark.RpcServiceWrapper proxy = proxies[i_1 % numProxies];
				ctx.AddThread(new _RepeatingTestThread_367(this, proxy, echoMessage, ctx));
			}
			return ctx;
		}

		private sealed class _PrivilegedExceptionAction_347 : PrivilegedExceptionAction<RPCCallBenchmark.RpcServiceWrapper
			>
		{
			public _PrivilegedExceptionAction_347(RPCCallBenchmark _enclosing, RPCCallBenchmark.MyOptions
				 opts)
			{
				this._enclosing = _enclosing;
				this.opts = opts;
			}

			/// <exception cref="System.Exception"/>
			public RPCCallBenchmark.RpcServiceWrapper Run()
			{
				return this._enclosing.CreateRpcClient(opts);
			}

			private readonly RPCCallBenchmark _enclosing;

			private readonly RPCCallBenchmark.MyOptions opts;
		}

		private sealed class _RepeatingTestThread_367 : MultithreadedTestUtil.RepeatingTestThread
		{
			public _RepeatingTestThread_367(RPCCallBenchmark _enclosing, RPCCallBenchmark.RpcServiceWrapper
				 proxy, string echoMessage, MultithreadedTestUtil.TestContext baseArg1)
				: base(baseArg1)
			{
				this._enclosing = _enclosing;
				this.proxy = proxy;
				this.echoMessage = echoMessage;
			}

			/// <exception cref="System.Exception"/>
			public override void DoAnAction()
			{
				proxy.DoEcho(echoMessage);
				this._enclosing.callCount.IncrementAndGet();
			}

			private readonly RPCCallBenchmark _enclosing;

			private readonly RPCCallBenchmark.RpcServiceWrapper proxy;

			private readonly string echoMessage;
		}

		/// <summary>
		/// Simple interface that can be implemented either by the
		/// protobuf or writable implementations.
		/// </summary>
		private interface RpcServiceWrapper
		{
			/// <exception cref="System.Exception"/>
			string DoEcho(string msg);
		}

		/// <summary>Create a client proxy for the specified engine.</summary>
		/// <exception cref="System.IO.IOException"/>
		private RPCCallBenchmark.RpcServiceWrapper CreateRpcClient(RPCCallBenchmark.MyOptions
			 opts)
		{
			IPEndPoint addr = NetUtils.CreateSocketAddr(opts.host, opts.GetPort());
			if (opts.rpcEngine == typeof(ProtobufRpcEngine))
			{
				TestProtoBufRpc.TestRpcService proxy = RPC.GetProxy<TestProtoBufRpc.TestRpcService
					>(0, addr, conf);
				return new _RpcServiceWrapper_394(proxy);
			}
			else
			{
				if (opts.rpcEngine == typeof(WritableRpcEngine))
				{
					TestRPC.TestProtocol proxy = RPC.GetProxy<TestRPC.TestProtocol>(TestRPC.TestProtocol
						.versionID, addr, conf);
					return new _RpcServiceWrapper_407(proxy);
				}
				else
				{
					throw new RuntimeException("unsupported engine: " + opts.rpcEngine);
				}
			}
		}

		private sealed class _RpcServiceWrapper_394 : RPCCallBenchmark.RpcServiceWrapper
		{
			public _RpcServiceWrapper_394(TestProtoBufRpc.TestRpcService proxy)
			{
				this.proxy = proxy;
			}

			/// <exception cref="System.Exception"/>
			public string DoEcho(string msg)
			{
				TestProtos.EchoRequestProto req = ((TestProtos.EchoRequestProto)TestProtos.EchoRequestProto
					.NewBuilder().SetMessage(msg).Build());
				TestProtos.EchoResponseProto responseProto = proxy.Echo(null, req);
				return responseProto.GetMessage();
			}

			private readonly TestProtoBufRpc.TestRpcService proxy;
		}

		private sealed class _RpcServiceWrapper_407 : RPCCallBenchmark.RpcServiceWrapper
		{
			public _RpcServiceWrapper_407(TestRPC.TestProtocol proxy)
			{
				this.proxy = proxy;
			}

			/// <exception cref="System.Exception"/>
			public string DoEcho(string msg)
			{
				return proxy.Echo(msg);
			}

			private readonly TestRPC.TestProtocol proxy;
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int rc = ToolRunner.Run(new RPCCallBenchmark(), args);
			System.Environment.Exit(rc);
		}

		public virtual void SetConf(Configuration conf)
		{
			this.conf = conf;
		}

		public virtual Configuration GetConf()
		{
			return conf;
		}
	}
}
