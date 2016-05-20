using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>Benchmark for protobuf RPC.</summary>
	/// <remarks>
	/// Benchmark for protobuf RPC.
	/// Run with --help option for usage.
	/// </remarks>
	public class RPCCallBenchmark : org.apache.hadoop.util.Tool
	{
		private org.apache.hadoop.conf.Configuration conf;

		private java.util.concurrent.atomic.AtomicLong callCount = new java.util.concurrent.atomic.AtomicLong
			(0);

		private static java.lang.management.ThreadMXBean threadBean = java.lang.management.ManagementFactory
			.getThreadMXBean();

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

			public java.lang.Class rpcEngine = Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.WritableRpcEngine
				));

			private MyOptions(string[] args)
			{
				try
				{
					org.apache.commons.cli.Options opts = buildOptions();
					org.apache.commons.cli.CommandLineParser parser = new org.apache.commons.cli.GnuParser
						();
					org.apache.commons.cli.CommandLine line = parser.parse(opts, args, true);
					processOptions(line, opts);
					validateOptions();
				}
				catch (org.apache.commons.cli.ParseException e)
				{
					System.Console.Error.WriteLine(e.Message);
					System.Console.Error.WriteLine("Try \"--help\" option for details.");
					failed = true;
				}
			}

			/// <exception cref="org.apache.commons.cli.ParseException"/>
			private void validateOptions()
			{
				if (serverThreads <= 0 && clientThreads <= 0)
				{
					throw new org.apache.commons.cli.ParseException("Must specify at least -c or -s");
				}
			}

			private org.apache.commons.cli.Options buildOptions()
			{
				org.apache.commons.cli.Options opts = new org.apache.commons.cli.Options();
				opts.addOption(org.apache.commons.cli.OptionBuilder.create("s"));
				opts.addOption(org.apache.commons.cli.OptionBuilder.create("r"));
				opts.addOption(org.apache.commons.cli.OptionBuilder.create("c"));
				opts.addOption(org.apache.commons.cli.OptionBuilder.create("m"));
				opts.addOption(org.apache.commons.cli.OptionBuilder.create("t"));
				opts.addOption(org.apache.commons.cli.OptionBuilder.create("p"));
				opts.addOption(org.apache.commons.cli.OptionBuilder.create('h'));
				opts.addOption(org.apache.commons.cli.OptionBuilder.create('e'));
				opts.addOption(org.apache.commons.cli.OptionBuilder.create('?'));
				return opts;
			}

			/// <exception cref="org.apache.commons.cli.ParseException"/>
			private void processOptions(org.apache.commons.cli.CommandLine line, org.apache.commons.cli.Options
				 opts)
			{
				if (line.hasOption("help") || line.hasOption('?'))
				{
					org.apache.commons.cli.HelpFormatter formatter = new org.apache.commons.cli.HelpFormatter
						();
					System.Console.Out.WriteLine("Protobuf IPC benchmark.");
					System.Console.Out.WriteLine();
					formatter.printHelp(100, "java ... PBRPCBenchmark [options]", "\nSupported options:"
						, opts, string.Empty);
					return;
				}
				if (line.hasOption('s'))
				{
					serverThreads = System.Convert.ToInt32(line.getOptionValue('s'));
				}
				if (line.hasOption('r'))
				{
					serverReaderThreads = System.Convert.ToInt32(line.getOptionValue('r'));
				}
				if (line.hasOption('c'))
				{
					clientThreads = System.Convert.ToInt32(line.getOptionValue('c'));
				}
				if (line.hasOption('t'))
				{
					secondsToRun = System.Convert.ToInt32(line.getOptionValue('t'));
				}
				if (line.hasOption('m'))
				{
					msgSize = System.Convert.ToInt32(line.getOptionValue('m'));
				}
				if (line.hasOption('p'))
				{
					port = System.Convert.ToInt32(line.getOptionValue('p'));
				}
				if (line.hasOption('h'))
				{
					host = line.getOptionValue('h');
				}
				if (line.hasOption('e'))
				{
					string eng = line.getOptionValue('e');
					if ("protobuf".Equals(eng))
					{
						rpcEngine = Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.ProtobufRpcEngine
							));
					}
					else
					{
						if ("writable".Equals(eng))
						{
							rpcEngine = Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.WritableRpcEngine
								));
						}
						else
						{
							throw new org.apache.commons.cli.ParseException("invalid engine: " + eng);
						}
					}
				}
				string[] remainingArgs = line.getArgs();
				if (remainingArgs.Length != 0)
				{
					throw new org.apache.commons.cli.ParseException("Extra arguments: " + com.google.common.@base.Joiner
						.on(" ").join(remainingArgs));
				}
			}

			public virtual int getPort()
			{
				if (port == 0)
				{
					port = org.apache.hadoop.net.NetUtils.getFreeSocketPort();
					if (port == 0)
					{
						throw new System.Exception("Could not find a free port");
					}
				}
				return port;
			}

			public override string ToString()
			{
				return "rpcEngine=" + rpcEngine + "\nserverThreads=" + serverThreads + "\nserverReaderThreads="
					 + serverReaderThreads + "\nclientThreads=" + clientThreads + "\nhost=" + host +
					 "\nport=" + getPort() + "\nsecondsToRun=" + secondsToRun + "\nmsgSize=" + msgSize;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private org.apache.hadoop.ipc.RPC.Server startServer(org.apache.hadoop.ipc.RPCCallBenchmark.MyOptions
			 opts)
		{
			if (opts.serverThreads <= 0)
			{
				return null;
			}
			conf.setInt(org.apache.hadoop.fs.CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_KEY
				, opts.serverReaderThreads);
			org.apache.hadoop.ipc.RPC.Server server;
			// Get RPC server for server side implementation
			if (opts.rpcEngine == Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.ProtobufRpcEngine
				)))
			{
				// Create server side implementation
				org.apache.hadoop.ipc.TestProtoBufRpc.PBServerImpl serverImpl = new org.apache.hadoop.ipc.TestProtoBufRpc.PBServerImpl
					();
				com.google.protobuf.BlockingService service = org.apache.hadoop.ipc.protobuf.TestRpcServiceProtos.TestProtobufRpcProto
					.newReflectiveBlockingService(serverImpl);
				server = new org.apache.hadoop.ipc.RPC.Builder(conf).setProtocol(Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.ipc.TestProtoBufRpc.TestRpcService))).setInstance(service
					).setBindAddress(opts.host).setPort(opts.getPort()).setNumHandlers(opts.serverThreads
					).setVerbose(false).build();
			}
			else
			{
				if (opts.rpcEngine == Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.WritableRpcEngine
					)))
				{
					server = new org.apache.hadoop.ipc.RPC.Builder(conf).setProtocol(Sharpen.Runtime.getClassForType
						(typeof(org.apache.hadoop.ipc.TestRPC.TestProtocol))).setInstance(new org.apache.hadoop.ipc.TestRPC.TestImpl
						()).setBindAddress(opts.host).setPort(opts.getPort()).setNumHandlers(opts.serverThreads
						).setVerbose(false).build();
				}
				else
				{
					throw new System.Exception("Bad engine: " + opts.rpcEngine);
				}
			}
			server.start();
			return server;
		}

		private long getTotalCpuTime<_T0>(System.Collections.Generic.IEnumerable<_T0> threads
			)
			where _T0 : java.lang.Thread
		{
			long total = 0;
			foreach (java.lang.Thread t in threads)
			{
				long tid = t.getId();
				total += threadBean.getThreadCpuTime(tid);
			}
			return total;
		}

		/// <exception cref="System.Exception"/>
		public virtual int run(string[] args)
		{
			org.apache.hadoop.ipc.RPCCallBenchmark.MyOptions opts = new org.apache.hadoop.ipc.RPCCallBenchmark.MyOptions
				(args);
			if (opts.failed)
			{
				return -1;
			}
			// Set RPC engine to the configured RPC engine
			org.apache.hadoop.ipc.RPC.setProtocolEngine(conf, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestProtoBufRpc.TestRpcService)), opts.rpcEngine);
			org.apache.hadoop.ipc.RPC.Server server = startServer(opts);
			try
			{
				org.apache.hadoop.test.MultithreadedTestUtil.TestContext ctx = setupClientTestContext
					(opts);
				if (ctx != null)
				{
					long totalCalls = 0;
					ctx.startThreads();
					long veryStart = Sharpen.Runtime.nanoTime();
					// Loop printing results every second until the specified
					// time has elapsed
					for (int i = 0; i < opts.secondsToRun; i++)
					{
						long st = Sharpen.Runtime.nanoTime();
						ctx.waitFor(1000);
						long et = Sharpen.Runtime.nanoTime();
						long ct = callCount.getAndSet(0);
						totalCalls += ct;
						double callsPerSec = (ct * 1000000000) / (et - st);
						System.Console.Out.WriteLine("Calls per second: " + callsPerSec);
					}
					// Print results
					if (totalCalls > 0)
					{
						long veryEnd = Sharpen.Runtime.nanoTime();
						double callsPerSec = (totalCalls * 1000000000) / (veryEnd - veryStart);
						long cpuNanosClient = getTotalCpuTime(ctx.getTestThreads());
						long cpuNanosServer = -1;
						if (server != null)
						{
							cpuNanosServer = getTotalCpuTime(server.getHandlers());
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
					ctx.stop();
				}
				else
				{
					while (true)
					{
						java.lang.Thread.sleep(10000);
					}
				}
			}
			finally
			{
				if (server != null)
				{
					server.stop();
				}
			}
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private org.apache.hadoop.test.MultithreadedTestUtil.TestContext setupClientTestContext
			(org.apache.hadoop.ipc.RPCCallBenchmark.MyOptions opts)
		{
			if (opts.clientThreads <= 0)
			{
				return null;
			}
			// Set up a separate proxy for each client thread,
			// rather than making them share TCP pipes.
			int numProxies = opts.clientThreads;
			org.apache.hadoop.ipc.RPCCallBenchmark.RpcServiceWrapper[] proxies = new org.apache.hadoop.ipc.RPCCallBenchmark.RpcServiceWrapper
				[numProxies];
			for (int i = 0; i < numProxies; i++)
			{
				proxies[i] = org.apache.hadoop.security.UserGroupInformation.createUserForTesting
					("proxy-" + i, new string[] {  }).doAs(new _PrivilegedExceptionAction_347(this, 
					opts));
			}
			// Create an echo message of the desired length
			java.lang.StringBuilder msgBuilder = new java.lang.StringBuilder(opts.msgSize);
			for (int c = 0; c < opts.msgSize; c++)
			{
				msgBuilder.Append('x');
			}
			string echoMessage = msgBuilder.ToString();
			// Create the clients in a test context
			org.apache.hadoop.test.MultithreadedTestUtil.TestContext ctx = new org.apache.hadoop.test.MultithreadedTestUtil.TestContext
				();
			for (int i_1 = 0; i_1 < opts.clientThreads; i_1++)
			{
				org.apache.hadoop.ipc.RPCCallBenchmark.RpcServiceWrapper proxy = proxies[i_1 % numProxies
					];
				ctx.addThread(new _RepeatingTestThread_367(this, proxy, echoMessage, ctx));
			}
			return ctx;
		}

		private sealed class _PrivilegedExceptionAction_347 : java.security.PrivilegedExceptionAction
			<org.apache.hadoop.ipc.RPCCallBenchmark.RpcServiceWrapper>
		{
			public _PrivilegedExceptionAction_347(RPCCallBenchmark _enclosing, org.apache.hadoop.ipc.RPCCallBenchmark.MyOptions
				 opts)
			{
				this._enclosing = _enclosing;
				this.opts = opts;
			}

			/// <exception cref="System.Exception"/>
			public org.apache.hadoop.ipc.RPCCallBenchmark.RpcServiceWrapper run()
			{
				return this._enclosing.createRpcClient(opts);
			}

			private readonly RPCCallBenchmark _enclosing;

			private readonly org.apache.hadoop.ipc.RPCCallBenchmark.MyOptions opts;
		}

		private sealed class _RepeatingTestThread_367 : org.apache.hadoop.test.MultithreadedTestUtil.RepeatingTestThread
		{
			public _RepeatingTestThread_367(RPCCallBenchmark _enclosing, org.apache.hadoop.ipc.RPCCallBenchmark.RpcServiceWrapper
				 proxy, string echoMessage, org.apache.hadoop.test.MultithreadedTestUtil.TestContext
				 baseArg1)
				: base(baseArg1)
			{
				this._enclosing = _enclosing;
				this.proxy = proxy;
				this.echoMessage = echoMessage;
			}

			/// <exception cref="System.Exception"/>
			public override void doAnAction()
			{
				proxy.doEcho(echoMessage);
				this._enclosing.callCount.incrementAndGet();
			}

			private readonly RPCCallBenchmark _enclosing;

			private readonly org.apache.hadoop.ipc.RPCCallBenchmark.RpcServiceWrapper proxy;

			private readonly string echoMessage;
		}

		/// <summary>
		/// Simple interface that can be implemented either by the
		/// protobuf or writable implementations.
		/// </summary>
		private interface RpcServiceWrapper
		{
			/// <exception cref="System.Exception"/>
			string doEcho(string msg);
		}

		/// <summary>Create a client proxy for the specified engine.</summary>
		/// <exception cref="System.IO.IOException"/>
		private org.apache.hadoop.ipc.RPCCallBenchmark.RpcServiceWrapper createRpcClient(
			org.apache.hadoop.ipc.RPCCallBenchmark.MyOptions opts)
		{
			java.net.InetSocketAddress addr = org.apache.hadoop.net.NetUtils.createSocketAddr
				(opts.host, opts.getPort());
			if (opts.rpcEngine == Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.ProtobufRpcEngine
				)))
			{
				org.apache.hadoop.ipc.TestProtoBufRpc.TestRpcService proxy = org.apache.hadoop.ipc.RPC
					.getProxy<org.apache.hadoop.ipc.TestProtoBufRpc.TestRpcService>(0, addr, conf);
				return new _RpcServiceWrapper_394(proxy);
			}
			else
			{
				if (opts.rpcEngine == Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.WritableRpcEngine
					)))
				{
					org.apache.hadoop.ipc.TestRPC.TestProtocol proxy = org.apache.hadoop.ipc.RPC.getProxy
						<org.apache.hadoop.ipc.TestRPC.TestProtocol>(org.apache.hadoop.ipc.TestRPC.TestProtocol
						.versionID, addr, conf);
					return new _RpcServiceWrapper_407(proxy);
				}
				else
				{
					throw new System.Exception("unsupported engine: " + opts.rpcEngine);
				}
			}
		}

		private sealed class _RpcServiceWrapper_394 : org.apache.hadoop.ipc.RPCCallBenchmark.RpcServiceWrapper
		{
			public _RpcServiceWrapper_394(org.apache.hadoop.ipc.TestProtoBufRpc.TestRpcService
				 proxy)
			{
				this.proxy = proxy;
			}

			/// <exception cref="System.Exception"/>
			public string doEcho(string msg)
			{
				org.apache.hadoop.ipc.protobuf.TestProtos.EchoRequestProto req = ((org.apache.hadoop.ipc.protobuf.TestProtos.EchoRequestProto
					)org.apache.hadoop.ipc.protobuf.TestProtos.EchoRequestProto.newBuilder().setMessage
					(msg).build());
				org.apache.hadoop.ipc.protobuf.TestProtos.EchoResponseProto responseProto = proxy
					.echo(null, req);
				return responseProto.getMessage();
			}

			private readonly org.apache.hadoop.ipc.TestProtoBufRpc.TestRpcService proxy;
		}

		private sealed class _RpcServiceWrapper_407 : org.apache.hadoop.ipc.RPCCallBenchmark.RpcServiceWrapper
		{
			public _RpcServiceWrapper_407(org.apache.hadoop.ipc.TestRPC.TestProtocol proxy)
			{
				this.proxy = proxy;
			}

			/// <exception cref="System.Exception"/>
			public string doEcho(string msg)
			{
				return proxy.echo(msg);
			}

			private readonly org.apache.hadoop.ipc.TestRPC.TestProtocol proxy;
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int rc = org.apache.hadoop.util.ToolRunner.run(new org.apache.hadoop.ipc.RPCCallBenchmark
				(), args);
			System.Environment.Exit(rc);
		}

		public virtual void setConf(org.apache.hadoop.conf.Configuration conf)
		{
			this.conf = conf;
		}

		public virtual org.apache.hadoop.conf.Configuration getConf()
		{
			return conf;
		}
	}
}
