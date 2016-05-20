using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>
	/// This test provokes partial writes in the server, which is
	/// serving multiple clients.
	/// </summary>
	public class TestIPCServerResponder : NUnit.Framework.TestCase
	{
		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.TestIPCServerResponder
			)));

		private static org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
			();

		public TestIPCServerResponder(string name)
			: base(name)
		{
		}

		private static readonly java.util.Random RANDOM = new java.util.Random();

		private const string ADDRESS = "0.0.0.0";

		private const int BYTE_COUNT = 1024;

		private static readonly byte[] BYTES = new byte[BYTE_COUNT];

		static TestIPCServerResponder()
		{
			for (int i = 0; i < BYTE_COUNT; i++)
			{
				BYTES[i] = unchecked((byte)((byte)('a') + (i % 26)));
			}
		}

		private class TestServer : org.apache.hadoop.ipc.Server
		{
			private bool sleep;

			/// <exception cref="System.IO.IOException"/>
			public TestServer(int handlerCount, bool sleep)
				: base(ADDRESS, 0, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.BytesWritable
					)), handlerCount, conf)
			{
				// Set the buffer size to half of the maximum parameter/result size 
				// to force the socket to block
				this.setSocketSendBufSize(BYTE_COUNT / 2);
				this.sleep = sleep;
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.io.Writable call(org.apache.hadoop.ipc.RPC.RpcKind
				 rpcKind, string protocol, org.apache.hadoop.io.Writable param, long receiveTime
				)
			{
				if (sleep)
				{
					try
					{
						java.lang.Thread.sleep(RANDOM.nextInt(20));
					}
					catch (System.Exception)
					{
					}
				}
				// sleep a bit
				return param;
			}
		}

		private class Caller : java.lang.Thread
		{
			private org.apache.hadoop.ipc.Client client;

			private int count;

			private java.net.InetSocketAddress address;

			private bool failed;

			public Caller(org.apache.hadoop.ipc.Client client, java.net.InetSocketAddress address
				, int count)
			{
				this.client = client;
				this.address = address;
				this.count = count;
			}

			public override void run()
			{
				for (int i = 0; i < count; i++)
				{
					try
					{
						int byteSize = RANDOM.nextInt(BYTE_COUNT);
						byte[] bytes = new byte[byteSize];
						System.Array.Copy(BYTES, 0, bytes, 0, byteSize);
						org.apache.hadoop.io.Writable param = new org.apache.hadoop.io.BytesWritable(bytes
							);
						client.call(param, address);
						java.lang.Thread.sleep(RANDOM.nextInt(20));
					}
					catch (System.Exception e)
					{
						LOG.fatal("Caught Exception", e);
						failed = true;
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void testResponseBuffer()
		{
			org.apache.hadoop.ipc.Server.INITIAL_RESP_BUF_SIZE = 1;
			conf.setInt(org.apache.hadoop.fs.CommonConfigurationKeys.IPC_SERVER_RPC_MAX_RESPONSE_SIZE_KEY
				, 1);
			testServerResponder(1, true, 1, 1, 5);
			conf = new org.apache.hadoop.conf.Configuration();
		}

		// reset configuration
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void testServerResponder()
		{
			testServerResponder(10, true, 1, 10, 200);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void testServerResponder(int handlerCount, bool handlerSleep, int 
			clientCount, int callerCount, int callCount)
		{
			org.apache.hadoop.ipc.Server server = new org.apache.hadoop.ipc.TestIPCServerResponder.TestServer
				(handlerCount, handlerSleep);
			server.start();
			java.net.InetSocketAddress address = org.apache.hadoop.net.NetUtils.getConnectAddress
				(server);
			org.apache.hadoop.ipc.Client[] clients = new org.apache.hadoop.ipc.Client[clientCount
				];
			for (int i = 0; i < clientCount; i++)
			{
				clients[i] = new org.apache.hadoop.ipc.Client(Sharpen.Runtime.getClassForType(typeof(
					org.apache.hadoop.io.BytesWritable)), conf);
			}
			org.apache.hadoop.ipc.TestIPCServerResponder.Caller[] callers = new org.apache.hadoop.ipc.TestIPCServerResponder.Caller
				[callerCount];
			for (int i_1 = 0; i_1 < callerCount; i_1++)
			{
				callers[i_1] = new org.apache.hadoop.ipc.TestIPCServerResponder.Caller(clients[i_1
					 % clientCount], address, callCount);
				callers[i_1].start();
			}
			for (int i_2 = 0; i_2 < callerCount; i_2++)
			{
				callers[i_2].join();
				NUnit.Framework.Assert.IsFalse(callers[i_2].failed);
			}
			for (int i_3 = 0; i_3 < clientCount; i_3++)
			{
				clients[i_3].stop();
			}
			server.stop();
		}
	}
}
