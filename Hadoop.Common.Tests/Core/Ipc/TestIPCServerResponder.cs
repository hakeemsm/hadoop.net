using System;
using System.Net;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net;
using Sharpen;

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>
	/// This test provokes partial writes in the server, which is
	/// serving multiple clients.
	/// </summary>
	public class TestIPCServerResponder : TestCase
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Ipc.TestIPCServerResponder
			));

		private static Configuration conf = new Configuration();

		public TestIPCServerResponder(string name)
			: base(name)
		{
		}

		private static readonly Random Random = new Random();

		private const string Address = "0.0.0.0";

		private const int ByteCount = 1024;

		private static readonly byte[] Bytes = new byte[ByteCount];

		static TestIPCServerResponder()
		{
			for (int i = 0; i < ByteCount; i++)
			{
				Bytes[i] = unchecked((byte)((byte)('a') + (i % 26)));
			}
		}

		private class TestServer : Server
		{
			private bool sleep;

			/// <exception cref="System.IO.IOException"/>
			public TestServer(int handlerCount, bool sleep)
				: base(Address, 0, typeof(BytesWritable), handlerCount, conf)
			{
				// Set the buffer size to half of the maximum parameter/result size 
				// to force the socket to block
				this.SetSocketSendBufSize(ByteCount / 2);
				this.sleep = sleep;
			}

			/// <exception cref="System.IO.IOException"/>
			public override Writable Call(RPC.RpcKind rpcKind, string protocol, Writable param
				, long receiveTime)
			{
				if (sleep)
				{
					try
					{
						Sharpen.Thread.Sleep(Random.Next(20));
					}
					catch (Exception)
					{
					}
				}
				// sleep a bit
				return param;
			}
		}

		private class Caller : Sharpen.Thread
		{
			private Client client;

			private int count;

			private IPEndPoint address;

			private bool failed;

			public Caller(Client client, IPEndPoint address, int count)
			{
				this.client = client;
				this.address = address;
				this.count = count;
			}

			public override void Run()
			{
				for (int i = 0; i < count; i++)
				{
					try
					{
						int byteSize = Random.Next(ByteCount);
						byte[] bytes = new byte[byteSize];
						System.Array.Copy(Bytes, 0, bytes, 0, byteSize);
						Writable param = new BytesWritable(bytes);
						client.Call(param, address);
						Sharpen.Thread.Sleep(Random.Next(20));
					}
					catch (Exception e)
					{
						Log.Fatal("Caught Exception", e);
						failed = true;
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestResponseBuffer()
		{
			Server.InitialRespBufSize = 1;
			conf.SetInt(CommonConfigurationKeys.IpcServerRpcMaxResponseSizeKey, 1);
			TestServerResponder(1, true, 1, 1, 5);
			conf = new Configuration();
		}

		// reset configuration
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestServerResponder()
		{
			TestServerResponder(10, true, 1, 10, 200);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestServerResponder(int handlerCount, bool handlerSleep, int 
			clientCount, int callerCount, int callCount)
		{
			Server server = new TestIPCServerResponder.TestServer(handlerCount, handlerSleep);
			server.Start();
			IPEndPoint address = NetUtils.GetConnectAddress(server);
			Client[] clients = new Client[clientCount];
			for (int i = 0; i < clientCount; i++)
			{
				clients[i] = new Client(typeof(BytesWritable), conf);
			}
			TestIPCServerResponder.Caller[] callers = new TestIPCServerResponder.Caller[callerCount
				];
			for (int i_1 = 0; i_1 < callerCount; i_1++)
			{
				callers[i_1] = new TestIPCServerResponder.Caller(clients[i_1 % clientCount], address
					, callCount);
				callers[i_1].Start();
			}
			for (int i_2 = 0; i_2 < callerCount; i_2++)
			{
				callers[i_2].Join();
				NUnit.Framework.Assert.IsFalse(callers[i_2].failed);
			}
			for (int i_3 = 0; i_3 < clientCount; i_3++)
			{
				clients[i_3].Stop();
			}
			server.Stop();
		}
	}
}
