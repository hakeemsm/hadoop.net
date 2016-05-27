using System.IO;
using System.Net;
using System.Net.Sockets;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>
	/// This is intended to be a set of unit tests for the
	/// org.apache.hadoop.ipc.Server class.
	/// </summary>
	public class TestServer
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBind()
		{
			Configuration conf = new Configuration();
			Socket socket = new Socket();
			IPEndPoint address = new IPEndPoint("0.0.0.0", 0);
			socket.Bind(address);
			try
			{
				int min = socket.GetLocalPort();
				int max = min + 100;
				conf.Set("TestRange", min + "-" + max);
				Socket socket2 = new Socket();
				IPEndPoint address2 = new IPEndPoint("0.0.0.0", 0);
				Server.Bind(socket2, address2, 10, conf, "TestRange");
				try
				{
					NUnit.Framework.Assert.IsTrue(socket2.IsBound());
					NUnit.Framework.Assert.IsTrue(socket2.GetLocalPort() > min);
					NUnit.Framework.Assert.IsTrue(socket2.GetLocalPort() <= max);
				}
				finally
				{
					socket2.Close();
				}
			}
			finally
			{
				socket.Close();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBindSimple()
		{
			Socket socket = new Socket();
			IPEndPoint address = new IPEndPoint("0.0.0.0", 0);
			Server.Bind(socket, address, 10);
			try
			{
				NUnit.Framework.Assert.IsTrue(socket.IsBound());
			}
			finally
			{
				socket.Close();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestEmptyConfig()
		{
			Configuration conf = new Configuration();
			conf.Set("TestRange", string.Empty);
			Socket socket = new Socket();
			IPEndPoint address = new IPEndPoint("0.0.0.0", 0);
			try
			{
				Server.Bind(socket, address, 10, conf, "TestRange");
				NUnit.Framework.Assert.IsTrue(socket.IsBound());
			}
			finally
			{
				socket.Close();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBindError()
		{
			Configuration conf = new Configuration();
			Socket socket = new Socket();
			IPEndPoint address = new IPEndPoint("0.0.0.0", 0);
			socket.Bind(address);
			try
			{
				int min = socket.GetLocalPort();
				conf.Set("TestRange", min + "-" + min);
				Socket socket2 = new Socket();
				IPEndPoint address2 = new IPEndPoint("0.0.0.0", 0);
				bool caught = false;
				try
				{
					Server.Bind(socket2, address2, 10, conf, "TestRange");
				}
				catch (BindException)
				{
					caught = true;
				}
				finally
				{
					socket2.Close();
				}
				NUnit.Framework.Assert.IsTrue("Failed to catch the expected bind exception", caught
					);
			}
			finally
			{
				socket.Close();
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestExceptionsHandler()
		{
			Server.ExceptionsHandler handler = new Server.ExceptionsHandler();
			handler.AddTerseExceptions(typeof(IOException));
			handler.AddTerseExceptions(typeof(RpcServerException), typeof(IpcException));
			NUnit.Framework.Assert.IsTrue(handler.IsTerse(typeof(IOException)));
			NUnit.Framework.Assert.IsTrue(handler.IsTerse(typeof(RpcServerException)));
			NUnit.Framework.Assert.IsTrue(handler.IsTerse(typeof(IpcException)));
			NUnit.Framework.Assert.IsFalse(handler.IsTerse(typeof(RpcClientException)));
		}
	}
}
