using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using NUnit.Framework;
using Org.Apache.Zookeeper.Server;
using Org.Apache.Zookeeper.Server.Persistence;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Lib
{
	public class TestZKClient
	{
		public static int ConnectionTimeout = 30000;

		internal static readonly FilePath Basetest = new FilePath(Runtime.GetProperty("build.test.dir"
			, "target/zookeeper-build"));

		protected internal string hostPort = "127.0.0.1:2000";

		protected internal int maxCnxns = 0;

		protected internal NIOServerCnxnFactory factory = null;

		protected internal ZooKeeperServer zks;

		protected internal FilePath tmpDir = null;

		/// <exception cref="System.IO.IOException"/>
		public static string Send4LetterWord(string host, int port, string cmd)
		{
			Socket sock = Sharpen.Extensions.CreateSocket(host, port);
			BufferedReader reader = null;
			try
			{
				OutputStream outstream = sock.GetOutputStream();
				outstream.Write(Sharpen.Runtime.GetBytesForString(cmd));
				outstream.Flush();
				// this replicates NC - close the output stream before reading
				sock.ShutdownOutput();
				reader = new BufferedReader(new InputStreamReader(sock.GetInputStream()));
				StringBuilder sb = new StringBuilder();
				string line;
				while ((line = reader.ReadLine()) != null)
				{
					sb.Append(line + "\n");
				}
				return sb.ToString();
			}
			finally
			{
				sock.Close();
				if (reader != null)
				{
					reader.Close();
				}
			}
		}

		public static bool WaitForServerDown(string hp, long timeout)
		{
			long start = Runtime.CurrentTimeMillis();
			while (true)
			{
				try
				{
					string host = hp.Split(":")[0];
					int port = System.Convert.ToInt32(hp.Split(":")[1]);
					Send4LetterWord(host, port, "stat");
				}
				catch (IOException)
				{
					return true;
				}
				if (Runtime.CurrentTimeMillis() > start + timeout)
				{
					break;
				}
				try
				{
					Sharpen.Thread.Sleep(250);
				}
				catch (Exception)
				{
				}
			}
			// ignore
			return false;
		}

		public static bool WaitForServerUp(string hp, long timeout)
		{
			long start = Runtime.CurrentTimeMillis();
			while (true)
			{
				try
				{
					string host = hp.Split(":")[0];
					int port = System.Convert.ToInt32(hp.Split(":")[1]);
					// if there are multiple hostports, just take the first one
					string result = Send4LetterWord(host, port, "stat");
					if (result.StartsWith("Zookeeper version:"))
					{
						return true;
					}
				}
				catch (IOException)
				{
				}
				if (Runtime.CurrentTimeMillis() > start + timeout)
				{
					break;
				}
				try
				{
					Sharpen.Thread.Sleep(250);
				}
				catch (Exception)
				{
				}
			}
			// ignore
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		public static FilePath CreateTmpDir(FilePath parentDir)
		{
			FilePath tmpFile = FilePath.CreateTempFile("test", ".junit", parentDir);
			// don't delete tmpFile - this ensures we don't attempt to create
			// a tmpDir with a duplicate name
			FilePath tmpDir = new FilePath(tmpFile + ".dir");
			NUnit.Framework.Assert.IsFalse(tmpDir.Exists());
			NUnit.Framework.Assert.IsTrue(tmpDir.Mkdirs());
			return tmpDir;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			Runtime.SetProperty("zookeeper.preAllocSize", "100");
			FileTxnLog.SetPreallocSize(100 * 1024);
			if (!Basetest.Exists())
			{
				Basetest.Mkdirs();
			}
			FilePath dataDir = CreateTmpDir(Basetest);
			zks = new ZooKeeperServer(dataDir, dataDir, 3000);
			int Port = System.Convert.ToInt32(hostPort.Split(":")[1]);
			if (factory == null)
			{
				factory = new NIOServerCnxnFactory();
				factory.Configure(new IPEndPoint(Port), maxCnxns);
			}
			factory.Startup(zks);
			NUnit.Framework.Assert.IsTrue("waiting for server up", WaitForServerUp("127.0.0.1:"
				 + Port, ConnectionTimeout));
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (zks != null)
			{
				ZKDatabase zkDb = zks.GetZKDatabase();
				factory.Shutdown();
				try
				{
					zkDb.Close();
				}
				catch (IOException)
				{
				}
				int Port = System.Convert.ToInt32(hostPort.Split(":")[1]);
				NUnit.Framework.Assert.IsTrue("waiting for server down", WaitForServerDown("127.0.0.1:"
					 + Port, ConnectionTimeout));
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestzkClient()
		{
			Test("/some/test");
		}

		/// <exception cref="System.Exception"/>
		private void Test(string testClient)
		{
			ZKClient client = new ZKClient(hostPort);
			client.RegisterService("/nodemanager", "hostPort");
			client.UnregisterService("/nodemanager");
		}
	}
}
