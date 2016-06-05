using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Text;
using Com.Google.Common.Annotations;
using NUnit.Framework;
using Org.Apache.Hadoop.Util;
using Org.Apache.Zookeeper;
using Org.Apache.Zookeeper.Server;
using Org.Apache.Zookeeper.Server.Persistence;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.HA
{
	/// <summary>
	/// Copy-paste of ClientBase from ZooKeeper, but without any of the
	/// JMXEnv verification.
	/// </summary>
	/// <remarks>
	/// Copy-paste of ClientBase from ZooKeeper, but without any of the
	/// JMXEnv verification. There seems to be a bug ZOOKEEPER-1438
	/// which causes spurious failures in the JMXEnv verification when
	/// we run these tests with the upstream ClientBase.
	/// </remarks>
	public abstract class ClientBaseWithFixes : ZKTestCase
	{
		protected internal static readonly Logger Log = LoggerFactory.GetLogger(typeof(ClientBaseWithFixes
			));

		public static int ConnectionTimeout = 30000;

		internal static readonly FilePath Basetest = new FilePath(Runtime.GetProperty("test.build.data"
			, "build"));

		protected internal readonly string hostPort;

		protected internal int maxCnxns = 0;

		protected internal ServerCnxnFactory serverFactory = null;

		protected internal FilePath tmpDir = null;

		internal long initialFdCount;

		/// <summary>In general don't use this.</summary>
		/// <remarks>
		/// In general don't use this. Only use in the special case that you
		/// want to ignore results (for whatever reason) in your test. Don't
		/// use empty watchers in real code!
		/// </remarks>
		protected internal class NullWatcher : Watcher
		{
			public override void Process(WatchedEvent @event)
			{
			}

			internal NullWatcher(ClientBaseWithFixes _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly ClientBaseWithFixes _enclosing;
			/* nada */
		}

		protected internal class CountdownWatcher : Watcher
		{
			internal volatile CountDownLatch clientConnected;

			internal volatile bool connected;

			protected internal ZooKeeper client;

			// XXX this doesn't need to be volatile! (Should probably be final)
			public virtual void InitializeWatchedClient(ZooKeeper zk)
			{
				if (client != null)
				{
					throw new RuntimeException("Watched Client was already set");
				}
				client = zk;
			}

			public CountdownWatcher()
			{
				Reset();
			}

			public virtual void Reset()
			{
				lock (this)
				{
					clientConnected = new CountDownLatch(1);
					connected = false;
				}
			}

			public override void Process(WatchedEvent @event)
			{
				lock (this)
				{
					if (@event.GetState() == Watcher.Event.KeeperState.SyncConnected || @event.GetState
						() == Watcher.Event.KeeperState.ConnectedReadOnly)
					{
						connected = true;
						Sharpen.Runtime.NotifyAll(this);
						clientConnected.CountDown();
					}
					else
					{
						connected = false;
						Sharpen.Runtime.NotifyAll(this);
					}
				}
			}

			internal virtual bool IsConnected()
			{
				lock (this)
				{
					return connected;
				}
			}

			/// <exception cref="System.Exception"/>
			/// <exception cref="Sharpen.TimeoutException"/>
			[VisibleForTesting]
			public virtual void WaitForConnected(long timeout)
			{
				lock (this)
				{
					long expire = Time.Now() + timeout;
					long left = timeout;
					while (!connected && left > 0)
					{
						Sharpen.Runtime.Wait(this, left);
						left = expire - Time.Now();
					}
					if (!connected)
					{
						throw new TimeoutException("Did not connect");
					}
				}
			}

			/// <exception cref="System.Exception"/>
			/// <exception cref="Sharpen.TimeoutException"/>
			[VisibleForTesting]
			public virtual void WaitForDisconnected(long timeout)
			{
				lock (this)
				{
					long expire = Time.Now() + timeout;
					long left = timeout;
					while (connected && left > 0)
					{
						Sharpen.Runtime.Wait(this, left);
						left = expire - Time.Now();
					}
					if (connected)
					{
						throw new TimeoutException("Did not disconnect");
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal virtual TestableZooKeeper CreateClient()
		{
			return CreateClient(hostPort);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal virtual TestableZooKeeper CreateClient(string hp)
		{
			ClientBaseWithFixes.CountdownWatcher watcher = new ClientBaseWithFixes.CountdownWatcher
				();
			return CreateClient(watcher, hp);
		}

		private List<ZooKeeper> allClients;

		private bool allClientsSetup = false;

		private RandomAccessFile portNumLockFile;

		private FilePath portNumFile;

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal virtual TestableZooKeeper CreateClient(ClientBaseWithFixes.CountdownWatcher
			 watcher, string hp)
		{
			return CreateClient(watcher, hp, ConnectionTimeout);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal virtual TestableZooKeeper CreateClient(ClientBaseWithFixes.CountdownWatcher
			 watcher, string hp, int timeout)
		{
			watcher.Reset();
			TestableZooKeeper zk = new TestableZooKeeper(hp, timeout, watcher);
			if (!watcher.clientConnected.Await(timeout, TimeUnit.Milliseconds))
			{
				NUnit.Framework.Assert.Fail("Unable to connect to server");
			}
			lock (this)
			{
				if (!allClientsSetup)
				{
					Log.Error("allClients never setup");
					NUnit.Framework.Assert.Fail("allClients never setup");
				}
				if (allClients != null)
				{
					allClients.AddItem(zk);
				}
				else
				{
					// test done - close the zk, not needed
					zk.Close();
				}
			}
			watcher.InitializeWatchedClient(zk);
			return zk;
		}

		public class HostPort
		{
			internal string host;

			internal int port;

			public HostPort(string host, int port)
			{
				this.host = host;
				this.port = port;
			}
		}

		public static IList<ClientBaseWithFixes.HostPort> ParseHostPortList(string hplist
			)
		{
			AList<ClientBaseWithFixes.HostPort> alist = new AList<ClientBaseWithFixes.HostPort
				>();
			foreach (string hp in hplist.Split(","))
			{
				int idx = hp.LastIndexOf(':');
				string host = Sharpen.Runtime.Substring(hp, 0, idx);
				int port;
				try
				{
					port = System.Convert.ToInt32(Sharpen.Runtime.Substring(hp, idx + 1));
				}
				catch (RuntimeException e)
				{
					throw new RuntimeException("Problem parsing " + hp + e.ToString());
				}
				alist.AddItem(new ClientBaseWithFixes.HostPort(host, port));
			}
			return alist;
		}

		/// <summary>Send the 4letterword</summary>
		/// <param name="host">the destination host</param>
		/// <param name="port">the destination port</param>
		/// <param name="cmd">the 4letterword</param>
		/// <returns/>
		/// <exception cref="System.IO.IOException"/>
		public static string Send4LetterWord(string host, int port, string cmd)
		{
			Log.Info("connecting to " + host + " " + port);
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

		public static bool WaitForServerUp(string hp, long timeout)
		{
			long start = Time.Now();
			while (true)
			{
				try
				{
					// if there are multiple hostports, just take the first one
					ClientBaseWithFixes.HostPort hpobj = ParseHostPortList(hp)[0];
					string result = Send4LetterWord(hpobj.host, hpobj.port, "stat");
					if (result.StartsWith("Zookeeper version:") && !result.Contains("READ-ONLY"))
					{
						return true;
					}
				}
				catch (IOException e)
				{
					// ignore as this is expected
					Log.Info("server " + hp + " not up " + e);
				}
				if (Time.Now() > start + timeout)
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

		public static bool WaitForServerDown(string hp, long timeout)
		{
			long start = Time.Now();
			while (true)
			{
				try
				{
					ClientBaseWithFixes.HostPort hpobj = ParseHostPortList(hp)[0];
					Send4LetterWord(hpobj.host, hpobj.port, "stat");
				}
				catch (IOException)
				{
					return true;
				}
				if (Time.Now() > start + timeout)
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
		public static FilePath CreateTmpDir()
		{
			return CreateTmpDir(Basetest);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static FilePath CreateTmpDir(FilePath parentDir)
		{
			FilePath tmpFile = FilePath.CreateTempFile("test", ".junit", parentDir);
			// don't delete tmpFile - this ensures we don't attempt to create
			// a tmpDir with a duplicate name
			FilePath tmpDir = new FilePath(tmpFile + ".dir");
			NUnit.Framework.Assert.IsFalse(tmpDir.Exists());
			// never true if tmpfile does it's job
			Assert.True(tmpDir.Mkdirs());
			return tmpDir;
		}

		private static int GetPort(string hostPort)
		{
			string[] split = hostPort.Split(":");
			string portstr = split[split.Length - 1];
			string[] pc = portstr.Split("/");
			if (pc.Length > 1)
			{
				portstr = pc[0];
			}
			return System.Convert.ToInt32(portstr);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		internal static ServerCnxnFactory CreateNewServerInstance(FilePath dataDir, ServerCnxnFactory
			 factory, string hostPort, int maxCnxns)
		{
			ZooKeeperServer zks = new ZooKeeperServer(dataDir, dataDir, 3000);
			int Port = GetPort(hostPort);
			if (factory == null)
			{
				factory = ServerCnxnFactory.CreateFactory(Port, maxCnxns);
			}
			factory.Startup(zks);
			Assert.True("waiting for server up", ClientBaseWithFixes.WaitForServerUp
				("127.0.0.1:" + Port, ConnectionTimeout));
			return factory;
		}

		internal static void ShutdownServerInstance(ServerCnxnFactory factory, string hostPort
			)
		{
			if (factory != null)
			{
				ZKDatabase zkDb;
				{
					ZooKeeperServer zs = GetServer(factory);
					zkDb = zs.GetZKDatabase();
				}
				factory.Shutdown();
				try
				{
					zkDb.Close();
				}
				catch (IOException ie)
				{
					Log.Warn("Error closing logs ", ie);
				}
				int Port = GetPort(hostPort);
				Assert.True("waiting for server down", ClientBaseWithFixes.WaitForServerDown
					("127.0.0.1:" + Port, ConnectionTimeout));
			}
		}

		/// <summary>Test specific setup</summary>
		public static void SetupTestEnv()
		{
			// during the tests we run with 100K prealloc in the logs.
			// on windows systems prealloc of 64M was seen to take ~15seconds
			// resulting in test Assert.failure (client timeout on first session).
			// set env and directly in order to handle static init/gc issues
			Runtime.SetProperty("zookeeper.preAllocSize", "100");
			FileTxnLog.SetPreallocSize(100 * 1024);
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void SetUpAll()
		{
			allClients = new List<ZooKeeper>();
			allClientsSetup = true;
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void SetUp()
		{
			Basetest.Mkdirs();
			SetupTestEnv();
			SetUpAll();
			tmpDir = CreateTmpDir(Basetest);
			StartServer();
			Log.Info("Client test setup finished");
		}

		private string InitHostPort()
		{
			Basetest.Mkdirs();
			int port;
			for (; ; )
			{
				port = PortAssignment.Unique();
				FileLock Lock = null;
				portNumLockFile = null;
				try
				{
					try
					{
						portNumFile = new FilePath(Basetest, port + ".lock");
						portNumLockFile = new RandomAccessFile(portNumFile, "rw");
						try
						{
							Lock = portNumLockFile.GetChannel().TryLock();
						}
						catch (OverlappingFileLockException)
						{
							continue;
						}
					}
					finally
					{
						if (Lock != null)
						{
							break;
						}
						if (portNumLockFile != null)
						{
							portNumLockFile.Close();
						}
					}
				}
				catch (IOException e)
				{
					throw new RuntimeException(e);
				}
			}
			return "127.0.0.1:" + port;
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void StartServer()
		{
			Log.Info("STARTING server");
			serverFactory = CreateNewServerInstance(tmpDir, serverFactory, hostPort, maxCnxns
				);
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void StopServer()
		{
			Log.Info("STOPPING server");
			ShutdownServerInstance(serverFactory, hostPort);
			serverFactory = null;
		}

		protected internal static ZooKeeperServer GetServer(ServerCnxnFactory fac)
		{
			ZooKeeperServer zs = ServerCnxnFactoryAccessor.GetZkServer(fac);
			return zs;
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void TearDownAll()
		{
			lock (this)
			{
				if (allClients != null)
				{
					foreach (ZooKeeper zk in allClients)
					{
						try
						{
							if (zk != null)
							{
								zk.Close();
							}
						}
						catch (Exception e)
						{
							Log.Warn("ignoring interrupt", e);
						}
					}
				}
				allClients = null;
			}
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void TearDown()
		{
			Log.Info("tearDown starting");
			TearDownAll();
			StopServer();
			portNumLockFile.Close();
			portNumFile.Delete();
			if (tmpDir != null)
			{
				Assert.True("delete " + tmpDir.ToString(), RecursiveDelete(tmpDir
					));
			}
			// This has to be set to null when the same instance of this class is reused between test cases
			serverFactory = null;
		}

		public static bool RecursiveDelete(FilePath d)
		{
			if (d.IsDirectory())
			{
				FilePath[] children = d.ListFiles();
				foreach (FilePath f in children)
				{
					Assert.True("delete " + f.ToString(), RecursiveDelete(f));
				}
			}
			return d.Delete();
		}

		public ClientBaseWithFixes()
		{
			hostPort = InitHostPort();
		}
	}
}
