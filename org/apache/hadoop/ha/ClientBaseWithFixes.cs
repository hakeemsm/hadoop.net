using Sharpen;

namespace org.apache.hadoop.ha
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
	public abstract class ClientBaseWithFixes : org.apache.zookeeper.ZKTestCase
	{
		protected internal static readonly org.slf4j.Logger LOG = org.slf4j.LoggerFactory
			.getLogger(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ha.ClientBaseWithFixes
			)));

		public static int CONNECTION_TIMEOUT = 30000;

		internal static readonly java.io.File BASETEST = new java.io.File(Sharpen.Runtime
			.getProperty("test.build.data", "build"));

		protected internal readonly string hostPort;

		protected internal int maxCnxns = 0;

		protected internal org.apache.zookeeper.server.ServerCnxnFactory serverFactory = 
			null;

		protected internal java.io.File tmpDir = null;

		internal long initialFdCount;

		/// <summary>In general don't use this.</summary>
		/// <remarks>
		/// In general don't use this. Only use in the special case that you
		/// want to ignore results (for whatever reason) in your test. Don't
		/// use empty watchers in real code!
		/// </remarks>
		protected internal class NullWatcher : org.apache.zookeeper.Watcher
		{
			public override void process(org.apache.zookeeper.WatchedEvent @event)
			{
			}

			internal NullWatcher(ClientBaseWithFixes _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly ClientBaseWithFixes _enclosing;
			/* nada */
		}

		protected internal class CountdownWatcher : org.apache.zookeeper.Watcher
		{
			internal volatile java.util.concurrent.CountDownLatch clientConnected;

			internal volatile bool connected;

			protected internal org.apache.zookeeper.ZooKeeper client;

			// XXX this doesn't need to be volatile! (Should probably be final)
			public virtual void initializeWatchedClient(org.apache.zookeeper.ZooKeeper zk)
			{
				if (client != null)
				{
					throw new System.Exception("Watched Client was already set");
				}
				client = zk;
			}

			public CountdownWatcher()
			{
				reset();
			}

			public virtual void reset()
			{
				lock (this)
				{
					clientConnected = new java.util.concurrent.CountDownLatch(1);
					connected = false;
				}
			}

			public override void process(org.apache.zookeeper.WatchedEvent @event)
			{
				lock (this)
				{
					if (@event.getState() == org.apache.zookeeper.Watcher.Event.KeeperState.SyncConnected
						 || @event.getState() == org.apache.zookeeper.Watcher.Event.KeeperState.ConnectedReadOnly)
					{
						connected = true;
						Sharpen.Runtime.notifyAll(this);
						clientConnected.countDown();
					}
					else
					{
						connected = false;
						Sharpen.Runtime.notifyAll(this);
					}
				}
			}

			internal virtual bool isConnected()
			{
				lock (this)
				{
					return connected;
				}
			}

			/// <exception cref="System.Exception"/>
			/// <exception cref="java.util.concurrent.TimeoutException"/>
			[com.google.common.annotations.VisibleForTesting]
			public virtual void waitForConnected(long timeout)
			{
				lock (this)
				{
					long expire = org.apache.hadoop.util.Time.now() + timeout;
					long left = timeout;
					while (!connected && left > 0)
					{
						Sharpen.Runtime.wait(this, left);
						left = expire - org.apache.hadoop.util.Time.now();
					}
					if (!connected)
					{
						throw new java.util.concurrent.TimeoutException("Did not connect");
					}
				}
			}

			/// <exception cref="System.Exception"/>
			/// <exception cref="java.util.concurrent.TimeoutException"/>
			[com.google.common.annotations.VisibleForTesting]
			public virtual void waitForDisconnected(long timeout)
			{
				lock (this)
				{
					long expire = org.apache.hadoop.util.Time.now() + timeout;
					long left = timeout;
					while (connected && left > 0)
					{
						Sharpen.Runtime.wait(this, left);
						left = expire - org.apache.hadoop.util.Time.now();
					}
					if (connected)
					{
						throw new java.util.concurrent.TimeoutException("Did not disconnect");
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal virtual org.apache.zookeeper.TestableZooKeeper createClient()
		{
			return createClient(hostPort);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal virtual org.apache.zookeeper.TestableZooKeeper createClient(string
			 hp)
		{
			org.apache.hadoop.ha.ClientBaseWithFixes.CountdownWatcher watcher = new org.apache.hadoop.ha.ClientBaseWithFixes.CountdownWatcher
				();
			return createClient(watcher, hp);
		}

		private System.Collections.Generic.LinkedList<org.apache.zookeeper.ZooKeeper> allClients;

		private bool allClientsSetup = false;

		private java.io.RandomAccessFile portNumLockFile;

		private java.io.File portNumFile;

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal virtual org.apache.zookeeper.TestableZooKeeper createClient(org.apache.hadoop.ha.ClientBaseWithFixes.CountdownWatcher
			 watcher, string hp)
		{
			return createClient(watcher, hp, CONNECTION_TIMEOUT);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal virtual org.apache.zookeeper.TestableZooKeeper createClient(org.apache.hadoop.ha.ClientBaseWithFixes.CountdownWatcher
			 watcher, string hp, int timeout)
		{
			watcher.reset();
			org.apache.zookeeper.TestableZooKeeper zk = new org.apache.zookeeper.TestableZooKeeper
				(hp, timeout, watcher);
			if (!watcher.clientConnected.await(timeout, java.util.concurrent.TimeUnit.MILLISECONDS
				))
			{
				NUnit.Framework.Assert.Fail("Unable to connect to server");
			}
			lock (this)
			{
				if (!allClientsSetup)
				{
					LOG.error("allClients never setup");
					NUnit.Framework.Assert.Fail("allClients never setup");
				}
				if (allClients != null)
				{
					allClients.add(zk);
				}
				else
				{
					// test done - close the zk, not needed
					zk.close();
				}
			}
			watcher.initializeWatchedClient(zk);
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

		public static System.Collections.Generic.IList<org.apache.hadoop.ha.ClientBaseWithFixes.HostPort
			> parseHostPortList(string hplist)
		{
			System.Collections.Generic.List<org.apache.hadoop.ha.ClientBaseWithFixes.HostPort
				> alist = new System.Collections.Generic.List<org.apache.hadoop.ha.ClientBaseWithFixes.HostPort
				>();
			foreach (string hp in hplist.split(","))
			{
				int idx = hp.LastIndexOf(':');
				string host = Sharpen.Runtime.substring(hp, 0, idx);
				int port;
				try
				{
					port = System.Convert.ToInt32(Sharpen.Runtime.substring(hp, idx + 1));
				}
				catch (System.Exception e)
				{
					throw new System.Exception("Problem parsing " + hp + e.ToString());
				}
				alist.add(new org.apache.hadoop.ha.ClientBaseWithFixes.HostPort(host, port));
			}
			return alist;
		}

		/// <summary>Send the 4letterword</summary>
		/// <param name="host">the destination host</param>
		/// <param name="port">the destination port</param>
		/// <param name="cmd">the 4letterword</param>
		/// <returns/>
		/// <exception cref="System.IO.IOException"/>
		public static string send4LetterWord(string host, int port, string cmd)
		{
			LOG.info("connecting to " + host + " " + port);
			java.net.Socket sock = new java.net.Socket(host, port);
			java.io.BufferedReader reader = null;
			try
			{
				java.io.OutputStream outstream = sock.getOutputStream();
				outstream.write(Sharpen.Runtime.getBytesForString(cmd));
				outstream.flush();
				// this replicates NC - close the output stream before reading
				sock.shutdownOutput();
				reader = new java.io.BufferedReader(new java.io.InputStreamReader(sock.getInputStream
					()));
				java.lang.StringBuilder sb = new java.lang.StringBuilder();
				string line;
				while ((line = reader.readLine()) != null)
				{
					sb.Append(line + "\n");
				}
				return sb.ToString();
			}
			finally
			{
				sock.close();
				if (reader != null)
				{
					reader.close();
				}
			}
		}

		public static bool waitForServerUp(string hp, long timeout)
		{
			long start = org.apache.hadoop.util.Time.now();
			while (true)
			{
				try
				{
					// if there are multiple hostports, just take the first one
					org.apache.hadoop.ha.ClientBaseWithFixes.HostPort hpobj = parseHostPortList(hp)[0
						];
					string result = send4LetterWord(hpobj.host, hpobj.port, "stat");
					if (result.StartsWith("Zookeeper version:") && !result.contains("READ-ONLY"))
					{
						return true;
					}
				}
				catch (System.IO.IOException e)
				{
					// ignore as this is expected
					LOG.info("server " + hp + " not up " + e);
				}
				if (org.apache.hadoop.util.Time.now() > start + timeout)
				{
					break;
				}
				try
				{
					java.lang.Thread.sleep(250);
				}
				catch (System.Exception)
				{
				}
			}
			// ignore
			return false;
		}

		public static bool waitForServerDown(string hp, long timeout)
		{
			long start = org.apache.hadoop.util.Time.now();
			while (true)
			{
				try
				{
					org.apache.hadoop.ha.ClientBaseWithFixes.HostPort hpobj = parseHostPortList(hp)[0
						];
					send4LetterWord(hpobj.host, hpobj.port, "stat");
				}
				catch (System.IO.IOException)
				{
					return true;
				}
				if (org.apache.hadoop.util.Time.now() > start + timeout)
				{
					break;
				}
				try
				{
					java.lang.Thread.sleep(250);
				}
				catch (System.Exception)
				{
				}
			}
			// ignore
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		public static java.io.File createTmpDir()
		{
			return createTmpDir(BASETEST);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static java.io.File createTmpDir(java.io.File parentDir)
		{
			java.io.File tmpFile = java.io.File.createTempFile("test", ".junit", parentDir);
			// don't delete tmpFile - this ensures we don't attempt to create
			// a tmpDir with a duplicate name
			java.io.File tmpDir = new java.io.File(tmpFile + ".dir");
			NUnit.Framework.Assert.IsFalse(tmpDir.exists());
			// never true if tmpfile does it's job
			NUnit.Framework.Assert.IsTrue(tmpDir.mkdirs());
			return tmpDir;
		}

		private static int getPort(string hostPort)
		{
			string[] split = hostPort.split(":");
			string portstr = split[split.Length - 1];
			string[] pc = portstr.split("/");
			if (pc.Length > 1)
			{
				portstr = pc[0];
			}
			return System.Convert.ToInt32(portstr);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		internal static org.apache.zookeeper.server.ServerCnxnFactory createNewServerInstance
			(java.io.File dataDir, org.apache.zookeeper.server.ServerCnxnFactory factory, string
			 hostPort, int maxCnxns)
		{
			org.apache.zookeeper.server.ZooKeeperServer zks = new org.apache.zookeeper.server.ZooKeeperServer
				(dataDir, dataDir, 3000);
			int PORT = getPort(hostPort);
			if (factory == null)
			{
				factory = org.apache.zookeeper.server.ServerCnxnFactory.createFactory(PORT, maxCnxns
					);
			}
			factory.startup(zks);
			NUnit.Framework.Assert.IsTrue("waiting for server up", org.apache.hadoop.ha.ClientBaseWithFixes
				.waitForServerUp("127.0.0.1:" + PORT, CONNECTION_TIMEOUT));
			return factory;
		}

		internal static void shutdownServerInstance(org.apache.zookeeper.server.ServerCnxnFactory
			 factory, string hostPort)
		{
			if (factory != null)
			{
				org.apache.zookeeper.server.ZKDatabase zkDb;
				{
					org.apache.zookeeper.server.ZooKeeperServer zs = getServer(factory);
					zkDb = zs.getZKDatabase();
				}
				factory.shutdown();
				try
				{
					zkDb.close();
				}
				catch (System.IO.IOException ie)
				{
					LOG.warn("Error closing logs ", ie);
				}
				int PORT = getPort(hostPort);
				NUnit.Framework.Assert.IsTrue("waiting for server down", org.apache.hadoop.ha.ClientBaseWithFixes
					.waitForServerDown("127.0.0.1:" + PORT, CONNECTION_TIMEOUT));
			}
		}

		/// <summary>Test specific setup</summary>
		public static void setupTestEnv()
		{
			// during the tests we run with 100K prealloc in the logs.
			// on windows systems prealloc of 64M was seen to take ~15seconds
			// resulting in test Assert.failure (client timeout on first session).
			// set env and directly in order to handle static init/gc issues
			Sharpen.Runtime.setProperty("zookeeper.preAllocSize", "100");
			org.apache.zookeeper.server.persistence.FileTxnLog.setPreallocSize(100 * 1024);
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void setUpAll()
		{
			allClients = new System.Collections.Generic.LinkedList<org.apache.zookeeper.ZooKeeper
				>();
			allClientsSetup = true;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void setUp()
		{
			BASETEST.mkdirs();
			setupTestEnv();
			setUpAll();
			tmpDir = createTmpDir(BASETEST);
			startServer();
			LOG.info("Client test setup finished");
		}

		private string initHostPort()
		{
			BASETEST.mkdirs();
			int port;
			for (; ; )
			{
				port = org.apache.zookeeper.PortAssignment.unique();
				java.nio.channels.FileLock Lock = null;
				portNumLockFile = null;
				try
				{
					try
					{
						portNumFile = new java.io.File(BASETEST, port + ".lock");
						portNumLockFile = new java.io.RandomAccessFile(portNumFile, "rw");
						try
						{
							Lock = portNumLockFile.getChannel().tryLock();
						}
						catch (java.nio.channels.OverlappingFileLockException)
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
							portNumLockFile.close();
						}
					}
				}
				catch (System.IO.IOException e)
				{
					throw new System.Exception(e);
				}
			}
			return "127.0.0.1:" + port;
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void startServer()
		{
			LOG.info("STARTING server");
			serverFactory = createNewServerInstance(tmpDir, serverFactory, hostPort, maxCnxns
				);
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void stopServer()
		{
			LOG.info("STOPPING server");
			shutdownServerInstance(serverFactory, hostPort);
			serverFactory = null;
		}

		protected internal static org.apache.zookeeper.server.ZooKeeperServer getServer(org.apache.zookeeper.server.ServerCnxnFactory
			 fac)
		{
			org.apache.zookeeper.server.ZooKeeperServer zs = org.apache.zookeeper.server.ServerCnxnFactoryAccessor
				.getZkServer(fac);
			return zs;
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void tearDownAll()
		{
			lock (this)
			{
				if (allClients != null)
				{
					foreach (org.apache.zookeeper.ZooKeeper zk in allClients)
					{
						try
						{
							if (zk != null)
							{
								zk.close();
							}
						}
						catch (System.Exception e)
						{
							LOG.warn("ignoring interrupt", e);
						}
					}
				}
				allClients = null;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void tearDown()
		{
			LOG.info("tearDown starting");
			tearDownAll();
			stopServer();
			portNumLockFile.close();
			portNumFile.delete();
			if (tmpDir != null)
			{
				NUnit.Framework.Assert.IsTrue("delete " + tmpDir.ToString(), recursiveDelete(tmpDir
					));
			}
			// This has to be set to null when the same instance of this class is reused between test cases
			serverFactory = null;
		}

		public static bool recursiveDelete(java.io.File d)
		{
			if (d.isDirectory())
			{
				java.io.File[] children = d.listFiles();
				foreach (java.io.File f in children)
				{
					NUnit.Framework.Assert.IsTrue("delete " + f.ToString(), recursiveDelete(f));
				}
			}
			return d.delete();
		}

		public ClientBaseWithFixes()
		{
			hostPort = initHostPort();
		}
	}
}
