using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Tools;
using Sharpen;

namespace Org.Apache.Hadoop
{
	public class TestRefreshCallQueue
	{
		private MiniDFSCluster cluster;

		private Configuration config;

		private FileSystem fs;

		internal static int mockQueueConstructions;

		internal static int mockQueuePuts;

		private string callQueueConfigKey = string.Empty;

		private readonly Random rand = new Random();

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			// We want to count additional events, so we reset here
			mockQueueConstructions = 0;
			mockQueuePuts = 0;
			int portRetries = 5;
			int nnPort;
			for (; portRetries > 0; --portRetries)
			{
				// Pick a random port in the range [30000,60000).
				nnPort = 30000 + rand.Next(30000);
				config = new Configuration();
				callQueueConfigKey = "ipc." + nnPort + ".callqueue.impl";
				config.SetClass(callQueueConfigKey, typeof(TestRefreshCallQueue.MockCallQueue), typeof(
					BlockingQueue));
				config.Set("hadoop.security.authorization", "true");
				FileSystem.SetDefaultUri(config, "hdfs://localhost:" + nnPort);
				fs = FileSystem.Get(config);
				try
				{
					cluster = new MiniDFSCluster.Builder(config).NameNodePort(nnPort).Build();
					cluster.WaitActive();
					break;
				}
				catch (BindException)
				{
				}
			}
			// Retry with a different port number.
			if (portRetries == 0)
			{
				// Bail if we get very unlucky with our choice of ports.
				NUnit.Framework.Assert.Fail("Failed to pick an ephemeral port for the NameNode RPC server."
					);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		[System.Serializable]
		public class MockCallQueue<E> : LinkedBlockingQueue<E>
		{
			public MockCallQueue(int cap, string ns, Configuration conf)
				: base(cap)
			{
				mockQueueConstructions++;
			}

			/// <exception cref="System.Exception"/>
			public override void Put(E e)
			{
				base.Put(e);
				mockQueuePuts++;
			}
		}

		// Returns true if mock queue was used for put
		/// <exception cref="System.IO.IOException"/>
		public virtual bool CanPutInMockQueue()
		{
			int putsBefore = mockQueuePuts;
			fs.Exists(new Path("/"));
			// Make an RPC call
			return mockQueuePuts > putsBefore;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefresh()
		{
			NUnit.Framework.Assert.IsTrue("Mock queue should have been constructed", mockQueueConstructions
				 > 0);
			NUnit.Framework.Assert.IsTrue("Puts are routed through MockQueue", CanPutInMockQueue
				());
			int lastMockQueueConstructions = mockQueueConstructions;
			// Replace queue with the queue specified in core-site.xml, which would be the LinkedBlockingQueue
			DFSAdmin admin = new DFSAdmin(config);
			string[] args = new string[] { "-refreshCallQueue" };
			int exitCode = admin.Run(args);
			NUnit.Framework.Assert.AreEqual("DFSAdmin should return 0", 0, exitCode);
			NUnit.Framework.Assert.AreEqual("Mock queue should have no additional constructions"
				, lastMockQueueConstructions, mockQueueConstructions);
			try
			{
				NUnit.Framework.Assert.IsFalse("Puts are routed through LBQ instead of MockQueue"
					, CanPutInMockQueue());
			}
			catch (IOException)
			{
				NUnit.Framework.Assert.Fail("Could not put into queue at all");
			}
		}
	}
}
