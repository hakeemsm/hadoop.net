using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestGenericJournalConf
	{
		private const string DummyUri = "dummy://test";

		/// <summary>
		/// Test that an exception is thrown if a journal class doesn't exist
		/// in the configuration
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestNotConfigured()
		{
			MiniDFSCluster cluster = null;
			Configuration conf = new Configuration();
			conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, "dummy://test");
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				cluster.WaitActive();
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>
		/// Test that an exception is thrown if a journal class doesn't
		/// exist in the classloader.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestClassDoesntExist()
		{
			MiniDFSCluster cluster = null;
			Configuration conf = new Configuration();
			conf.Set(DFSConfigKeys.DfsNamenodeEditsPluginPrefix + ".dummy", "org.apache.hadoop.nonexistent"
				);
			conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, "dummy://test");
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				cluster.WaitActive();
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>
		/// Test that a implementation of JournalManager without a
		/// (Configuration,URI) constructor throws an exception
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBadConstructor()
		{
			MiniDFSCluster cluster = null;
			Configuration conf = new Configuration();
			conf.Set(DFSConfigKeys.DfsNamenodeEditsPluginPrefix + ".dummy", typeof(TestGenericJournalConf.BadConstructorJournalManager
				).FullName);
			conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, "dummy://test");
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				cluster.WaitActive();
				NUnit.Framework.Assert.Fail("Should have failed before this point");
			}
			catch (ArgumentException iae)
			{
				if (!iae.Message.Contains("Unable to construct journal"))
				{
					NUnit.Framework.Assert.Fail("Should have failed with unable to construct exception"
						);
				}
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>
		/// Test that a dummy implementation of JournalManager can
		/// be initialized on startup
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDummyJournalManager()
		{
			MiniDFSCluster cluster = null;
			Configuration conf = new Configuration();
			conf.Set(DFSConfigKeys.DfsNamenodeEditsPluginPrefix + ".dummy", typeof(TestGenericJournalConf.DummyJournalManager
				).FullName);
			conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, DummyUri);
			conf.SetInt(DFSConfigKeys.DfsNamenodeCheckedVolumesMinimumKey, 0);
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				cluster.WaitActive();
				NUnit.Framework.Assert.IsTrue(TestGenericJournalConf.DummyJournalManager.shouldPromptCalled
					);
				NUnit.Framework.Assert.IsTrue(TestGenericJournalConf.DummyJournalManager.formatCalled
					);
				NUnit.Framework.Assert.IsNotNull(TestGenericJournalConf.DummyJournalManager.conf);
				NUnit.Framework.Assert.AreEqual(new URI(DummyUri), TestGenericJournalConf.DummyJournalManager
					.uri);
				NUnit.Framework.Assert.IsNotNull(TestGenericJournalConf.DummyJournalManager.nsInfo
					);
				NUnit.Framework.Assert.AreEqual(TestGenericJournalConf.DummyJournalManager.nsInfo
					.GetClusterID(), cluster.GetNameNode().GetNamesystem().GetClusterId());
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		public class DummyJournalManager : JournalManager
		{
			internal static Configuration conf = null;

			internal static URI uri = null;

			internal static NamespaceInfo nsInfo = null;

			internal static bool formatCalled = false;

			internal static bool shouldPromptCalled = false;

			public DummyJournalManager(Configuration conf, URI u, NamespaceInfo nsInfo)
			{
				// Set static vars so the test case can verify them.
				TestGenericJournalConf.DummyJournalManager.conf = conf;
				TestGenericJournalConf.DummyJournalManager.uri = u;
				TestGenericJournalConf.DummyJournalManager.nsInfo = nsInfo;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Format(NamespaceInfo nsInfo)
			{
				formatCalled = true;
			}

			/// <exception cref="System.IO.IOException"/>
			public override EditLogOutputStream StartLogSegment(long txId, int layoutVersion)
			{
				return Org.Mockito.Mockito.Mock<EditLogOutputStream>();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void FinalizeLogSegment(long firstTxId, long lastTxId)
			{
			}

			// noop
			public virtual void SelectInputStreams(ICollection<EditLogInputStream> streams, long
				 fromTxnId, bool inProgressOk)
			{
			}

			public override void SetOutputBufferCapacity(int size)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void PurgeLogsOlderThan(long minTxIdToKeep)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void RecoverUnfinalizedSegments()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual bool HasSomeData()
			{
				shouldPromptCalled = true;
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void DoPreUpgrade()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void DoUpgrade(Storage storage)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void DoFinalize()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool CanRollBack(StorageInfo storage, StorageInfo prevStorage, int
				 targetLayoutVersion)
			{
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void DoRollback()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void DiscardSegments(long startTxId)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override long GetJournalCTime()
			{
				return -1;
			}
		}

		public class BadConstructorJournalManager : TestGenericJournalConf.DummyJournalManager
		{
			public BadConstructorJournalManager()
				: base(null, null, null)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void DoPreUpgrade()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void DoUpgrade(Storage storage)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void DoFinalize()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool CanRollBack(StorageInfo storage, StorageInfo prevStorage, int
				 targetLayoutVersion)
			{
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void DoRollback()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override long GetJournalCTime()
			{
				return -1;
			}
		}
	}
}
