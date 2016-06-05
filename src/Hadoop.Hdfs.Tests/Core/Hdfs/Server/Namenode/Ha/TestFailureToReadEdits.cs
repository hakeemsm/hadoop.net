using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using NUnit.Framework;
using NUnit.Framework.Runners;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Qjournal;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Mockito;
using Org.Mockito.Invocation;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	public class TestFailureToReadEdits
	{
		private const string TestDir1 = "/test1";

		private const string TestDir2 = "/test2";

		private const string TestDir3 = "/test3";

		private readonly TestFailureToReadEdits.TestType clusterType;

		private Configuration conf;

		private MiniDFSCluster cluster;

		private MiniQJMHACluster miniQjmHaCluster;

		private NameNode nn0;

		private NameNode nn1;

		private FileSystem fs;

		private enum TestType
		{
			SharedDirHa,
			QjmHa
		}

		// for QJM case only
		/// <summary>
		/// Run this suite of tests both for QJM-based HA and for file-based
		/// HA.
		/// </summary>
		[Parameterized.Parameters]
		public static IEnumerable<object[]> Data()
		{
			return Arrays.AsList(new object[][] { new object[] { TestFailureToReadEdits.TestType
				.SharedDirHa }, new object[] { TestFailureToReadEdits.TestType.QjmHa } });
		}

		public TestFailureToReadEdits(TestFailureToReadEdits.TestType clusterType)
		{
			this.clusterType = clusterType;
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void SetUpCluster()
		{
			conf = new Configuration();
			conf.SetInt(DFSConfigKeys.DfsNamenodeCheckpointCheckPeriodKey, 1);
			conf.SetInt(DFSConfigKeys.DfsNamenodeCheckpointTxnsKey, 1);
			conf.SetInt(DFSConfigKeys.DfsNamenodeNumCheckpointsRetainedKey, 10);
			conf.SetInt(DFSConfigKeys.DfsHaTaileditsPeriodKey, 1);
			HAUtil.SetAllowStandbyReads(conf, true);
			if (clusterType == TestFailureToReadEdits.TestType.SharedDirHa)
			{
				MiniDFSNNTopology topology = MiniQJMHACluster.CreateDefaultTopology(10000);
				cluster = new MiniDFSCluster.Builder(conf).NnTopology(topology).NumDataNodes(0).CheckExitOnShutdown
					(false).Build();
			}
			else
			{
				MiniQJMHACluster.Builder builder = new MiniQJMHACluster.Builder(conf);
				builder.GetDfsBuilder().NumDataNodes(0).CheckExitOnShutdown(false);
				miniQjmHaCluster = builder.Build();
				cluster = miniQjmHaCluster.GetDfsCluster();
			}
			cluster.WaitActive();
			nn0 = cluster.GetNameNode(0);
			nn1 = cluster.GetNameNode(1);
			cluster.TransitionToActive(0);
			fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void TearDownCluster()
		{
			if (fs != null)
			{
				fs.Close();
			}
			if (clusterType == TestFailureToReadEdits.TestType.SharedDirHa)
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
			else
			{
				if (miniQjmHaCluster != null)
				{
					miniQjmHaCluster.Shutdown();
				}
			}
		}

		/// <summary>
		/// Test that the standby NN won't double-replay earlier edits if it encounters
		/// a failure to read a later edit.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailuretoReadEdits()
		{
			NUnit.Framework.Assert.IsTrue(fs.Mkdirs(new Path(TestDir1)));
			HATestUtil.WaitForStandbyToCatchUp(nn0, nn1);
			// If these two ops are applied twice, the first op will throw an
			// exception the second time its replayed.
			fs.SetOwner(new Path(TestDir1), "foo", "bar");
			NUnit.Framework.Assert.IsTrue(fs.Delete(new Path(TestDir1), true));
			// This op should get applied just fine.
			NUnit.Framework.Assert.IsTrue(fs.Mkdirs(new Path(TestDir2)));
			// This is the op the mocking will cause to fail to be read.
			NUnit.Framework.Assert.IsTrue(fs.Mkdirs(new Path(TestDir3)));
			TestFailureToReadEdits.LimitedEditLogAnswer answer = CauseFailureOnEditLogRead();
			try
			{
				HATestUtil.WaitForStandbyToCatchUp(nn0, nn1);
				NUnit.Framework.Assert.Fail("Standby fully caught up, but should not have been able to"
					);
			}
			catch (HATestUtil.CouldNotCatchUpException)
			{
			}
			// Expected. The NN did not exit.
			// Null because it was deleted.
			NUnit.Framework.Assert.IsNull(NameNodeAdapter.GetFileInfo(nn1, TestDir1, false));
			// Should have been successfully created.
			NUnit.Framework.Assert.IsTrue(NameNodeAdapter.GetFileInfo(nn1, TestDir2, false).IsDir
				());
			// Null because it hasn't been created yet.
			NUnit.Framework.Assert.IsNull(NameNodeAdapter.GetFileInfo(nn1, TestDir3, false));
			// Now let the standby read ALL the edits.
			answer.SetThrowExceptionOnRead(false);
			HATestUtil.WaitForStandbyToCatchUp(nn0, nn1);
			// Null because it was deleted.
			NUnit.Framework.Assert.IsNull(NameNodeAdapter.GetFileInfo(nn1, TestDir1, false));
			// Should have been successfully created.
			NUnit.Framework.Assert.IsTrue(NameNodeAdapter.GetFileInfo(nn1, TestDir2, false).IsDir
				());
			// Should now have been successfully created.
			NUnit.Framework.Assert.IsTrue(NameNodeAdapter.GetFileInfo(nn1, TestDir3, false).IsDir
				());
		}

		/// <summary>
		/// Test the following case:
		/// 1.
		/// </summary>
		/// <remarks>
		/// Test the following case:
		/// 1. SBN is reading a finalized edits file when NFS disappears halfway
		/// through (or some intermittent error happens)
		/// 2. SBN performs a checkpoint and uploads it to the NN
		/// 3. NN receives a checkpoint that doesn't correspond to the end of any log
		/// segment
		/// 4. Both NN and SBN should be able to restart at this point.
		/// This is a regression test for HDFS-2766.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCheckpointStartingMidEditsFile()
		{
			NUnit.Framework.Assert.IsTrue(fs.Mkdirs(new Path(TestDir1)));
			HATestUtil.WaitForStandbyToCatchUp(nn0, nn1);
			// Once the standby catches up, it should notice that it needs to
			// do a checkpoint and save one to its local directories.
			HATestUtil.WaitForCheckpoint(cluster, 1, ImmutableList.Of(0, 3));
			// It should also upload it back to the active.
			HATestUtil.WaitForCheckpoint(cluster, 0, ImmutableList.Of(0, 3));
			CauseFailureOnEditLogRead();
			NUnit.Framework.Assert.IsTrue(fs.Mkdirs(new Path(TestDir2)));
			NUnit.Framework.Assert.IsTrue(fs.Mkdirs(new Path(TestDir3)));
			try
			{
				HATestUtil.WaitForStandbyToCatchUp(nn0, nn1);
				NUnit.Framework.Assert.Fail("Standby fully caught up, but should not have been able to"
					);
			}
			catch (HATestUtil.CouldNotCatchUpException)
			{
			}
			// Expected. The NN did not exit.
			// 5 because we should get OP_START_LOG_SEGMENT and one successful OP_MKDIR
			HATestUtil.WaitForCheckpoint(cluster, 1, ImmutableList.Of(0, 3, 5));
			// It should also upload it back to the active.
			HATestUtil.WaitForCheckpoint(cluster, 0, ImmutableList.Of(0, 3, 5));
			// Restart the active NN
			cluster.RestartNameNode(0);
			HATestUtil.WaitForCheckpoint(cluster, 0, ImmutableList.Of(0, 3, 5));
			FileSystem fs0 = null;
			try
			{
				// Make sure that when the active restarts, it loads all the edits.
				fs0 = FileSystem.Get(NameNode.GetUri(nn0.GetNameNodeAddress()), conf);
				NUnit.Framework.Assert.IsTrue(fs0.Exists(new Path(TestDir1)));
				NUnit.Framework.Assert.IsTrue(fs0.Exists(new Path(TestDir2)));
				NUnit.Framework.Assert.IsTrue(fs0.Exists(new Path(TestDir3)));
			}
			finally
			{
				if (fs0 != null)
				{
					fs0.Close();
				}
			}
		}

		/// <summary>
		/// Ensure that the standby fails to become active if it cannot read all
		/// available edits in the shared edits dir when it is transitioning to active
		/// state.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailureToReadEditsOnTransitionToActive()
		{
			NUnit.Framework.Assert.IsTrue(fs.Mkdirs(new Path(TestDir1)));
			HATestUtil.WaitForStandbyToCatchUp(nn0, nn1);
			// It should also upload it back to the active.
			HATestUtil.WaitForCheckpoint(cluster, 0, ImmutableList.Of(0, 3));
			CauseFailureOnEditLogRead();
			NUnit.Framework.Assert.IsTrue(fs.Mkdirs(new Path(TestDir2)));
			NUnit.Framework.Assert.IsTrue(fs.Mkdirs(new Path(TestDir3)));
			try
			{
				HATestUtil.WaitForStandbyToCatchUp(nn0, nn1);
				NUnit.Framework.Assert.Fail("Standby fully caught up, but should not have been able to"
					);
			}
			catch (HATestUtil.CouldNotCatchUpException)
			{
			}
			// Expected. The NN did not exit.
			// Shutdown the active NN.
			cluster.ShutdownNameNode(0);
			try
			{
				// Transition the standby to active.
				cluster.TransitionToActive(1);
				NUnit.Framework.Assert.Fail("Standby transitioned to active, but should not have been able to"
					);
			}
			catch (ExitUtil.ExitException ee)
			{
				GenericTestUtils.AssertExceptionContains("Error replaying edit log", ee);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private TestFailureToReadEdits.LimitedEditLogAnswer CauseFailureOnEditLogRead()
		{
			FSEditLog spyEditLog = NameNodeAdapter.SpyOnEditLog(nn1);
			TestFailureToReadEdits.LimitedEditLogAnswer answer = new TestFailureToReadEdits.LimitedEditLogAnswer
				();
			Org.Mockito.Mockito.DoAnswer(answer).When(spyEditLog).SelectInputStreams(Matchers.AnyLong
				(), Matchers.AnyLong(), (MetaRecoveryContext)Matchers.AnyObject(), Matchers.AnyBoolean
				());
			return answer;
		}

		private class LimitedEditLogAnswer : Org.Mockito.Stubbing.Answer<ICollection<EditLogInputStream
			>>
		{
			private bool throwExceptionOnRead = true;

			/// <exception cref="System.Exception"/>
			public virtual ICollection<EditLogInputStream> Answer(InvocationOnMock invocation
				)
			{
				ICollection<EditLogInputStream> streams = (ICollection<EditLogInputStream>)invocation
					.CallRealMethod();
				if (!throwExceptionOnRead)
				{
					return streams;
				}
				else
				{
					ICollection<EditLogInputStream> ret = new List<EditLogInputStream>();
					foreach (EditLogInputStream stream in streams)
					{
						EditLogInputStream spyStream = Org.Mockito.Mockito.Spy(stream);
						Org.Mockito.Mockito.DoAnswer(new _Answer_325(this)).When(spyStream).ReadOp();
						ret.AddItem(spyStream);
					}
					return ret;
				}
			}

			private sealed class _Answer_325 : Org.Mockito.Stubbing.Answer<FSEditLogOp>
			{
				public _Answer_325(LimitedEditLogAnswer _enclosing)
				{
					this._enclosing = _enclosing;
				}

				/// <exception cref="System.Exception"/>
				public FSEditLogOp Answer(InvocationOnMock invocation)
				{
					FSEditLogOp op = (FSEditLogOp)invocation.CallRealMethod();
					if (this._enclosing.throwExceptionOnRead && TestFailureToReadEdits.TestDir3.Equals
						(NameNodeAdapter.GetMkdirOpPath(op)))
					{
						throw new IOException("failed to read op creating " + TestFailureToReadEdits.TestDir3
							);
					}
					else
					{
						return op;
					}
				}

				private readonly LimitedEditLogAnswer _enclosing;
			}

			public virtual void SetThrowExceptionOnRead(bool throwExceptionOnRead)
			{
				this.throwExceptionOnRead = throwExceptionOnRead;
			}
		}
	}
}
