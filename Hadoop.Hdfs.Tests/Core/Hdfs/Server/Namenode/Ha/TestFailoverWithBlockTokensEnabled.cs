using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	public class TestFailoverWithBlockTokensEnabled
	{
		private static readonly Path TestPath = new Path("/test-path");

		private const string TestData = "very important text";

		private Configuration conf;

		private MiniDFSCluster cluster;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void StartCluster()
		{
			conf = new Configuration();
			conf.SetBoolean(DFSConfigKeys.DfsBlockAccessTokenEnableKey, true);
			// Set short retry timeouts so this test runs faster
			conf.SetInt(DFSConfigKeys.DfsClientRetryWindowBase, 10);
			cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleHATopology
				()).NumDataNodes(1).Build();
		}

		[TearDown]
		public virtual void ShutDownCluster()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		[NUnit.Framework.Test]
		public virtual void EnsureSerialNumbersNeverOverlap()
		{
			BlockTokenSecretManager btsm1 = cluster.GetNamesystem(0).GetBlockManager().GetBlockTokenSecretManager
				();
			BlockTokenSecretManager btsm2 = cluster.GetNamesystem(1).GetBlockManager().GetBlockTokenSecretManager
				();
			btsm1.SetSerialNo(0);
			btsm2.SetSerialNo(0);
			NUnit.Framework.Assert.IsFalse(btsm1.GetSerialNoForTesting() == btsm2.GetSerialNoForTesting
				());
			btsm1.SetSerialNo(int.MaxValue);
			btsm2.SetSerialNo(int.MaxValue);
			NUnit.Framework.Assert.IsFalse(btsm1.GetSerialNoForTesting() == btsm2.GetSerialNoForTesting
				());
			btsm1.SetSerialNo(int.MinValue);
			btsm2.SetSerialNo(int.MinValue);
			NUnit.Framework.Assert.IsFalse(btsm1.GetSerialNoForTesting() == btsm2.GetSerialNoForTesting
				());
			btsm1.SetSerialNo(int.MaxValue / 2);
			btsm2.SetSerialNo(int.MaxValue / 2);
			NUnit.Framework.Assert.IsFalse(btsm1.GetSerialNoForTesting() == btsm2.GetSerialNoForTesting
				());
			btsm1.SetSerialNo(int.MinValue / 2);
			btsm2.SetSerialNo(int.MinValue / 2);
			NUnit.Framework.Assert.IsFalse(btsm1.GetSerialNoForTesting() == btsm2.GetSerialNoForTesting
				());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void EnsureInvalidBlockTokensAreRejected()
		{
			cluster.TransitionToActive(0);
			FileSystem fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
			DFSTestUtil.WriteFile(fs, TestPath, TestData);
			NUnit.Framework.Assert.AreEqual(TestData, DFSTestUtil.ReadFile(fs, TestPath));
			DFSClient dfsClient = DFSClientAdapter.GetDFSClient((DistributedFileSystem)fs);
			DFSClient spyDfsClient = Org.Mockito.Mockito.Spy(dfsClient);
			Org.Mockito.Mockito.DoAnswer(new _Answer_121()).When(spyDfsClient).GetLocatedBlocks
				(Org.Mockito.Mockito.AnyString(), Org.Mockito.Mockito.AnyLong(), Org.Mockito.Mockito
				.AnyLong());
			// This will make the token invalid, since the password
			// won't match anymore
			DFSClientAdapter.SetDFSClient((DistributedFileSystem)fs, spyDfsClient);
			try
			{
				NUnit.Framework.Assert.AreEqual(TestData, DFSTestUtil.ReadFile(fs, TestPath));
				NUnit.Framework.Assert.Fail("Shouldn't have been able to read a file with invalid block tokens"
					);
			}
			catch (IOException ioe)
			{
				GenericTestUtils.AssertExceptionContains("Could not obtain block", ioe);
			}
		}

		private sealed class _Answer_121 : Answer<LocatedBlocks>
		{
			public _Answer_121()
			{
			}

			/// <exception cref="System.Exception"/>
			public LocatedBlocks Answer(InvocationOnMock arg0)
			{
				LocatedBlocks locatedBlocks = (LocatedBlocks)arg0.CallRealMethod();
				foreach (LocatedBlock lb in locatedBlocks.GetLocatedBlocks())
				{
					Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> token = lb.GetBlockToken
						();
					BlockTokenIdentifier id = lb.GetBlockToken().DecodeIdentifier();
					id.SetExpiryDate(Time.Now() + 10);
					Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> newToken = new Org.Apache.Hadoop.Security.Token.Token
						<BlockTokenIdentifier>(id.GetBytes(), token.GetPassword(), token.GetKind(), token
						.GetService());
					lb.SetBlockToken(newToken);
				}
				return locatedBlocks;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestFailoverAfterRegistration()
		{
			WriteUsingBothNameNodes();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailoverAfterAccessKeyUpdate()
		{
			LowerKeyUpdateIntervalAndClearKeys(cluster);
			// Sleep 10s to guarantee DNs heartbeat and get new keys.
			Sharpen.Thread.Sleep(10 * 1000);
			WriteUsingBothNameNodes();
		}

		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		private void WriteUsingBothNameNodes()
		{
			cluster.TransitionToActive(0);
			FileSystem fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
			DFSTestUtil.WriteFile(fs, TestPath, TestData);
			cluster.TransitionToStandby(0);
			cluster.TransitionToActive(1);
			fs.Delete(TestPath, false);
			DFSTestUtil.WriteFile(fs, TestPath, TestData);
		}

		private static void LowerKeyUpdateIntervalAndClearKeys(MiniDFSCluster cluster)
		{
			LowerKeyUpdateIntervalAndClearKeys(cluster.GetNamesystem(0));
			LowerKeyUpdateIntervalAndClearKeys(cluster.GetNamesystem(1));
			foreach (DataNode dn in cluster.GetDataNodes())
			{
				dn.ClearAllBlockSecretKeys();
			}
		}

		private static void LowerKeyUpdateIntervalAndClearKeys(FSNamesystem namesystem)
		{
			BlockTokenSecretManager btsm = namesystem.GetBlockManager().GetBlockTokenSecretManager
				();
			btsm.SetKeyUpdateIntervalForTesting(2 * 1000);
			btsm.SetTokenLifetime(2 * 1000);
			btsm.ClearAllKeysForTesting();
		}
	}
}
