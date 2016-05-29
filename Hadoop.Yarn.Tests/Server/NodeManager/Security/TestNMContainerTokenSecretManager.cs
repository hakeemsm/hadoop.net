using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Security
{
	public class TestNMContainerTokenSecretManager
	{
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRecovery()
		{
			YarnConfiguration conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.NmRecoveryEnabled, true);
			NodeId nodeId = NodeId.NewInstance("somehost", 1234);
			ContainerId cid1 = BuilderUtils.NewContainerId(1, 1, 1, 1);
			ContainerId cid2 = BuilderUtils.NewContainerId(2, 2, 2, 2);
			TestNMContainerTokenSecretManager.ContainerTokenKeyGeneratorForTest keygen = new 
				TestNMContainerTokenSecretManager.ContainerTokenKeyGeneratorForTest(conf);
			NMMemoryStateStoreService stateStore = new NMMemoryStateStoreService();
			stateStore.Init(conf);
			stateStore.Start();
			NMContainerTokenSecretManager secretMgr = new NMContainerTokenSecretManager(conf, 
				stateStore);
			secretMgr.SetNodeId(nodeId);
			MasterKey currentKey = keygen.GenerateKey();
			secretMgr.SetMasterKey(currentKey);
			ContainerTokenIdentifier tokenId1 = CreateContainerTokenId(cid1, nodeId, "user1", 
				secretMgr);
			ContainerTokenIdentifier tokenId2 = CreateContainerTokenId(cid2, nodeId, "user2", 
				secretMgr);
			NUnit.Framework.Assert.IsNotNull(secretMgr.RetrievePassword(tokenId1));
			NUnit.Framework.Assert.IsNotNull(secretMgr.RetrievePassword(tokenId2));
			// restart and verify tokens still valid
			secretMgr = new NMContainerTokenSecretManager(conf, stateStore);
			secretMgr.SetNodeId(nodeId);
			secretMgr.Recover();
			NUnit.Framework.Assert.AreEqual(currentKey, secretMgr.GetCurrentKey());
			NUnit.Framework.Assert.IsTrue(secretMgr.IsValidStartContainerRequest(tokenId1));
			NUnit.Framework.Assert.IsTrue(secretMgr.IsValidStartContainerRequest(tokenId2));
			NUnit.Framework.Assert.IsNotNull(secretMgr.RetrievePassword(tokenId1));
			NUnit.Framework.Assert.IsNotNull(secretMgr.RetrievePassword(tokenId2));
			// roll master key and start a container
			secretMgr.StartContainerSuccessful(tokenId2);
			currentKey = keygen.GenerateKey();
			secretMgr.SetMasterKey(currentKey);
			// restart and verify tokens still valid due to prev key persist
			secretMgr = new NMContainerTokenSecretManager(conf, stateStore);
			secretMgr.SetNodeId(nodeId);
			secretMgr.Recover();
			NUnit.Framework.Assert.AreEqual(currentKey, secretMgr.GetCurrentKey());
			NUnit.Framework.Assert.IsTrue(secretMgr.IsValidStartContainerRequest(tokenId1));
			NUnit.Framework.Assert.IsFalse(secretMgr.IsValidStartContainerRequest(tokenId2));
			NUnit.Framework.Assert.IsNotNull(secretMgr.RetrievePassword(tokenId1));
			NUnit.Framework.Assert.IsNotNull(secretMgr.RetrievePassword(tokenId2));
			// roll master key again, restart, and verify keys no longer valid
			currentKey = keygen.GenerateKey();
			secretMgr.SetMasterKey(currentKey);
			secretMgr = new NMContainerTokenSecretManager(conf, stateStore);
			secretMgr.SetNodeId(nodeId);
			secretMgr.Recover();
			NUnit.Framework.Assert.AreEqual(currentKey, secretMgr.GetCurrentKey());
			NUnit.Framework.Assert.IsTrue(secretMgr.IsValidStartContainerRequest(tokenId1));
			NUnit.Framework.Assert.IsFalse(secretMgr.IsValidStartContainerRequest(tokenId2));
			try
			{
				secretMgr.RetrievePassword(tokenId1);
				NUnit.Framework.Assert.Fail("token should not be valid");
			}
			catch (SecretManager.InvalidToken)
			{
			}
			// expected
			try
			{
				secretMgr.RetrievePassword(tokenId2);
				NUnit.Framework.Assert.Fail("token should not be valid");
			}
			catch (SecretManager.InvalidToken)
			{
			}
			// expected
			stateStore.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private static ContainerTokenIdentifier CreateContainerTokenId(ContainerId cid, NodeId
			 nodeId, string user, NMContainerTokenSecretManager secretMgr)
		{
			long rmid = cid.GetApplicationAttemptId().GetApplicationId().GetClusterTimestamp(
				);
			ContainerTokenIdentifier ctid = new ContainerTokenIdentifier(cid, nodeId.ToString
				(), user, BuilderUtils.NewResource(1024, 1), Runtime.CurrentTimeMillis() + 100000L
				, secretMgr.GetCurrentKey().GetKeyId(), rmid, Priority.NewInstance(0), 0);
			Org.Apache.Hadoop.Yarn.Api.Records.Token token = BuilderUtils.NewContainerToken(nodeId
				, secretMgr.CreatePassword(ctid), ctid);
			return BuilderUtils.NewContainerTokenIdentifier(token);
		}

		private class ContainerTokenKeyGeneratorForTest : BaseContainerTokenSecretManager
		{
			public ContainerTokenKeyGeneratorForTest(Configuration conf)
				: base(conf)
			{
			}

			public virtual MasterKey GenerateKey()
			{
				return CreateNewMasterKey().GetMasterKey();
			}
		}
	}
}
