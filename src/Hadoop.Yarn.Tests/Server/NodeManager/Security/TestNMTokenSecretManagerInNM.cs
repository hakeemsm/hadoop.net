using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Security
{
	public class TestNMTokenSecretManagerInNM
	{
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRecovery()
		{
			YarnConfiguration conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.NmRecoveryEnabled, true);
			NodeId nodeId = NodeId.NewInstance("somehost", 1234);
			ApplicationAttemptId attempt1 = ApplicationAttemptId.NewInstance(ApplicationId.NewInstance
				(1, 1), 1);
			ApplicationAttemptId attempt2 = ApplicationAttemptId.NewInstance(ApplicationId.NewInstance
				(2, 2), 2);
			TestNMTokenSecretManagerInNM.NMTokenKeyGeneratorForTest keygen = new TestNMTokenSecretManagerInNM.NMTokenKeyGeneratorForTest
				();
			NMMemoryStateStoreService stateStore = new NMMemoryStateStoreService();
			stateStore.Init(conf);
			stateStore.Start();
			NMTokenSecretManagerInNM secretMgr = new NMTokenSecretManagerInNM(stateStore);
			secretMgr.SetNodeId(nodeId);
			MasterKey currentKey = keygen.GenerateKey();
			secretMgr.SetMasterKey(currentKey);
			NMTokenIdentifier attemptToken1 = GetNMTokenId(secretMgr.CreateNMToken(attempt1, 
				nodeId, "user1"));
			NMTokenIdentifier attemptToken2 = GetNMTokenId(secretMgr.CreateNMToken(attempt2, 
				nodeId, "user2"));
			secretMgr.AppAttemptStartContainer(attemptToken1);
			secretMgr.AppAttemptStartContainer(attemptToken2);
			NUnit.Framework.Assert.IsTrue(secretMgr.IsAppAttemptNMTokenKeyPresent(attempt1));
			NUnit.Framework.Assert.IsTrue(secretMgr.IsAppAttemptNMTokenKeyPresent(attempt2));
			NUnit.Framework.Assert.IsNotNull(secretMgr.RetrievePassword(attemptToken1));
			NUnit.Framework.Assert.IsNotNull(secretMgr.RetrievePassword(attemptToken2));
			// restart and verify key is still there and token still valid
			secretMgr = new NMTokenSecretManagerInNM(stateStore);
			secretMgr.Recover();
			secretMgr.SetNodeId(nodeId);
			NUnit.Framework.Assert.AreEqual(currentKey, secretMgr.GetCurrentKey());
			NUnit.Framework.Assert.IsTrue(secretMgr.IsAppAttemptNMTokenKeyPresent(attempt1));
			NUnit.Framework.Assert.IsTrue(secretMgr.IsAppAttemptNMTokenKeyPresent(attempt2));
			NUnit.Framework.Assert.IsNotNull(secretMgr.RetrievePassword(attemptToken1));
			NUnit.Framework.Assert.IsNotNull(secretMgr.RetrievePassword(attemptToken2));
			// roll master key and remove an app
			currentKey = keygen.GenerateKey();
			secretMgr.SetMasterKey(currentKey);
			secretMgr.AppFinished(attempt1.GetApplicationId());
			// restart and verify attempt1 key is still valid due to prev key persist
			secretMgr = new NMTokenSecretManagerInNM(stateStore);
			secretMgr.Recover();
			secretMgr.SetNodeId(nodeId);
			NUnit.Framework.Assert.AreEqual(currentKey, secretMgr.GetCurrentKey());
			NUnit.Framework.Assert.IsFalse(secretMgr.IsAppAttemptNMTokenKeyPresent(attempt1));
			NUnit.Framework.Assert.IsTrue(secretMgr.IsAppAttemptNMTokenKeyPresent(attempt2));
			NUnit.Framework.Assert.IsNotNull(secretMgr.RetrievePassword(attemptToken1));
			NUnit.Framework.Assert.IsNotNull(secretMgr.RetrievePassword(attemptToken2));
			// roll master key again, restart, and verify attempt1 key is bad but
			// attempt2 is still good due to app key persist
			currentKey = keygen.GenerateKey();
			secretMgr.SetMasterKey(currentKey);
			secretMgr = new NMTokenSecretManagerInNM(stateStore);
			secretMgr.Recover();
			secretMgr.SetNodeId(nodeId);
			NUnit.Framework.Assert.AreEqual(currentKey, secretMgr.GetCurrentKey());
			NUnit.Framework.Assert.IsFalse(secretMgr.IsAppAttemptNMTokenKeyPresent(attempt1));
			NUnit.Framework.Assert.IsTrue(secretMgr.IsAppAttemptNMTokenKeyPresent(attempt2));
			try
			{
				secretMgr.RetrievePassword(attemptToken1);
				NUnit.Framework.Assert.Fail("attempt token should not still be valid");
			}
			catch (SecretManager.InvalidToken)
			{
			}
			// expected
			NUnit.Framework.Assert.IsNotNull(secretMgr.RetrievePassword(attemptToken2));
			// remove last attempt, restart, verify both tokens are now bad
			secretMgr.AppFinished(attempt2.GetApplicationId());
			secretMgr = new NMTokenSecretManagerInNM(stateStore);
			secretMgr.Recover();
			secretMgr.SetNodeId(nodeId);
			NUnit.Framework.Assert.AreEqual(currentKey, secretMgr.GetCurrentKey());
			NUnit.Framework.Assert.IsFalse(secretMgr.IsAppAttemptNMTokenKeyPresent(attempt1));
			NUnit.Framework.Assert.IsFalse(secretMgr.IsAppAttemptNMTokenKeyPresent(attempt2));
			try
			{
				secretMgr.RetrievePassword(attemptToken1);
				NUnit.Framework.Assert.Fail("attempt token should not still be valid");
			}
			catch (SecretManager.InvalidToken)
			{
			}
			// expected
			try
			{
				secretMgr.RetrievePassword(attemptToken2);
				NUnit.Framework.Assert.Fail("attempt token should not still be valid");
			}
			catch (SecretManager.InvalidToken)
			{
			}
			// expected
			stateStore.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private NMTokenIdentifier GetNMTokenId(Org.Apache.Hadoop.Yarn.Api.Records.Token token
			)
		{
			Org.Apache.Hadoop.Security.Token.Token<NMTokenIdentifier> convertedToken = ConverterUtils
				.ConvertFromYarn(token, (Text)null);
			return convertedToken.DecodeIdentifier();
		}

		private class NMTokenKeyGeneratorForTest : BaseNMTokenSecretManager
		{
			public virtual MasterKey GenerateKey()
			{
				return CreateNewMasterKey().GetMasterKey();
			}
		}
	}
}
