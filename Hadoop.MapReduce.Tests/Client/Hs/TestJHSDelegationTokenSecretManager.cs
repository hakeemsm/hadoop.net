using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication.Util;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS
{
	public class TestJHSDelegationTokenSecretManager
	{
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRecovery()
		{
			Configuration conf = new Configuration();
			HistoryServerStateStoreService store = new HistoryServerMemStateStoreService();
			store.Init(conf);
			store.Start();
			TestJHSDelegationTokenSecretManager.JHSDelegationTokenSecretManagerForTest mgr = 
				new TestJHSDelegationTokenSecretManager.JHSDelegationTokenSecretManagerForTest(store
				);
			mgr.StartThreads();
			MRDelegationTokenIdentifier tokenId1 = new MRDelegationTokenIdentifier(new Text("tokenOwner"
				), new Text("tokenRenewer"), new Text("tokenUser"));
			Org.Apache.Hadoop.Security.Token.Token<MRDelegationTokenIdentifier> token1 = new 
				Org.Apache.Hadoop.Security.Token.Token<MRDelegationTokenIdentifier>(tokenId1, mgr
				);
			MRDelegationTokenIdentifier tokenId2 = new MRDelegationTokenIdentifier(new Text("tokenOwner"
				), new Text("tokenRenewer"), new Text("tokenUser"));
			Org.Apache.Hadoop.Security.Token.Token<MRDelegationTokenIdentifier> token2 = new 
				Org.Apache.Hadoop.Security.Token.Token<MRDelegationTokenIdentifier>(tokenId2, mgr
				);
			DelegationKey[] keys = mgr.GetAllKeys();
			long tokenRenewDate1 = mgr.GetAllTokens()[tokenId1].GetRenewDate();
			long tokenRenewDate2 = mgr.GetAllTokens()[tokenId2].GetRenewDate();
			mgr.StopThreads();
			mgr = new TestJHSDelegationTokenSecretManager.JHSDelegationTokenSecretManagerForTest
				(store);
			mgr.Recover(store.LoadState());
			IList<DelegationKey> recoveredKeys = Arrays.AsList(mgr.GetAllKeys());
			foreach (DelegationKey key in keys)
			{
				NUnit.Framework.Assert.IsTrue("key missing after recovery", recoveredKeys.Contains
					(key));
			}
			NUnit.Framework.Assert.IsTrue("token1 missing", mgr.GetAllTokens().Contains(tokenId1
				));
			NUnit.Framework.Assert.AreEqual("token1 renew date", tokenRenewDate1, mgr.GetAllTokens
				()[tokenId1].GetRenewDate());
			NUnit.Framework.Assert.IsTrue("token2 missing", mgr.GetAllTokens().Contains(tokenId2
				));
			NUnit.Framework.Assert.AreEqual("token2 renew date", tokenRenewDate2, mgr.GetAllTokens
				()[tokenId2].GetRenewDate());
			mgr.StartThreads();
			mgr.VerifyToken(tokenId1, token1.GetPassword());
			mgr.VerifyToken(tokenId2, token2.GetPassword());
			MRDelegationTokenIdentifier tokenId3 = new MRDelegationTokenIdentifier(new Text("tokenOwner"
				), new Text("tokenRenewer"), new Text("tokenUser"));
			Org.Apache.Hadoop.Security.Token.Token<MRDelegationTokenIdentifier> token3 = new 
				Org.Apache.Hadoop.Security.Token.Token<MRDelegationTokenIdentifier>(tokenId3, mgr
				);
			NUnit.Framework.Assert.AreEqual("sequence number restore", tokenId2.GetSequenceNumber
				() + 1, tokenId3.GetSequenceNumber());
			mgr.CancelToken(token1, "tokenOwner");
			// Testing with full principal name
			MRDelegationTokenIdentifier tokenIdFull = new MRDelegationTokenIdentifier(new Text
				("tokenOwner/localhost@LOCALHOST"), new Text("tokenRenewer"), new Text("tokenUser"
				));
			KerberosName.SetRules("RULE:[1:$1]\nRULE:[2:$1]");
			Org.Apache.Hadoop.Security.Token.Token<MRDelegationTokenIdentifier> tokenFull = new 
				Org.Apache.Hadoop.Security.Token.Token<MRDelegationTokenIdentifier>(tokenIdFull, 
				mgr);
			// Negative test
			try
			{
				mgr.CancelToken(tokenFull, "tokenOwner");
			}
			catch (AccessControlException ace)
			{
				NUnit.Framework.Assert.IsTrue(ace.Message.Contains("is not authorized to cancel the token"
					));
			}
			// Succeed to cancel with full principal
			mgr.CancelToken(tokenFull, tokenIdFull.GetOwner().ToString());
			long tokenRenewDate3 = mgr.GetAllTokens()[tokenId3].GetRenewDate();
			mgr.StopThreads();
			mgr = new TestJHSDelegationTokenSecretManager.JHSDelegationTokenSecretManagerForTest
				(store);
			mgr.Recover(store.LoadState());
			NUnit.Framework.Assert.IsFalse("token1 should be missing", mgr.GetAllTokens().Contains
				(tokenId1));
			NUnit.Framework.Assert.IsTrue("token2 missing", mgr.GetAllTokens().Contains(tokenId2
				));
			NUnit.Framework.Assert.AreEqual("token2 renew date", tokenRenewDate2, mgr.GetAllTokens
				()[tokenId2].GetRenewDate());
			NUnit.Framework.Assert.IsTrue("token3 missing", mgr.GetAllTokens().Contains(tokenId3
				));
			NUnit.Framework.Assert.AreEqual("token3 renew date", tokenRenewDate3, mgr.GetAllTokens
				()[tokenId3].GetRenewDate());
			mgr.StartThreads();
			mgr.VerifyToken(tokenId2, token2.GetPassword());
			mgr.VerifyToken(tokenId3, token3.GetPassword());
			mgr.StopThreads();
		}

		private class JHSDelegationTokenSecretManagerForTest : JHSDelegationTokenSecretManager
		{
			public JHSDelegationTokenSecretManagerForTest(HistoryServerStateStoreService store
				)
				: base(10000, 10000, 10000, 10000, store)
			{
			}

			public virtual IDictionary<MRDelegationTokenIdentifier, AbstractDelegationTokenSecretManager.DelegationTokenInformation
				> GetAllTokens()
			{
				return new Dictionary<MRDelegationTokenIdentifier, AbstractDelegationTokenSecretManager.DelegationTokenInformation
					>(currentTokens);
			}
		}
	}
}
