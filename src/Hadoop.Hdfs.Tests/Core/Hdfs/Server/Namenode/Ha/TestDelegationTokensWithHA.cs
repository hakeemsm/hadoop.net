using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Security;
using Com.Google.Common.Base;
using Javax.Servlet.Http;
using Javax.WS.RS.Core;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Apache.Hadoop.Hdfs.Web.Resources;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Test;
using Org.Mockito.Internal.Util.Reflection;
using Org.Mortbay.Util.Ajax;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	/// <summary>Test case for client support of delegation tokens in an HA cluster.</summary>
	/// <remarks>
	/// Test case for client support of delegation tokens in an HA cluster.
	/// See HDFS-2904 for more info.
	/// </remarks>
	public class TestDelegationTokensWithHA
	{
		private static readonly Configuration conf = new Configuration();

		private static readonly Log Log = LogFactory.GetLog(typeof(TestDelegationTokensWithHA
			));

		private static MiniDFSCluster cluster;

		private static NameNode nn0;

		private static NameNode nn1;

		private static FileSystem fs;

		private static DelegationTokenSecretManager dtSecretManager;

		private static DistributedFileSystem dfs;

		private volatile bool catchup = false;

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void SetupCluster()
		{
			SecurityUtilTestHelper.SetTokenServiceUseIp(true);
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeDelegationTokenAlwaysUseKey, true);
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthToLocal, "RULE:[2:$1@$0](JobTracker@.*FOO.COM)s/@.*//"
				 + "DEFAULT");
			cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleHATopology
				()).NumDataNodes(0).Build();
			cluster.WaitActive();
			string logicalName = HATestUtil.GetLogicalHostname(cluster);
			HATestUtil.SetFailoverConfigurations(cluster, conf, logicalName, 0);
			nn0 = cluster.GetNameNode(0);
			nn1 = cluster.GetNameNode(1);
			fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
			dfs = (DistributedFileSystem)fs;
			cluster.TransitionToActive(0);
			dtSecretManager = NameNodeAdapter.GetDtSecretManager(nn0.GetNamesystem());
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void ShutdownCluster()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDelegationTokenDFSApi()
		{
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = GetDelegationToken
				(fs, "JobTracker");
			DelegationTokenIdentifier identifier = new DelegationTokenIdentifier();
			byte[] tokenId = token.GetIdentifier();
			identifier.ReadFields(new DataInputStream(new ByteArrayInputStream(tokenId)));
			// Ensure that it's present in the NN's secret manager and can
			// be renewed directly from there.
			Log.Info("A valid token should have non-null password, " + "and should be renewed successfully"
				);
			NUnit.Framework.Assert.IsTrue(null != dtSecretManager.RetrievePassword(identifier
				));
			dtSecretManager.RenewToken(token, "JobTracker");
			// Use the client conf with the failover info present to check
			// renewal.
			Configuration clientConf = dfs.GetConf();
			DoRenewOrCancel(token, clientConf, TestDelegationTokensWithHA.TokenTestAction.Renew
				);
			// Using a configuration that doesn't have the logical nameservice
			// configured should result in a reasonable error message.
			Configuration emptyConf = new Configuration();
			try
			{
				DoRenewOrCancel(token, emptyConf, TestDelegationTokensWithHA.TokenTestAction.Renew
					);
				NUnit.Framework.Assert.Fail("Did not throw trying to renew with an empty conf!");
			}
			catch (IOException ioe)
			{
				GenericTestUtils.AssertExceptionContains("Unable to map logical nameservice URI", 
					ioe);
			}
			// Ensure that the token can be renewed again after a failover.
			cluster.TransitionToStandby(0);
			cluster.TransitionToActive(1);
			DoRenewOrCancel(token, clientConf, TestDelegationTokensWithHA.TokenTestAction.Renew
				);
			DoRenewOrCancel(token, clientConf, TestDelegationTokensWithHA.TokenTestAction.Cancel
				);
		}

		private class EditLogTailerForTest : EditLogTailer
		{
			public EditLogTailerForTest(TestDelegationTokensWithHA _enclosing, FSNamesystem namesystem
				, Configuration conf)
				: base(namesystem, conf)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void CatchupDuringFailover()
			{
				lock (this._enclosing)
				{
					while (!this._enclosing.catchup)
					{
						try
						{
							EditLogTailer.Log.Info("The editlog tailer is waiting to catchup...");
							Sharpen.Runtime.Wait(this._enclosing);
						}
						catch (Exception)
						{
						}
					}
				}
				base.CatchupDuringFailover();
			}

			private readonly TestDelegationTokensWithHA _enclosing;
		}

		/// <summary>
		/// Test if correct exception (StandbyException or RetriableException) can be
		/// thrown during the NN failover.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestDelegationTokenDuringNNFailover()
		{
			EditLogTailer editLogTailer = nn1.GetNamesystem().GetEditLogTailer();
			// stop the editLogTailer of nn1
			editLogTailer.Stop();
			Configuration conf = (Configuration)Whitebox.GetInternalState(editLogTailer, "conf"
				);
			nn1.GetNamesystem().SetEditLogTailerForTests(new TestDelegationTokensWithHA.EditLogTailerForTest
				(this, nn1.GetNamesystem(), conf));
			// create token
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = GetDelegationToken
				(fs, "JobTracker");
			DelegationTokenIdentifier identifier = new DelegationTokenIdentifier();
			byte[] tokenId = token.GetIdentifier();
			identifier.ReadFields(new DataInputStream(new ByteArrayInputStream(tokenId)));
			// Ensure that it's present in the nn0 secret manager and can
			// be renewed directly from there.
			Log.Info("A valid token should have non-null password, " + "and should be renewed successfully"
				);
			NUnit.Framework.Assert.IsTrue(null != dtSecretManager.RetrievePassword(identifier
				));
			dtSecretManager.RenewToken(token, "JobTracker");
			// transition nn0 to standby
			cluster.TransitionToStandby(0);
			try
			{
				cluster.GetNameNodeRpc(0).RenewDelegationToken(token);
				NUnit.Framework.Assert.Fail("StandbyException is expected since nn0 is in standby state"
					);
			}
			catch (StandbyException e)
			{
				GenericTestUtils.AssertExceptionContains(HAServiceProtocol.HAServiceState.Standby
					.ToString(), e);
			}
			new _Thread_220().Start();
			Sharpen.Thread.Sleep(1000);
			try
			{
				nn1.GetNamesystem().VerifyToken(token.DecodeIdentifier(), token.GetPassword());
				NUnit.Framework.Assert.Fail("RetriableException/StandbyException is expected since nn1 is in transition"
					);
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue(e is StandbyException || e is RetriableException);
				Log.Info("Got expected exception", e);
			}
			catchup = true;
			lock (this)
			{
				Sharpen.Runtime.NotifyAll(this);
			}
			Configuration clientConf = dfs.GetConf();
			DoRenewOrCancel(token, clientConf, TestDelegationTokensWithHA.TokenTestAction.Renew
				);
			DoRenewOrCancel(token, clientConf, TestDelegationTokensWithHA.TokenTestAction.Cancel
				);
		}

		private sealed class _Thread_220 : Sharpen.Thread
		{
			public _Thread_220()
			{
			}

			public override void Run()
			{
				try
				{
					TestDelegationTokensWithHA.cluster.TransitionToActive(1);
				}
				catch (Exception e)
				{
					TestDelegationTokensWithHA.Log.Error("Transition nn1 to active failed", e);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDelegationTokenWithDoAs()
		{
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = GetDelegationToken
				(fs, "JobTracker");
			UserGroupInformation longUgi = UserGroupInformation.CreateRemoteUser("JobTracker/foo.com@FOO.COM"
				);
			UserGroupInformation shortUgi = UserGroupInformation.CreateRemoteUser("JobTracker"
				);
			longUgi.DoAs(new _PrivilegedExceptionAction_260(token));
			// try renew with long name
			shortUgi.DoAs(new _PrivilegedExceptionAction_268(token));
			longUgi.DoAs(new _PrivilegedExceptionAction_275(token));
		}

		private sealed class _PrivilegedExceptionAction_260 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_260(Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> token)
			{
				this.token = token;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				token.Renew(TestDelegationTokensWithHA.conf);
				return null;
			}

			private readonly Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> token;
		}

		private sealed class _PrivilegedExceptionAction_268 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_268(Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> token)
			{
				this.token = token;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				token.Renew(TestDelegationTokensWithHA.conf);
				return null;
			}

			private readonly Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> token;
		}

		private sealed class _PrivilegedExceptionAction_275 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_275(Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> token)
			{
				this.token = token;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				token.Cancel(TestDelegationTokensWithHA.conf);
				return null;
			}

			private readonly Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> token;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestHAUtilClonesDelegationTokens()
		{
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = GetDelegationToken
				(fs, "JobTracker");
			UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser("test");
			URI haUri = new URI("hdfs://my-ha-uri/");
			token.SetService(HAUtil.BuildTokenServiceForLogicalUri(haUri, HdfsConstants.HdfsUriScheme
				));
			ugi.AddToken(token);
			ICollection<IPEndPoint> nnAddrs = new HashSet<IPEndPoint>();
			nnAddrs.AddItem(new IPEndPoint("localhost", nn0.GetNameNodeAddress().Port));
			nnAddrs.AddItem(new IPEndPoint("localhost", nn1.GetNameNodeAddress().Port));
			HAUtil.CloneDelegationTokenForLogicalUri(ugi, haUri, nnAddrs);
			ICollection<Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier>> tokens = ugi
				.GetTokens();
			NUnit.Framework.Assert.AreEqual(3, tokens.Count);
			Log.Info("Tokens:\n" + Joiner.On("\n").Join(tokens));
			DelegationTokenSelector dts = new DelegationTokenSelector();
			// check that the token selected for one of the physical IPC addresses
			// matches the one we received
			foreach (IPEndPoint addr in nnAddrs)
			{
				Text ipcDtService = SecurityUtil.BuildTokenService(addr);
				Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token2 = dts.SelectToken
					(ipcDtService, ugi.GetTokens());
				NUnit.Framework.Assert.IsNotNull(token2);
				Assert.AssertArrayEquals(token.GetIdentifier(), token2.GetIdentifier());
				Assert.AssertArrayEquals(token.GetPassword(), token2.GetPassword());
			}
			// switch to host-based tokens, shouldn't match existing tokens 
			SecurityUtilTestHelper.SetTokenServiceUseIp(false);
			foreach (IPEndPoint addr_1 in nnAddrs)
			{
				Text ipcDtService = SecurityUtil.BuildTokenService(addr_1);
				Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token2 = dts.SelectToken
					(ipcDtService, ugi.GetTokens());
				NUnit.Framework.Assert.IsNull(token2);
			}
			// reclone the tokens, and see if they match now
			HAUtil.CloneDelegationTokenForLogicalUri(ugi, haUri, nnAddrs);
			foreach (IPEndPoint addr_2 in nnAddrs)
			{
				Text ipcDtService = SecurityUtil.BuildTokenService(addr_2);
				Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token2 = dts.SelectToken
					(ipcDtService, ugi.GetTokens());
				NUnit.Framework.Assert.IsNotNull(token2);
				Assert.AssertArrayEquals(token.GetIdentifier(), token2.GetIdentifier());
				Assert.AssertArrayEquals(token.GetPassword(), token2.GetPassword());
			}
		}

		/// <summary>
		/// HDFS-3062: DistributedFileSystem.getCanonicalServiceName() throws an
		/// exception if the URI is a logical URI.
		/// </summary>
		/// <remarks>
		/// HDFS-3062: DistributedFileSystem.getCanonicalServiceName() throws an
		/// exception if the URI is a logical URI. This bug fails the combination of
		/// ha + mapred + security.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestDFSGetCanonicalServiceName()
		{
			URI hAUri = HATestUtil.GetLogicalUri(cluster);
			string haService = HAUtil.BuildTokenServiceForLogicalUri(hAUri, HdfsConstants.HdfsUriScheme
				).ToString();
			NUnit.Framework.Assert.AreEqual(haService, dfs.GetCanonicalServiceName());
			string renewer = UserGroupInformation.GetCurrentUser().GetShortUserName();
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = GetDelegationToken
				(dfs, renewer);
			NUnit.Framework.Assert.AreEqual(haService, token.GetService().ToString());
			// make sure the logical uri is handled correctly
			token.Renew(dfs.GetConf());
			token.Cancel(dfs.GetConf());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestHdfsGetCanonicalServiceName()
		{
			Configuration conf = dfs.GetConf();
			URI haUri = HATestUtil.GetLogicalUri(cluster);
			AbstractFileSystem afs = AbstractFileSystem.CreateFileSystem(haUri, conf);
			string haService = HAUtil.BuildTokenServiceForLogicalUri(haUri, HdfsConstants.HdfsUriScheme
				).ToString();
			NUnit.Framework.Assert.AreEqual(haService, afs.GetCanonicalServiceName());
			Org.Apache.Hadoop.Security.Token.Token<object> token = afs.GetDelegationTokens(UserGroupInformation
				.GetCurrentUser().GetShortUserName())[0];
			NUnit.Framework.Assert.AreEqual(haService, token.GetService().ToString());
			// make sure the logical uri is handled correctly
			token.Renew(conf);
			token.Cancel(conf);
		}

		/// <summary>
		/// Test if StandbyException can be thrown from StandbyNN, when it's requested for
		/// password.
		/// </summary>
		/// <remarks>
		/// Test if StandbyException can be thrown from StandbyNN, when it's requested for
		/// password. (HDFS-6475). With StandbyException, the client can failover to try
		/// activeNN.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestDelegationTokenStandbyNNAppearFirst()
		{
			// make nn0 the standby NN, and nn1 the active NN
			cluster.TransitionToStandby(0);
			cluster.TransitionToActive(1);
			DelegationTokenSecretManager stSecretManager = NameNodeAdapter.GetDtSecretManager
				(nn1.GetNamesystem());
			// create token
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = GetDelegationToken
				(fs, "JobTracker");
			DelegationTokenIdentifier identifier = new DelegationTokenIdentifier();
			byte[] tokenId = token.GetIdentifier();
			identifier.ReadFields(new DataInputStream(new ByteArrayInputStream(tokenId)));
			NUnit.Framework.Assert.IsTrue(null != stSecretManager.RetrievePassword(identifier
				));
			UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser("JobTracker");
			ugi.AddToken(token);
			ugi.DoAs(new _PrivilegedExceptionAction_406(identifier));
		}

		private sealed class _PrivilegedExceptionAction_406 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_406(DelegationTokenIdentifier identifier)
			{
				this.identifier = identifier;
			}

			public object Run()
			{
				try
				{
					try
					{
						byte[] tmppw = TestDelegationTokensWithHA.dtSecretManager.RetrievePassword(identifier
							);
						NUnit.Framework.Assert.Fail("InvalidToken with cause StandbyException is expected"
							 + " since nn0 is standby");
						return tmppw;
					}
					catch (IOException e)
					{
						// Mimic the UserProvider class logic (server side) by throwing
						// SecurityException here
						throw new SecurityException(SecurityUtil.FailedToGetUgiMsgHeader + " " + e, e);
					}
				}
				catch (Exception oe)
				{
					//
					// The exception oe caught here is
					//     java.lang.SecurityException: Failed to obtain user group
					//     information: org.apache.hadoop.security.token.
					//     SecretManager$InvalidToken: StandbyException
					//
					HttpServletResponse response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
					ExceptionHandler eh = new ExceptionHandler();
					eh.InitResponse(response);
					// The Response (resp) below is what the server will send to client          
					//
					// BEFORE HDFS-6475 fix, the resp.entity is
					//     {"RemoteException":{"exception":"SecurityException",
					//      "javaClassName":"java.lang.SecurityException",
					//      "message":"Failed to obtain user group information: 
					//      org.apache.hadoop.security.token.SecretManager$InvalidToken:
					//        StandbyException"}}
					// AFTER the fix, the resp.entity is
					//     {"RemoteException":{"exception":"StandbyException",
					//      "javaClassName":"org.apache.hadoop.ipc.StandbyException",
					//      "message":"Operation category READ is not supported in
					//       state standby"}}
					//
					Response resp = eh.ToResponse(oe);
					// Mimic the client side logic by parsing the response from server
					//
					IDictionary<object, object> m = (IDictionary<object, object>)JSON.Parse(resp.GetEntity
						().ToString());
					RemoteException re = JsonUtil.ToRemoteException(m);
					Exception unwrapped = ((RemoteException)re).UnwrapRemoteException(typeof(StandbyException
						));
					NUnit.Framework.Assert.IsTrue(unwrapped is StandbyException);
					return null;
				}
			}

			private readonly DelegationTokenIdentifier identifier;
		}

		/// <exception cref="System.IO.IOException"/>
		private Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> GetDelegationToken
			(FileSystem fs, string renewer)
		{
			Org.Apache.Hadoop.Security.Token.Token<object>[] tokens = fs.AddDelegationTokens(
				renewer, null);
			NUnit.Framework.Assert.AreEqual(1, tokens.Length);
			return (Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier>)tokens[
				0];
		}

		internal enum TokenTestAction
		{
			Renew,
			Cancel
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private static void DoRenewOrCancel(Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
			> token, Configuration conf, TestDelegationTokensWithHA.TokenTestAction action)
		{
			UserGroupInformation.CreateRemoteUser("JobTracker").DoAs(new _PrivilegedExceptionAction_477
				(action, token, conf));
		}

		private sealed class _PrivilegedExceptionAction_477 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_477(TestDelegationTokensWithHA.TokenTestAction 
				action, Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token, 
				Configuration conf)
			{
				this.action = action;
				this.token = token;
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				switch (action)
				{
					case TestDelegationTokensWithHA.TokenTestAction.Renew:
					{
						token.Renew(conf);
						break;
					}

					case TestDelegationTokensWithHA.TokenTestAction.Cancel:
					{
						token.Cancel(conf);
						break;
					}

					default:
					{
						NUnit.Framework.Assert.Fail("bad action:" + action);
						break;
					}
				}
				return null;
			}

			private readonly TestDelegationTokensWithHA.TokenTestAction action;

			private readonly Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> token;

			private readonly Configuration conf;
		}
	}
}
