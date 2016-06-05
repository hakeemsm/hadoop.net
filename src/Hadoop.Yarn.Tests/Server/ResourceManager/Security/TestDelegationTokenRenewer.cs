using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Com.Google.Common.Base;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security
{
	/// <summary>
	/// unit test -
	/// tests addition/deletion/cancellation of renewals of delegation tokens
	/// </summary>
	public class TestDelegationTokenRenewer
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestDelegationTokenRenewer
			));

		private static readonly Text Kind = new Text("HDFS_DELEGATION_TOKEN");

		private static BlockingQueue<Org.Apache.Hadoop.Yarn.Event.Event> eventQueue;

		private static volatile AtomicInteger counter;

		private static AsyncDispatcher dispatcher;

		public class Renewer : TokenRenewer
		{
			private static int counter = 0;

			private static Org.Apache.Hadoop.Security.Token.Token<object> lastRenewed = null;

			private static Org.Apache.Hadoop.Security.Token.Token<object> tokenToRenewIn2Sec = 
				null;

			private static bool cancelled = false;

			private static void Reset()
			{
				counter = 0;
				lastRenewed = null;
				tokenToRenewIn2Sec = null;
				cancelled = false;
			}

			public override bool HandleKind(Text kind)
			{
				return Kind.Equals(kind);
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool IsManaged<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0> token
				)
			{
				return true;
			}

			/// <exception cref="System.IO.IOException"/>
			public override long Renew<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0> t, Configuration
				 conf)
			{
				if (!(t is TestDelegationTokenRenewer.MyToken))
				{
					// renew in 3 seconds
					return Runtime.CurrentTimeMillis() + 3000;
				}
				TestDelegationTokenRenewer.MyToken token = (TestDelegationTokenRenewer.MyToken)t;
				if (token.IsCanceled())
				{
					throw new SecretManager.InvalidToken("token has been canceled");
				}
				lastRenewed = token;
				counter++;
				Log.Info("Called MYDFS.renewdelegationtoken " + token + ";this dfs=" + this.GetHashCode
					() + ";c=" + counter);
				if (tokenToRenewIn2Sec == token)
				{
					// this token first renewal in 2 seconds
					Log.Info("RENEW in 2 seconds");
					tokenToRenewIn2Sec = null;
					return 2 * 1000 + Runtime.CurrentTimeMillis();
				}
				else
				{
					return 86400 * 1000 + Runtime.CurrentTimeMillis();
				}
			}

			public override void Cancel<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0> t, Configuration
				 conf)
			{
				cancelled = true;
				if (t is TestDelegationTokenRenewer.MyToken)
				{
					TestDelegationTokenRenewer.MyToken token = (TestDelegationTokenRenewer.MyToken)t;
					Log.Info("Cancel token " + token);
					token.CancelToken();
				}
			}
		}

		private static Configuration conf;

		internal DelegationTokenRenewer delegationTokenRenewer;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetUpClass()
		{
			conf = new Configuration();
			// create a fake FileSystem (MyFS) and assosiate it
			// with "hdfs" schema.
			URI uri = new URI(DelegationTokenRenewer.Scheme + "://localhost:0");
			System.Console.Out.WriteLine("scheme is : " + uri.GetScheme());
			conf.SetClass("fs." + uri.GetScheme() + ".impl", typeof(TestDelegationTokenRenewer.MyFS
				), typeof(DistributedFileSystem));
			FileSystem.SetDefaultUri(conf, uri);
			Log.Info("filesystem uri = " + FileSystem.GetDefaultUri(conf).ToString());
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void SetUp()
		{
			counter = new AtomicInteger(0);
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "kerberos");
			UserGroupInformation.SetConfiguration(conf);
			eventQueue = new LinkedBlockingQueue<Org.Apache.Hadoop.Yarn.Event.Event>();
			dispatcher = new AsyncDispatcher(eventQueue);
			TestDelegationTokenRenewer.Renewer.Reset();
			delegationTokenRenewer = CreateNewDelegationTokenRenewer(conf, counter);
			RMContext mockContext = Org.Mockito.Mockito.Mock<RMContext>();
			ClientRMService mockClientRMService = Org.Mockito.Mockito.Mock<ClientRMService>();
			Org.Mockito.Mockito.When(mockContext.GetSystemCredentialsForApps()).ThenReturn(new 
				ConcurrentHashMap<ApplicationId, ByteBuffer>());
			Org.Mockito.Mockito.When(mockContext.GetDelegationTokenRenewer()).ThenReturn(delegationTokenRenewer
				);
			Org.Mockito.Mockito.When(mockContext.GetDispatcher()).ThenReturn(dispatcher);
			Org.Mockito.Mockito.When(mockContext.GetClientRMService()).ThenReturn(mockClientRMService
				);
			IPEndPoint sockAddr = IPEndPoint.CreateUnresolved("localhost", 1234);
			Org.Mockito.Mockito.When(mockClientRMService.GetBindAddress()).ThenReturn(sockAddr
				);
			delegationTokenRenewer.SetRMContext(mockContext);
			delegationTokenRenewer.Init(conf);
			delegationTokenRenewer.Start();
		}

		[TearDown]
		public virtual void TearDown()
		{
			delegationTokenRenewer.Stop();
		}

		private class MyDelegationTokenSecretManager : DelegationTokenSecretManager
		{
			public MyDelegationTokenSecretManager(long delegationKeyUpdateInterval, long delegationTokenMaxLifetime
				, long delegationTokenRenewInterval, long delegationTokenRemoverScanInterval, FSNamesystem
				 namesystem)
				: base(delegationKeyUpdateInterval, delegationTokenMaxLifetime, delegationTokenRenewInterval
					, delegationTokenRemoverScanInterval, namesystem)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			protected override void LogUpdateMasterKey(DelegationKey key)
			{
				//DelegationTokenSecretManager
				return;
			}
		}

		/// <summary>
		/// add some extra functionality for testing
		/// 1.
		/// </summary>
		/// <remarks>
		/// add some extra functionality for testing
		/// 1. toString();
		/// 2. cancel() and isCanceled()
		/// </remarks>
		private class MyToken : Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
			>
		{
			public string status = "GOOD";

			public const string Canceled = "CANCELED";

			public MyToken(DelegationTokenIdentifier dtId1, TestDelegationTokenRenewer.MyDelegationTokenSecretManager
				 sm)
				: base(dtId1, sm)
			{
				SetKind(Kind);
				status = "GOOD";
			}

			public virtual bool IsCanceled()
			{
				return status.Equals(Canceled);
			}

			public virtual void CancelToken()
			{
				this.status = Canceled;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override long Renew(Configuration conf)
			{
				return base.Renew(conf);
			}

			public override string ToString()
			{
				StringBuilder sb = new StringBuilder(1024);
				sb.Append("id=");
				string id = StringUtils.ByteToHexString(this.GetIdentifier());
				int idLen = id.Length;
				sb.Append(Sharpen.Runtime.Substring(id, idLen - 6));
				sb.Append(";k=");
				sb.Append(this.GetKind());
				sb.Append(";s=");
				sb.Append(this.GetService());
				return sb.ToString();
			}
		}

		/// <summary>
		/// fake FileSystem
		/// overwrites three methods
		/// 1.
		/// </summary>
		/// <remarks>
		/// fake FileSystem
		/// overwrites three methods
		/// 1. getDelegationToken() - generates a token
		/// 2. renewDelegataionToken - counts number of calls, and remembers
		/// most recently renewed token.
		/// 3. cancelToken -cancels token (subsequent renew will cause IllegalToken
		/// exception
		/// </remarks>
		internal class MyFS : DistributedFileSystem
		{
			private static AtomicInteger instanceCounter = new AtomicInteger();

			public MyFS()
			{
				instanceCounter.IncrementAndGet();
			}

			public override void Close()
			{
				instanceCounter.DecrementAndGet();
			}

			public static int GetInstanceCounter()
			{
				return instanceCounter.Get();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Initialize(URI uri, Configuration conf)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override Org.Apache.Hadoop.Security.Token.Token<object> GetDelegationToken
				(string renewer)
			{
				TestDelegationTokenRenewer.MyToken result = CreateTokens(new Org.Apache.Hadoop.IO.Text
					(renewer));
				Log.Info("Called MYDFS.getdelegationtoken " + result);
				return result;
			}

			/// <exception cref="System.IO.IOException"/>
			public override Org.Apache.Hadoop.Security.Token.Token<object>[] AddDelegationTokens
				(string renewer, Credentials credentials)
			{
				return new Org.Apache.Hadoop.Security.Token.Token<object>[0];
			}
		}

		/// <summary>Auxiliary - create token</summary>
		/// <param name="renewer"/>
		/// <returns/>
		/// <exception cref="System.IO.IOException"/>
		internal static TestDelegationTokenRenewer.MyToken CreateTokens(Org.Apache.Hadoop.IO.Text
			 renewer)
		{
			Org.Apache.Hadoop.IO.Text user1 = new Org.Apache.Hadoop.IO.Text("user1");
			TestDelegationTokenRenewer.MyDelegationTokenSecretManager sm = new TestDelegationTokenRenewer.MyDelegationTokenSecretManager
				(DFSConfigKeys.DfsNamenodeDelegationKeyUpdateIntervalDefault, DFSConfigKeys.DfsNamenodeDelegationTokenMaxLifetimeDefault
				, DFSConfigKeys.DfsNamenodeDelegationTokenRenewIntervalDefault, 3600000, null);
			sm.StartThreads();
			DelegationTokenIdentifier dtId1 = new DelegationTokenIdentifier(user1, renewer, user1
				);
			TestDelegationTokenRenewer.MyToken token1 = new TestDelegationTokenRenewer.MyToken
				(dtId1, sm);
			token1.SetService(new Org.Apache.Hadoop.IO.Text("localhost:0"));
			return token1;
		}

		/// <summary>
		/// Basic idea of the test:
		/// 1.
		/// </summary>
		/// <remarks>
		/// Basic idea of the test:
		/// 1. create tokens.
		/// 2. Mark one of them to be renewed in 2 seconds (instead of
		/// 24 hours)
		/// 3. register them for renewal
		/// 4. sleep for 3 seconds
		/// 5. count number of renewals (should 3 initial ones + one extra)
		/// 6. register another token for 2 seconds
		/// 7. cancel it immediately
		/// 8. Sleep and check that the 2 seconds renew didn't happen
		/// (totally 5 renewals)
		/// 9. check cancellation
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestDTRenewal()
		{
			TestDelegationTokenRenewer.MyFS dfs = (TestDelegationTokenRenewer.MyFS)FileSystem
				.Get(conf);
			Log.Info("dfs=" + (object)dfs.GetHashCode() + ";conf=" + conf.GetHashCode());
			// Test 1. - add three tokens - make sure exactly one get's renewed
			// get the delegation tokens
			TestDelegationTokenRenewer.MyToken token1;
			TestDelegationTokenRenewer.MyToken token2;
			TestDelegationTokenRenewer.MyToken token3;
			token1 = ((TestDelegationTokenRenewer.MyToken)dfs.GetDelegationToken("user1"));
			token2 = ((TestDelegationTokenRenewer.MyToken)dfs.GetDelegationToken("user2"));
			token3 = ((TestDelegationTokenRenewer.MyToken)dfs.GetDelegationToken("user3"));
			//to cause this one to be set for renew in 2 secs
			TestDelegationTokenRenewer.Renewer.tokenToRenewIn2Sec = token1;
			Log.Info("token=" + token1 + " should be renewed for 2 secs");
			// three distinct Namenodes
			string nn1 = DelegationTokenRenewer.Scheme + "://host1:0";
			string nn2 = DelegationTokenRenewer.Scheme + "://host2:0";
			string nn3 = DelegationTokenRenewer.Scheme + "://host3:0";
			Credentials ts = new Credentials();
			// register the token for renewal
			ts.AddToken(new Org.Apache.Hadoop.IO.Text(nn1), token1);
			ts.AddToken(new Org.Apache.Hadoop.IO.Text(nn2), token2);
			ts.AddToken(new Org.Apache.Hadoop.IO.Text(nn3), token3);
			// register the tokens for renewal
			ApplicationId applicationId_0 = BuilderUtils.NewApplicationId(0, 0);
			delegationTokenRenewer.AddApplicationAsync(applicationId_0, ts, true, "user");
			WaitForEventsToGetProcessed(delegationTokenRenewer);
			// first 3 initial renewals + 1 real
			int numberOfExpectedRenewals = 3 + 1;
			int attempts = 10;
			while (attempts-- > 0)
			{
				try
				{
					Sharpen.Thread.Sleep(3 * 1000);
				}
				catch (Exception)
				{
				}
				// sleep 3 seconds, so it has time to renew
				// since we cannot guarantee timely execution - let's give few chances
				if (TestDelegationTokenRenewer.Renewer.counter == numberOfExpectedRenewals)
				{
					break;
				}
			}
			Log.Info("dfs=" + dfs.GetHashCode() + ";Counter = " + TestDelegationTokenRenewer.Renewer
				.counter + ";t=" + TestDelegationTokenRenewer.Renewer.lastRenewed);
			NUnit.Framework.Assert.AreEqual("renew wasn't called as many times as expected(4):"
				, numberOfExpectedRenewals, TestDelegationTokenRenewer.Renewer.counter);
			NUnit.Framework.Assert.AreEqual("most recently renewed token mismatch", TestDelegationTokenRenewer.Renewer
				.lastRenewed, token1);
			// Test 2. 
			// add another token ( that expires in 2 secs). Then remove it, before
			// time is up.
			// Wait for 3 secs , and make sure no renews were called
			ts = new Credentials();
			TestDelegationTokenRenewer.MyToken token4 = ((TestDelegationTokenRenewer.MyToken)
				dfs.GetDelegationToken("user4"));
			//to cause this one to be set for renew in 2 secs
			TestDelegationTokenRenewer.Renewer.tokenToRenewIn2Sec = token4;
			Log.Info("token=" + token4 + " should be renewed for 2 secs");
			string nn4 = DelegationTokenRenewer.Scheme + "://host4:0";
			ts.AddToken(new Org.Apache.Hadoop.IO.Text(nn4), token4);
			ApplicationId applicationId_1 = BuilderUtils.NewApplicationId(0, 1);
			delegationTokenRenewer.AddApplicationAsync(applicationId_1, ts, true, "user");
			WaitForEventsToGetProcessed(delegationTokenRenewer);
			delegationTokenRenewer.ApplicationFinished(applicationId_1);
			WaitForEventsToGetProcessed(delegationTokenRenewer);
			numberOfExpectedRenewals = TestDelegationTokenRenewer.Renewer.counter;
			// number of renewals so far
			try
			{
				Sharpen.Thread.Sleep(6 * 1000);
			}
			catch (Exception)
			{
			}
			// sleep 6 seconds, so it has time to renew
			Log.Info("Counter = " + TestDelegationTokenRenewer.Renewer.counter + ";t=" + TestDelegationTokenRenewer.Renewer
				.lastRenewed);
			// counter and the token should stil be the old ones
			NUnit.Framework.Assert.AreEqual("renew wasn't called as many times as expected", 
				numberOfExpectedRenewals, TestDelegationTokenRenewer.Renewer.counter);
			// also renewing of the cancelled token should fail
			try
			{
				token4.Renew(conf);
				NUnit.Framework.Assert.Fail("Renewal of cancelled token should have failed");
			}
			catch (SecretManager.InvalidToken)
			{
			}
		}

		//expected
		/// <exception cref="System.Exception"/>
		public virtual void TestAppRejectionWithCancelledDelegationToken()
		{
			TestDelegationTokenRenewer.MyFS dfs = (TestDelegationTokenRenewer.MyFS)FileSystem
				.Get(conf);
			Log.Info("dfs=" + (object)dfs.GetHashCode() + ";conf=" + conf.GetHashCode());
			TestDelegationTokenRenewer.MyToken token = ((TestDelegationTokenRenewer.MyToken)dfs
				.GetDelegationToken("user1"));
			token.CancelToken();
			Credentials ts = new Credentials();
			ts.AddToken(token.GetKind(), token);
			// register the tokens for renewal
			ApplicationId appId = BuilderUtils.NewApplicationId(0, 0);
			delegationTokenRenewer.AddApplicationAsync(appId, ts, true, "user");
			int waitCnt = 20;
			while (waitCnt-- > 0)
			{
				if (!eventQueue.IsEmpty())
				{
					Org.Apache.Hadoop.Yarn.Event.Event evt = eventQueue.Take();
					if (evt.GetType() == RMAppEventType.AppRejected)
					{
						NUnit.Framework.Assert.IsTrue(((RMAppEvent)evt).GetApplicationId().Equals(appId));
						return;
					}
				}
				else
				{
					Sharpen.Thread.Sleep(500);
				}
			}
			NUnit.Framework.Assert.Fail("App submission with a cancelled token should have failed"
				);
		}

		/// <summary>
		/// Basic idea of the test:
		/// 1.
		/// </summary>
		/// <remarks>
		/// Basic idea of the test:
		/// 1. register a token for 2 seconds with no cancel at the end
		/// 2. cancel it immediately
		/// 3. Sleep and check that the 2 seconds renew didn't happen
		/// (totally 5 renewals)
		/// 4. check cancellation
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestDTRenewalWithNoCancel()
		{
			TestDelegationTokenRenewer.MyFS dfs = (TestDelegationTokenRenewer.MyFS)FileSystem
				.Get(conf);
			Log.Info("dfs=" + (object)dfs.GetHashCode() + ";conf=" + conf.GetHashCode());
			Credentials ts = new Credentials();
			TestDelegationTokenRenewer.MyToken token1 = ((TestDelegationTokenRenewer.MyToken)
				dfs.GetDelegationToken("user1"));
			//to cause this one to be set for renew in 2 secs
			TestDelegationTokenRenewer.Renewer.tokenToRenewIn2Sec = token1;
			Log.Info("token=" + token1 + " should be renewed for 2 secs");
			string nn1 = DelegationTokenRenewer.Scheme + "://host1:0";
			ts.AddToken(new Org.Apache.Hadoop.IO.Text(nn1), token1);
			ApplicationId applicationId_1 = BuilderUtils.NewApplicationId(0, 1);
			delegationTokenRenewer.AddApplicationAsync(applicationId_1, ts, false, "user");
			WaitForEventsToGetProcessed(delegationTokenRenewer);
			delegationTokenRenewer.ApplicationFinished(applicationId_1);
			WaitForEventsToGetProcessed(delegationTokenRenewer);
			int numberOfExpectedRenewals = TestDelegationTokenRenewer.Renewer.counter;
			// number of renewals so far
			try
			{
				Sharpen.Thread.Sleep(6 * 1000);
			}
			catch (Exception)
			{
			}
			// sleep 6 seconds, so it has time to renew
			Log.Info("Counter = " + TestDelegationTokenRenewer.Renewer.counter + ";t=" + TestDelegationTokenRenewer.Renewer
				.lastRenewed);
			// counter and the token should still be the old ones
			NUnit.Framework.Assert.AreEqual("renew wasn't called as many times as expected", 
				numberOfExpectedRenewals, TestDelegationTokenRenewer.Renewer.counter);
			// also renewing of the canceled token should not fail, because it has not
			// been canceled
			token1.Renew(conf);
		}

		/// <summary>
		/// Basic idea of the test:
		/// 0.
		/// </summary>
		/// <remarks>
		/// Basic idea of the test:
		/// 0. Setup token KEEP_ALIVE
		/// 1. create tokens.
		/// 2. register them for renewal - to be cancelled on app complete
		/// 3. Complete app.
		/// 4. Verify token is alive within the KEEP_ALIVE time
		/// 5. Verify token has been cancelled after the KEEP_ALIVE_TIME
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestDTKeepAlive1()
		{
			Configuration lconf = new Configuration(conf);
			lconf.SetBoolean(YarnConfiguration.LogAggregationEnabled, true);
			//Keep tokens alive for 6 seconds.
			lconf.SetLong(YarnConfiguration.RmNmExpiryIntervalMs, 6000l);
			//Try removing tokens every second.
			lconf.SetLong(YarnConfiguration.RmDelayedDelegationTokenRemovalIntervalMs, 1000l);
			DelegationTokenRenewer localDtr = CreateNewDelegationTokenRenewer(lconf, counter);
			RMContext mockContext = Org.Mockito.Mockito.Mock<RMContext>();
			Org.Mockito.Mockito.When(mockContext.GetSystemCredentialsForApps()).ThenReturn(new 
				ConcurrentHashMap<ApplicationId, ByteBuffer>());
			ClientRMService mockClientRMService = Org.Mockito.Mockito.Mock<ClientRMService>();
			Org.Mockito.Mockito.When(mockContext.GetClientRMService()).ThenReturn(mockClientRMService
				);
			Org.Mockito.Mockito.When(mockContext.GetDelegationTokenRenewer()).ThenReturn(localDtr
				);
			Org.Mockito.Mockito.When(mockContext.GetDispatcher()).ThenReturn(dispatcher);
			IPEndPoint sockAddr = IPEndPoint.CreateUnresolved("localhost", 1234);
			Org.Mockito.Mockito.When(mockClientRMService.GetBindAddress()).ThenReturn(sockAddr
				);
			localDtr.SetRMContext(mockContext);
			localDtr.Init(lconf);
			localDtr.Start();
			TestDelegationTokenRenewer.MyFS dfs = (TestDelegationTokenRenewer.MyFS)FileSystem
				.Get(lconf);
			Log.Info("dfs=" + (object)dfs.GetHashCode() + ";conf=" + lconf.GetHashCode());
			Credentials ts = new Credentials();
			// get the delegation tokens
			TestDelegationTokenRenewer.MyToken token1 = ((TestDelegationTokenRenewer.MyToken)
				dfs.GetDelegationToken("user1"));
			string nn1 = DelegationTokenRenewer.Scheme + "://host1:0";
			ts.AddToken(new Org.Apache.Hadoop.IO.Text(nn1), token1);
			// register the tokens for renewal
			ApplicationId applicationId_0 = BuilderUtils.NewApplicationId(0, 0);
			localDtr.AddApplicationAsync(applicationId_0, ts, true, "user");
			WaitForEventsToGetProcessed(localDtr);
			if (!eventQueue.IsEmpty())
			{
				Org.Apache.Hadoop.Yarn.Event.Event evt = eventQueue.Take();
				if (evt is RMAppEvent)
				{
					NUnit.Framework.Assert.AreEqual(((RMAppEvent)evt).GetType(), RMAppEventType.Start
						);
				}
				else
				{
					NUnit.Framework.Assert.Fail("RMAppEvent.START was expected!!");
				}
			}
			localDtr.ApplicationFinished(applicationId_0);
			WaitForEventsToGetProcessed(localDtr);
			//Token should still be around. Renewal should not fail.
			token1.Renew(lconf);
			//Allow the keepalive time to run out
			Sharpen.Thread.Sleep(10000l);
			//The token should have been cancelled at this point. Renewal will fail.
			try
			{
				token1.Renew(lconf);
				NUnit.Framework.Assert.Fail("Renewal of cancelled token should have failed");
			}
			catch (SecretManager.InvalidToken)
			{
			}
		}

		/// <summary>
		/// Basic idea of the test:
		/// 0.
		/// </summary>
		/// <remarks>
		/// Basic idea of the test:
		/// 0. Setup token KEEP_ALIVE
		/// 1. create tokens.
		/// 2. register them for renewal - to be cancelled on app complete
		/// 3. Complete app.
		/// 4. Verify token is alive within the KEEP_ALIVE time
		/// 5. Send an explicity KEEP_ALIVE_REQUEST
		/// 6. Verify token KEEP_ALIVE time is renewed.
		/// 7. Verify token has been cancelled after the renewed KEEP_ALIVE_TIME.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestDTKeepAlive2()
		{
			Configuration lconf = new Configuration(conf);
			lconf.SetBoolean(YarnConfiguration.LogAggregationEnabled, true);
			//Keep tokens alive for 6 seconds.
			lconf.SetLong(YarnConfiguration.RmNmExpiryIntervalMs, 6000l);
			//Try removing tokens every second.
			lconf.SetLong(YarnConfiguration.RmDelayedDelegationTokenRemovalIntervalMs, 1000l);
			DelegationTokenRenewer localDtr = CreateNewDelegationTokenRenewer(conf, counter);
			RMContext mockContext = Org.Mockito.Mockito.Mock<RMContext>();
			Org.Mockito.Mockito.When(mockContext.GetSystemCredentialsForApps()).ThenReturn(new 
				ConcurrentHashMap<ApplicationId, ByteBuffer>());
			ClientRMService mockClientRMService = Org.Mockito.Mockito.Mock<ClientRMService>();
			Org.Mockito.Mockito.When(mockContext.GetClientRMService()).ThenReturn(mockClientRMService
				);
			Org.Mockito.Mockito.When(mockContext.GetDelegationTokenRenewer()).ThenReturn(localDtr
				);
			Org.Mockito.Mockito.When(mockContext.GetDispatcher()).ThenReturn(dispatcher);
			IPEndPoint sockAddr = IPEndPoint.CreateUnresolved("localhost", 1234);
			Org.Mockito.Mockito.When(mockClientRMService.GetBindAddress()).ThenReturn(sockAddr
				);
			localDtr.SetRMContext(mockContext);
			localDtr.Init(lconf);
			localDtr.Start();
			TestDelegationTokenRenewer.MyFS dfs = (TestDelegationTokenRenewer.MyFS)FileSystem
				.Get(lconf);
			Log.Info("dfs=" + (object)dfs.GetHashCode() + ";conf=" + lconf.GetHashCode());
			Credentials ts = new Credentials();
			// get the delegation tokens
			TestDelegationTokenRenewer.MyToken token1 = ((TestDelegationTokenRenewer.MyToken)
				dfs.GetDelegationToken("user1"));
			string nn1 = DelegationTokenRenewer.Scheme + "://host1:0";
			ts.AddToken(new Org.Apache.Hadoop.IO.Text(nn1), token1);
			// register the tokens for renewal
			ApplicationId applicationId_0 = BuilderUtils.NewApplicationId(0, 0);
			localDtr.AddApplicationAsync(applicationId_0, ts, true, "user");
			localDtr.ApplicationFinished(applicationId_0);
			WaitForEventsToGetProcessed(delegationTokenRenewer);
			//Send another keep alive.
			localDtr.UpdateKeepAliveApplications(Collections.SingletonList(applicationId_0));
			//Renewal should not fail.
			token1.Renew(lconf);
			//Token should be around after this. 
			Sharpen.Thread.Sleep(4500l);
			//Renewal should not fail. - ~1.5 seconds for keepalive timeout.
			token1.Renew(lconf);
			//Allow the keepalive time to run out
			Sharpen.Thread.Sleep(3000l);
			//The token should have been cancelled at this point. Renewal will fail.
			try
			{
				token1.Renew(lconf);
				NUnit.Framework.Assert.Fail("Renewal of cancelled token should have failed");
			}
			catch (SecretManager.InvalidToken)
			{
			}
		}

		private DelegationTokenRenewer CreateNewDelegationTokenRenewer(Configuration conf
			, AtomicInteger counter)
		{
			DelegationTokenRenewer renew = new _DelegationTokenRenewer_682(counter);
			renew.SetRMContext(TestUtils.GetMockRMContext());
			return renew;
		}

		private sealed class _DelegationTokenRenewer_682 : DelegationTokenRenewer
		{
			public _DelegationTokenRenewer_682(AtomicInteger counter)
			{
				this.counter = counter;
			}

			protected internal override ThreadPoolExecutor CreateNewThreadPoolService(Configuration
				 conf)
			{
				ThreadPoolExecutor pool = new _ThreadPoolExecutor_689(counter, 5, 5, 3L, TimeUnit
					.Seconds, new LinkedBlockingQueue<Runnable>());
				return pool;
			}

			private sealed class _ThreadPoolExecutor_689 : ThreadPoolExecutor
			{
				public _ThreadPoolExecutor_689(AtomicInteger counter, int baseArg1, int baseArg2, 
					long baseArg3, TimeUnit baseArg4, BlockingQueue<Runnable> baseArg5)
					: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5)
				{
					this.counter = counter;
				}

				protected override void AfterExecute(Runnable r, Exception t)
				{
					counter.DecrementAndGet();
					base.AfterExecute(r, t);
				}

				public override void Execute(Runnable command)
				{
					counter.IncrementAndGet();
					base.Execute(command);
				}

				private readonly AtomicInteger counter;
			}

			private readonly AtomicInteger counter;
		}

		/// <exception cref="System.Exception"/>
		private void WaitForEventsToGetProcessed(DelegationTokenRenewer dtr)
		{
			int wait = 40;
			while (wait-- > 0 && counter.Get() > 0)
			{
				Sharpen.Thread.Sleep(200);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Sharpen.BrokenBarrierException"/>
		public virtual void TestDTRonAppSubmission()
		{
			Credentials credsx = new Credentials();
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> tokenx = Org.Mockito.Mockito.Mock
				<Org.Apache.Hadoop.Security.Token.Token>();
			Org.Mockito.Mockito.When(tokenx.GetKind()).ThenReturn(new Org.Apache.Hadoop.IO.Text
				("HDFS_DELEGATION_TOKEN"));
			DelegationTokenIdentifier dtId1 = new DelegationTokenIdentifier(new Org.Apache.Hadoop.IO.Text
				("user1"), new Org.Apache.Hadoop.IO.Text("renewer"), new Org.Apache.Hadoop.IO.Text
				("user1"));
			Org.Mockito.Mockito.When(tokenx.DecodeIdentifier()).ThenReturn(dtId1);
			credsx.AddToken(new Org.Apache.Hadoop.IO.Text("token"), tokenx);
			Org.Mockito.Mockito.DoReturn(true).When(tokenx).IsManaged();
			Org.Mockito.Mockito.DoThrow(new IOException("boom")).When(tokenx).Renew(Matchers.Any
				<Configuration>());
			// fire up the renewer
			DelegationTokenRenewer dtr = CreateNewDelegationTokenRenewer(conf, counter);
			RMContext mockContext = Org.Mockito.Mockito.Mock<RMContext>();
			Org.Mockito.Mockito.When(mockContext.GetSystemCredentialsForApps()).ThenReturn(new 
				ConcurrentHashMap<ApplicationId, ByteBuffer>());
			ClientRMService mockClientRMService = Org.Mockito.Mockito.Mock<ClientRMService>();
			Org.Mockito.Mockito.When(mockContext.GetClientRMService()).ThenReturn(mockClientRMService
				);
			IPEndPoint sockAddr = IPEndPoint.CreateUnresolved("localhost", 1234);
			Org.Mockito.Mockito.When(mockClientRMService.GetBindAddress()).ThenReturn(sockAddr
				);
			dtr.SetRMContext(mockContext);
			Org.Mockito.Mockito.When(mockContext.GetDelegationTokenRenewer()).ThenReturn(dtr);
			dtr.Init(conf);
			dtr.Start();
			try
			{
				dtr.AddApplicationSync(Org.Mockito.Mockito.Mock<ApplicationId>(), credsx, false, 
					"user");
				NUnit.Framework.Assert.Fail("Catch IOException on app submission");
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains(tokenx.ToString()));
				NUnit.Framework.Assert.IsTrue(e.InnerException.ToString().Contains("boom"));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Sharpen.BrokenBarrierException"/>
		public virtual void TestConcurrentAddApplication()
		{
			CyclicBarrier startBarrier = new CyclicBarrier(2);
			CyclicBarrier endBarrier = new CyclicBarrier(2);
			// this token uses barriers to block during renew                          
			Credentials creds1 = new Credentials();
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token1 = Org.Mockito.Mockito.Mock
				<Org.Apache.Hadoop.Security.Token.Token>();
			Org.Mockito.Mockito.When(token1.GetKind()).ThenReturn(new Org.Apache.Hadoop.IO.Text
				("HDFS_DELEGATION_TOKEN"));
			DelegationTokenIdentifier dtId1 = new DelegationTokenIdentifier(new Org.Apache.Hadoop.IO.Text
				("user1"), new Org.Apache.Hadoop.IO.Text("renewer"), new Org.Apache.Hadoop.IO.Text
				("user1"));
			Org.Mockito.Mockito.When(token1.DecodeIdentifier()).ThenReturn(dtId1);
			creds1.AddToken(new Org.Apache.Hadoop.IO.Text("token"), token1);
			Org.Mockito.Mockito.DoReturn(true).When(token1).IsManaged();
			Org.Mockito.Mockito.DoAnswer(new _Answer_775(startBarrier, endBarrier)).When(token1
				).Renew(Matchers.Any<Configuration>());
			// this dummy token fakes renewing                                         
			Credentials creds2 = new Credentials();
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token2 = Org.Mockito.Mockito.Mock
				<Org.Apache.Hadoop.Security.Token.Token>();
			Org.Mockito.Mockito.When(token2.GetKind()).ThenReturn(new Org.Apache.Hadoop.IO.Text
				("HDFS_DELEGATION_TOKEN"));
			Org.Mockito.Mockito.When(token2.DecodeIdentifier()).ThenReturn(dtId1);
			creds2.AddToken(new Org.Apache.Hadoop.IO.Text("token"), token2);
			Org.Mockito.Mockito.DoReturn(true).When(token2).IsManaged();
			Org.Mockito.Mockito.DoReturn(long.MaxValue).When(token2).Renew(Matchers.Any<Configuration
				>());
			// fire up the renewer                                                     
			DelegationTokenRenewer dtr = CreateNewDelegationTokenRenewer(conf, counter);
			RMContext mockContext = Org.Mockito.Mockito.Mock<RMContext>();
			Org.Mockito.Mockito.When(mockContext.GetSystemCredentialsForApps()).ThenReturn(new 
				ConcurrentHashMap<ApplicationId, ByteBuffer>());
			ClientRMService mockClientRMService = Org.Mockito.Mockito.Mock<ClientRMService>();
			Org.Mockito.Mockito.When(mockContext.GetClientRMService()).ThenReturn(mockClientRMService
				);
			IPEndPoint sockAddr = IPEndPoint.CreateUnresolved("localhost", 1234);
			Org.Mockito.Mockito.When(mockClientRMService.GetBindAddress()).ThenReturn(sockAddr
				);
			dtr.SetRMContext(mockContext);
			Org.Mockito.Mockito.When(mockContext.GetDelegationTokenRenewer()).ThenReturn(dtr);
			dtr.Init(conf);
			dtr.Start();
			// submit a job that blocks during renewal                                 
			Sharpen.Thread submitThread = new _Thread_808(dtr, creds1);
			submitThread.Start();
			// wait till 1st submit blocks, then submit another
			startBarrier.Await();
			dtr.AddApplicationAsync(Org.Mockito.Mockito.Mock<ApplicationId>(), creds2, false, 
				"user");
			// signal 1st to complete                                                  
			endBarrier.Await();
			submitThread.Join();
		}

		private sealed class _Answer_775 : Answer<long>
		{
			public _Answer_775(CyclicBarrier startBarrier, CyclicBarrier endBarrier)
			{
				this.startBarrier = startBarrier;
				this.endBarrier = endBarrier;
			}

			/// <exception cref="System.Exception"/>
			/// <exception cref="Sharpen.BrokenBarrierException"/>
			public long Answer(InvocationOnMock invocation)
			{
				startBarrier.Await();
				endBarrier.Await();
				return long.MaxValue;
			}

			private readonly CyclicBarrier startBarrier;

			private readonly CyclicBarrier endBarrier;
		}

		private sealed class _Thread_808 : Sharpen.Thread
		{
			public _Thread_808(DelegationTokenRenewer dtr, Credentials creds1)
			{
				this.dtr = dtr;
				this.creds1 = creds1;
			}

			public override void Run()
			{
				dtr.AddApplicationAsync(Org.Mockito.Mockito.Mock<ApplicationId>(), creds1, false, 
					"user");
			}

			private readonly DelegationTokenRenewer dtr;

			private readonly Credentials creds1;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAppSubmissionWithInvalidDelegationToken()
		{
			Configuration conf = new Configuration();
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "kerberos");
			UserGroupInformation.SetConfiguration(conf);
			MockRM rm = new _MockRM_831(conf);
			// Skip the login.
			ByteBuffer tokens = ByteBuffer.Wrap(Sharpen.Runtime.GetBytesForString("BOGUS"));
			ContainerLaunchContext amContainer = ContainerLaunchContext.NewInstance(new Dictionary
				<string, LocalResource>(), new Dictionary<string, string>(), new AList<string>()
				, new Dictionary<string, ByteBuffer>(), tokens, new Dictionary<ApplicationAccessType
				, string>());
			ApplicationSubmissionContext appSubContext = ApplicationSubmissionContext.NewInstance
				(ApplicationId.NewInstance(1234121, 0), "BOGUS", "default", Priority.Undefined, 
				amContainer, false, true, 1, Resource.NewInstance(1024, 1), "BOGUS");
			SubmitApplicationRequest request = SubmitApplicationRequest.NewInstance(appSubContext
				);
			try
			{
				rm.GetClientRMService().SubmitApplication(request);
				NUnit.Framework.Assert.Fail("Error was excepted.");
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains("Bad header found in token storage"
					));
			}
		}

		private sealed class _MockRM_831 : MockRM
		{
			public _MockRM_831(Configuration baseArg1)
				: base(baseArg1)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void DoSecureLogin()
			{
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestReplaceExpiringDelegationToken()
		{
			conf.SetBoolean(YarnConfiguration.RmProxyUserPrivilegesEnabled, true);
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "kerberos");
			UserGroupInformation.SetConfiguration(conf);
			// create Token1:
			Org.Apache.Hadoop.IO.Text userText1 = new Org.Apache.Hadoop.IO.Text("user1");
			DelegationTokenIdentifier dtId1 = new DelegationTokenIdentifier(userText1, new Org.Apache.Hadoop.IO.Text
				("renewer1"), userText1);
			// set max date to 0 to simulate an expiring token;
			dtId1.SetMaxDate(0);
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token1 = new Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier>(dtId1.GetBytes(), Sharpen.Runtime.GetBytesForString(
				"password1"), dtId1.GetKind(), new Org.Apache.Hadoop.IO.Text("service1"));
			// create token2
			Org.Apache.Hadoop.IO.Text userText2 = new Org.Apache.Hadoop.IO.Text("user2");
			DelegationTokenIdentifier dtId2 = new DelegationTokenIdentifier(userText1, new Org.Apache.Hadoop.IO.Text
				("renewer2"), userText2);
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> expectedToken = 
				new Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier>(dtId2.GetBytes
				(), Sharpen.Runtime.GetBytesForString("password2"), dtId2.GetKind(), new Org.Apache.Hadoop.IO.Text
				("service2"));
			MockRM rm = new _TestSecurityMockRM_887(expectedToken, conf, null);
			rm.Start();
			Credentials credentials = new Credentials();
			credentials.AddToken(userText1, token1);
			RMApp app = rm.SubmitApp(200, "name", "user", new Dictionary<ApplicationAccessType
				, string>(), false, "default", 1, credentials);
			// wait for the initial expiring hdfs token to be removed from allTokens
			GenericTestUtils.WaitFor(new _Supplier_910(rm, token1), 1000, 20000);
			// wait for the initial expiring hdfs token to be removed from appTokens
			GenericTestUtils.WaitFor(new _Supplier_919(rm, token1), 1000, 20000);
			// wait for the new retrieved hdfs token.
			GenericTestUtils.WaitFor(new _Supplier_927(rm, expectedToken), 1000, 20000);
			// check nm can retrieve the token
			MockNM nm1 = new MockNM("127.0.0.1:1234", 15120, rm.GetResourceTrackerService());
			nm1.RegisterNode();
			NodeHeartbeatResponse response = nm1.NodeHeartbeat(true);
			ByteBuffer tokenBuffer = response.GetSystemCredentialsForApps()[app.GetApplicationId
				()];
			NUnit.Framework.Assert.IsNotNull(tokenBuffer);
			Credentials appCredentials = new Credentials();
			DataInputByteBuffer buf = new DataInputByteBuffer();
			tokenBuffer.Rewind();
			buf.Reset(tokenBuffer);
			appCredentials.ReadTokenStorageStream(buf);
			NUnit.Framework.Assert.IsTrue(appCredentials.GetAllTokens().Contains(expectedToken
				));
		}

		private sealed class _TestSecurityMockRM_887 : TestRMRestart.TestSecurityMockRM
		{
			public _TestSecurityMockRM_887(Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> expectedToken, Configuration baseArg1, RMStateStore baseArg2)
				: base(baseArg1, baseArg2)
			{
				this.expectedToken = expectedToken;
			}

			protected internal override DelegationTokenRenewer CreateDelegationTokenRenewer()
			{
				return new _DelegationTokenRenewer_890(expectedToken);
			}

			private sealed class _DelegationTokenRenewer_890 : DelegationTokenRenewer
			{
				public _DelegationTokenRenewer_890(Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
					> expectedToken)
				{
					this.expectedToken = expectedToken;
				}

				/// <exception cref="System.IO.IOException"/>
				protected internal override Org.Apache.Hadoop.Security.Token.Token<object>[] ObtainSystemTokensForUser
					(string user, Credentials credentials)
				{
					credentials.AddToken(expectedToken.GetService(), expectedToken);
					return new Org.Apache.Hadoop.Security.Token.Token<object>[] { expectedToken };
				}

				private readonly Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
					> expectedToken;
			}

			private readonly Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> expectedToken;
		}

		private sealed class _Supplier_910 : Supplier<bool>
		{
			public _Supplier_910(MockRM rm, Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> token1)
			{
				this.rm = rm;
				this.token1 = token1;
			}

			public bool Get()
			{
				return rm.GetRMContext().GetDelegationTokenRenewer().GetAllTokens()[token1] == null;
			}

			private readonly MockRM rm;

			private readonly Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> token1;
		}

		private sealed class _Supplier_919 : Supplier<bool>
		{
			public _Supplier_919(MockRM rm, Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> token1)
			{
				this.rm = rm;
				this.token1 = token1;
			}

			public bool Get()
			{
				return !rm.GetRMContext().GetDelegationTokenRenewer().GetDelegationTokens().Contains
					(token1);
			}

			private readonly MockRM rm;

			private readonly Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> token1;
		}

		private sealed class _Supplier_927 : Supplier<bool>
		{
			public _Supplier_927(MockRM rm, Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> expectedToken)
			{
				this.rm = rm;
				this.expectedToken = expectedToken;
			}

			public bool Get()
			{
				return rm.GetRMContext().GetDelegationTokenRenewer().GetDelegationTokens().Contains
					(expectedToken);
			}

			private readonly MockRM rm;

			private readonly Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> expectedToken;
		}

		// YARN will get the token for the app submitted without the delegation token.
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppSubmissionWithoutDelegationToken()
		{
			conf.SetBoolean(YarnConfiguration.RmProxyUserPrivilegesEnabled, true);
			// create token2
			Org.Apache.Hadoop.IO.Text userText2 = new Org.Apache.Hadoop.IO.Text("user2");
			DelegationTokenIdentifier dtId2 = new DelegationTokenIdentifier(new Org.Apache.Hadoop.IO.Text
				("user2"), new Org.Apache.Hadoop.IO.Text("renewer2"), userText2);
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token2 = new Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier>(dtId2.GetBytes(), Sharpen.Runtime.GetBytesForString(
				"password2"), dtId2.GetKind(), new Org.Apache.Hadoop.IO.Text("service2"));
			MockRM rm = new _TestSecurityMockRM_962(token2, conf, null);
			rm.Start();
			// submit an app without delegationToken
			RMApp app = rm.SubmitApp(200);
			// wait for the new retrieved hdfs token.
			GenericTestUtils.WaitFor(new _Supplier_981(rm, token2), 1000, 20000);
			// check nm can retrieve the token
			MockNM nm1 = new MockNM("127.0.0.1:1234", 15120, rm.GetResourceTrackerService());
			nm1.RegisterNode();
			NodeHeartbeatResponse response = nm1.NodeHeartbeat(true);
			ByteBuffer tokenBuffer = response.GetSystemCredentialsForApps()[app.GetApplicationId
				()];
			NUnit.Framework.Assert.IsNotNull(tokenBuffer);
			Credentials appCredentials = new Credentials();
			DataInputByteBuffer buf = new DataInputByteBuffer();
			tokenBuffer.Rewind();
			buf.Reset(tokenBuffer);
			appCredentials.ReadTokenStorageStream(buf);
			NUnit.Framework.Assert.IsTrue(appCredentials.GetAllTokens().Contains(token2));
		}

		private sealed class _TestSecurityMockRM_962 : TestRMRestart.TestSecurityMockRM
		{
			public _TestSecurityMockRM_962(Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> token2, Configuration baseArg1, RMStateStore baseArg2)
				: base(baseArg1, baseArg2)
			{
				this.token2 = token2;
			}

			protected internal override DelegationTokenRenewer CreateDelegationTokenRenewer()
			{
				return new _DelegationTokenRenewer_965(token2);
			}

			private sealed class _DelegationTokenRenewer_965 : DelegationTokenRenewer
			{
				public _DelegationTokenRenewer_965(Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
					> token2)
				{
					this.token2 = token2;
				}

				/// <exception cref="System.IO.IOException"/>
				protected internal override Org.Apache.Hadoop.Security.Token.Token<object>[] ObtainSystemTokensForUser
					(string user, Credentials credentials)
				{
					credentials.AddToken(token2.GetService(), token2);
					return new Org.Apache.Hadoop.Security.Token.Token<object>[] { token2 };
				}

				private readonly Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
					> token2;
			}

			private readonly Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> token2;
		}

		private sealed class _Supplier_981 : Supplier<bool>
		{
			public _Supplier_981(MockRM rm, Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> token2)
			{
				this.rm = rm;
				this.token2 = token2;
			}

			public bool Get()
			{
				return rm.GetRMContext().GetDelegationTokenRenewer().GetDelegationTokens().Contains
					(token2);
			}

			private readonly MockRM rm;

			private readonly Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> token2;
		}

		// Test submitting an application with the token obtained by a previously
		// submitted application.
		/// <exception cref="System.Exception"/>
		public virtual void TestAppSubmissionWithPreviousToken()
		{
			MockRM rm = new TestRMRestart.TestSecurityMockRM(conf, null);
			rm.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 15120, rm.GetResourceTrackerService());
			nm1.RegisterNode();
			// create Token1:
			Org.Apache.Hadoop.IO.Text userText1 = new Org.Apache.Hadoop.IO.Text("user");
			DelegationTokenIdentifier dtId1 = new DelegationTokenIdentifier(userText1, new Org.Apache.Hadoop.IO.Text
				("renewer1"), userText1);
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token1 = new Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier>(dtId1.GetBytes(), Sharpen.Runtime.GetBytesForString(
				"password1"), dtId1.GetKind(), new Org.Apache.Hadoop.IO.Text("service1"));
			Credentials credentials = new Credentials();
			credentials.AddToken(userText1, token1);
			// submit app1 with a token, set cancelTokenWhenComplete to false;
			RMApp app1 = rm.SubmitApp(200, "name", "user", null, false, null, 2, credentials, 
				null, true, false, false, null, 0, null, false);
			MockAM am1 = MockRM.LaunchAndRegisterAM(app1, rm, nm1);
			rm.WaitForState(app1.GetApplicationId(), RMAppState.Running);
			// submit app2 with the same token, set cancelTokenWhenComplete to true;
			RMApp app2 = rm.SubmitApp(200, "name", "user", null, false, null, 2, credentials, 
				null, true, false, false, null, 0, null, true);
			MockAM am2 = MockRM.LaunchAndRegisterAM(app2, rm, nm1);
			rm.WaitForState(app2.GetApplicationId(), RMAppState.Running);
			MockRM.FinishAMAndVerifyAppState(app2, rm, nm1, am2);
			NUnit.Framework.Assert.IsTrue(rm.GetRMContext().GetDelegationTokenRenewer().GetAllTokens
				().Contains(token1));
			MockRM.FinishAMAndVerifyAppState(app1, rm, nm1, am1);
			// app2 completes, app1 is still running, check the token is not cancelled
			NUnit.Framework.Assert.IsFalse(TestDelegationTokenRenewer.Renewer.cancelled);
		}

		// Test FileSystem memory leak in obtainSystemTokensForUser.
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFSLeakInObtainSystemTokensForUser()
		{
			Credentials credentials = new Credentials();
			string user = "test";
			int oldCounter = TestDelegationTokenRenewer.MyFS.GetInstanceCounter();
			delegationTokenRenewer.ObtainSystemTokensForUser(user, credentials);
			delegationTokenRenewer.ObtainSystemTokensForUser(user, credentials);
			delegationTokenRenewer.ObtainSystemTokensForUser(user, credentials);
			NUnit.Framework.Assert.AreEqual(oldCounter, TestDelegationTokenRenewer.MyFS.GetInstanceCounter
				());
		}

		// Test submitting an application with the token obtained by a previously
		// submitted application that is set to be cancelled.  Token should be
		// renewed while all apps are running, and then cancelled when all apps
		// complete
		/// <exception cref="System.Exception"/>
		public virtual void TestCancelWithMultipleAppSubmissions()
		{
			MockRM rm = new TestRMRestart.TestSecurityMockRM(conf, null);
			rm.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 15120, rm.GetResourceTrackerService());
			nm1.RegisterNode();
			//MyFS fs = (MyFS)FileSystem.get(conf);
			//MyToken token1 = fs.getDelegationToken("user123");
			// create Token1:
			Org.Apache.Hadoop.IO.Text userText1 = new Org.Apache.Hadoop.IO.Text("user");
			DelegationTokenIdentifier dtId1 = new DelegationTokenIdentifier(userText1, new Org.Apache.Hadoop.IO.Text
				("renewer1"), userText1);
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token1 = new Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier>(dtId1.GetBytes(), Sharpen.Runtime.GetBytesForString(
				"password1"), dtId1.GetKind(), new Org.Apache.Hadoop.IO.Text("service1"));
			Credentials credentials = new Credentials();
			credentials.AddToken(token1.GetService(), token1);
			DelegationTokenRenewer renewer = rm.GetRMContext().GetDelegationTokenRenewer();
			NUnit.Framework.Assert.IsTrue(renewer.GetAllTokens().IsEmpty());
			NUnit.Framework.Assert.IsFalse(TestDelegationTokenRenewer.Renewer.cancelled);
			RMApp app1 = rm.SubmitApp(200, "name", "user", null, false, null, 2, credentials, 
				null, true, false, false, null, 0, null, true);
			MockAM am1 = MockRM.LaunchAndRegisterAM(app1, rm, nm1);
			rm.WaitForState(app1.GetApplicationId(), RMAppState.Running);
			DelegationTokenRenewer.DelegationTokenToRenew dttr = renewer.GetAllTokens()[token1
				];
			NUnit.Framework.Assert.IsNotNull(dttr);
			NUnit.Framework.Assert.IsTrue(dttr.referringAppIds.Contains(app1.GetApplicationId
				()));
			RMApp app2 = rm.SubmitApp(200, "name", "user", null, false, null, 2, credentials, 
				null, true, false, false, null, 0, null, true);
			MockAM am2 = MockRM.LaunchAndRegisterAM(app2, rm, nm1);
			rm.WaitForState(app2.GetApplicationId(), RMAppState.Running);
			NUnit.Framework.Assert.IsTrue(renewer.GetAllTokens().Contains(token1));
			NUnit.Framework.Assert.IsTrue(dttr.referringAppIds.Contains(app2.GetApplicationId
				()));
			NUnit.Framework.Assert.IsTrue(dttr.referringAppIds.Contains(app2.GetApplicationId
				()));
			NUnit.Framework.Assert.IsFalse(TestDelegationTokenRenewer.Renewer.cancelled);
			MockRM.FinishAMAndVerifyAppState(app2, rm, nm1, am2);
			// app2 completes, app1 is still running, check the token is not cancelled
			NUnit.Framework.Assert.IsTrue(renewer.GetAllTokens().Contains(token1));
			NUnit.Framework.Assert.IsTrue(dttr.referringAppIds.Contains(app1.GetApplicationId
				()));
			NUnit.Framework.Assert.IsFalse(dttr.referringAppIds.Contains(app2.GetApplicationId
				()));
			NUnit.Framework.Assert.IsFalse(dttr.IsTimerCancelled());
			NUnit.Framework.Assert.IsFalse(TestDelegationTokenRenewer.Renewer.cancelled);
			RMApp app3 = rm.SubmitApp(200, "name", "user", null, false, null, 2, credentials, 
				null, true, false, false, null, 0, null, true);
			MockAM am3 = MockRM.LaunchAndRegisterAM(app3, rm, nm1);
			rm.WaitForState(app3.GetApplicationId(), RMAppState.Running);
			NUnit.Framework.Assert.IsTrue(renewer.GetAllTokens().Contains(token1));
			NUnit.Framework.Assert.IsTrue(dttr.referringAppIds.Contains(app1.GetApplicationId
				()));
			NUnit.Framework.Assert.IsTrue(dttr.referringAppIds.Contains(app3.GetApplicationId
				()));
			NUnit.Framework.Assert.IsFalse(dttr.IsTimerCancelled());
			NUnit.Framework.Assert.IsFalse(TestDelegationTokenRenewer.Renewer.cancelled);
			MockRM.FinishAMAndVerifyAppState(app1, rm, nm1, am1);
			NUnit.Framework.Assert.IsTrue(renewer.GetAllTokens().Contains(token1));
			NUnit.Framework.Assert.IsFalse(dttr.referringAppIds.Contains(app1.GetApplicationId
				()));
			NUnit.Framework.Assert.IsTrue(dttr.referringAppIds.Contains(app3.GetApplicationId
				()));
			NUnit.Framework.Assert.IsFalse(dttr.IsTimerCancelled());
			NUnit.Framework.Assert.IsFalse(TestDelegationTokenRenewer.Renewer.cancelled);
			MockRM.FinishAMAndVerifyAppState(app3, rm, nm1, am3);
			NUnit.Framework.Assert.IsFalse(renewer.GetAllTokens().Contains(token1));
			NUnit.Framework.Assert.IsTrue(dttr.referringAppIds.IsEmpty());
			NUnit.Framework.Assert.IsTrue(dttr.IsTimerCancelled());
			NUnit.Framework.Assert.IsTrue(TestDelegationTokenRenewer.Renewer.cancelled);
		}
	}
}
