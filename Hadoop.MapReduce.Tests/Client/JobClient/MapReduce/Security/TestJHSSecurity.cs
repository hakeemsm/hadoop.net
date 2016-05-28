using System.IO;
using System.Net;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Mapreduce.V2.HS;
using Org.Apache.Hadoop.Mapreduce.V2.Jobhistory;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Security
{
	public class TestJHSSecurity
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestJHSSecurity));

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDelegationToken()
		{
			Logger rootLogger = LogManager.GetRootLogger();
			rootLogger.SetLevel(Level.Debug);
			YarnConfiguration conf = new YarnConfiguration(new JobConf());
			// Just a random principle
			conf.Set(JHAdminConfig.MrHistoryPrincipal, "RandomOrc/localhost@apache.org");
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "kerberos");
			UserGroupInformation.SetConfiguration(conf);
			long initialInterval = 10000l;
			long maxLifetime = 20000l;
			long renewInterval = 10000l;
			JobHistoryServer jobHistoryServer = null;
			MRClientProtocol clientUsingDT = null;
			long tokenFetchTime;
			try
			{
				jobHistoryServer = new _JobHistoryServer_87(initialInterval, maxLifetime, renewInterval
					);
				// no keytab based login
				// Don't need it, skip.;
				//      final JobHistoryServer jobHistoryServer = jhServer;
				jobHistoryServer.Init(conf);
				jobHistoryServer.Start();
				MRClientProtocol hsService = jobHistoryServer.GetClientService().GetClientHandler
					();
				// Fake the authentication-method
				UserGroupInformation loggedInUser = UserGroupInformation.CreateRemoteUser("testrenewer@APACHE.ORG"
					);
				NUnit.Framework.Assert.AreEqual("testrenewer", loggedInUser.GetShortUserName());
				// Default realm is APACHE.ORG
				loggedInUser.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Kerberos
					);
				Token token = GetDelegationToken(loggedInUser, hsService, loggedInUser.GetShortUserName
					());
				tokenFetchTime = Runtime.CurrentTimeMillis();
				Log.Info("Got delegation token at: " + tokenFetchTime);
				// Now try talking to JHS using the delegation token
				clientUsingDT = GetMRClientProtocol(token, jobHistoryServer.GetClientService().GetBindAddress
					(), "TheDarkLord", conf);
				GetJobReportRequest jobReportRequest = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
					<GetJobReportRequest>();
				jobReportRequest.SetJobId(MRBuilderUtils.NewJobId(123456, 1, 1));
				try
				{
					clientUsingDT.GetJobReport(jobReportRequest);
				}
				catch (IOException e)
				{
					NUnit.Framework.Assert.AreEqual("Unknown job job_123456_0001", e.Message);
				}
				// Renew after 50% of token age.
				while (Runtime.CurrentTimeMillis() < tokenFetchTime + initialInterval / 2)
				{
					Sharpen.Thread.Sleep(500l);
				}
				long nextExpTime = RenewDelegationToken(loggedInUser, hsService, token);
				long renewalTime = Runtime.CurrentTimeMillis();
				Log.Info("Renewed token at: " + renewalTime + ", NextExpiryTime: " + nextExpTime);
				// Wait for first expiry, but before renewed expiry.
				while (Runtime.CurrentTimeMillis() > tokenFetchTime + initialInterval && Runtime.
					CurrentTimeMillis() < nextExpTime)
				{
					Sharpen.Thread.Sleep(500l);
				}
				Sharpen.Thread.Sleep(50l);
				// Valid token because of renewal.
				try
				{
					clientUsingDT.GetJobReport(jobReportRequest);
				}
				catch (IOException e)
				{
					NUnit.Framework.Assert.AreEqual("Unknown job job_123456_0001", e.Message);
				}
				// Wait for expiry.
				while (Runtime.CurrentTimeMillis() < renewalTime + renewInterval)
				{
					Sharpen.Thread.Sleep(500l);
				}
				Sharpen.Thread.Sleep(50l);
				Log.Info("At time: " + Runtime.CurrentTimeMillis() + ", token should be invalid");
				// Token should have expired.      
				try
				{
					clientUsingDT.GetJobReport(jobReportRequest);
					NUnit.Framework.Assert.Fail("Should not have succeeded with an expired token");
				}
				catch (IOException e)
				{
					NUnit.Framework.Assert.IsTrue(e.InnerException.Message.Contains("is expired"));
				}
				// Test cancellation
				// Stop the existing proxy, start another.
				if (clientUsingDT != null)
				{
					//        RPC.stopProxy(clientUsingDT);
					clientUsingDT = null;
				}
				token = GetDelegationToken(loggedInUser, hsService, loggedInUser.GetShortUserName
					());
				tokenFetchTime = Runtime.CurrentTimeMillis();
				Log.Info("Got delegation token at: " + tokenFetchTime);
				// Now try talking to HSService using the delegation token
				clientUsingDT = GetMRClientProtocol(token, jobHistoryServer.GetClientService().GetBindAddress
					(), "loginuser2", conf);
				try
				{
					clientUsingDT.GetJobReport(jobReportRequest);
				}
				catch (IOException e)
				{
					NUnit.Framework.Assert.Fail("Unexpected exception" + e);
				}
				CancelDelegationToken(loggedInUser, hsService, token);
				// Testing the token with different renewer to cancel the token
				Token tokenWithDifferentRenewer = GetDelegationToken(loggedInUser, hsService, "yarn"
					);
				CancelDelegationToken(loggedInUser, hsService, tokenWithDifferentRenewer);
				if (clientUsingDT != null)
				{
					//        RPC.stopProxy(clientUsingDT);
					clientUsingDT = null;
				}
				// Creating a new connection.
				clientUsingDT = GetMRClientProtocol(token, jobHistoryServer.GetClientService().GetBindAddress
					(), "loginuser2", conf);
				Log.Info("Cancelled delegation token at: " + Runtime.CurrentTimeMillis());
				// Verify cancellation worked.
				try
				{
					clientUsingDT.GetJobReport(jobReportRequest);
					NUnit.Framework.Assert.Fail("Should not have succeeded with a cancelled delegation token"
						);
				}
				catch (IOException)
				{
				}
			}
			finally
			{
				jobHistoryServer.Stop();
			}
		}

		private sealed class _JobHistoryServer_87 : JobHistoryServer
		{
			public _JobHistoryServer_87(long initialInterval, long maxLifetime, long renewInterval
				)
			{
				this.initialInterval = initialInterval;
				this.maxLifetime = maxLifetime;
				this.renewInterval = renewInterval;
			}

			/// <exception cref="System.IO.IOException"/>
			protected override void DoSecureLogin(Configuration conf)
			{
			}

			protected override JHSDelegationTokenSecretManager CreateJHSSecretManager(Configuration
				 conf, HistoryServerStateStoreService store)
			{
				return new JHSDelegationTokenSecretManager(initialInterval, maxLifetime, renewInterval
					, 3600000, store);
			}

			protected override HistoryClientService CreateHistoryClientService()
			{
				return new _HistoryClientService_102(this.historyContext, this.jhsDTSecretManager
					);
			}

			private sealed class _HistoryClientService_102 : HistoryClientService
			{
				public _HistoryClientService_102(HistoryContext baseArg1, JHSDelegationTokenSecretManager
					 baseArg2)
					: base(baseArg1, baseArg2)
				{
				}

				protected override void InitializeWebApp(Configuration conf)
				{
				}
			}

			private readonly long initialInterval;

			private readonly long maxLifetime;

			private readonly long renewInterval;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private Token GetDelegationToken(UserGroupInformation loggedInUser, MRClientProtocol
			 hsService, string renewerString)
		{
			// Get the delegation token directly as it is a little difficult to setup
			// the kerberos based rpc.
			Token token = loggedInUser.DoAs(new _PrivilegedExceptionAction_236(renewerString, 
				hsService));
			return token;
		}

		private sealed class _PrivilegedExceptionAction_236 : PrivilegedExceptionAction<Token
			>
		{
			public _PrivilegedExceptionAction_236(string renewerString, MRClientProtocol hsService
				)
			{
				this.renewerString = renewerString;
				this.hsService = hsService;
			}

			/// <exception cref="System.IO.IOException"/>
			public Token Run()
			{
				GetDelegationTokenRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
					<GetDelegationTokenRequest>();
				request.SetRenewer(renewerString);
				return hsService.GetDelegationToken(request).GetDelegationToken();
			}

			private readonly string renewerString;

			private readonly MRClientProtocol hsService;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private long RenewDelegationToken(UserGroupInformation loggedInUser, MRClientProtocol
			 hsService, Token dToken)
		{
			long nextExpTime = loggedInUser.DoAs(new _PrivilegedExceptionAction_252(dToken, hsService
				));
			return nextExpTime;
		}

		private sealed class _PrivilegedExceptionAction_252 : PrivilegedExceptionAction<long
			>
		{
			public _PrivilegedExceptionAction_252(Token dToken, MRClientProtocol hsService)
			{
				this.dToken = dToken;
				this.hsService = hsService;
			}

			/// <exception cref="System.IO.IOException"/>
			public long Run()
			{
				RenewDelegationTokenRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
					<RenewDelegationTokenRequest>();
				request.SetDelegationToken(dToken);
				return hsService.RenewDelegationToken(request).GetNextExpirationTime();
			}

			private readonly Token dToken;

			private readonly MRClientProtocol hsService;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void CancelDelegationToken(UserGroupInformation loggedInUser, MRClientProtocol
			 hsService, Token dToken)
		{
			loggedInUser.DoAs(new _PrivilegedExceptionAction_269(dToken, hsService));
		}

		private sealed class _PrivilegedExceptionAction_269 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_269(Token dToken, MRClientProtocol hsService)
			{
				this.dToken = dToken;
				this.hsService = hsService;
			}

			/// <exception cref="System.IO.IOException"/>
			public Void Run()
			{
				CancelDelegationTokenRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
					<CancelDelegationTokenRequest>();
				request.SetDelegationToken(dToken);
				hsService.CancelDelegationToken(request);
				return null;
			}

			private readonly Token dToken;

			private readonly MRClientProtocol hsService;
		}

		private MRClientProtocol GetMRClientProtocol(Token token, IPEndPoint hsAddress, string
			 user, Configuration conf)
		{
			UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser(user);
			ugi.AddToken(ConverterUtils.ConvertFromYarn(token, hsAddress));
			YarnRPC rpc = YarnRPC.Create(conf);
			MRClientProtocol hsWithDT = ugi.DoAs(new _PrivilegedAction_288(rpc, hsAddress, conf
				));
			return hsWithDT;
		}

		private sealed class _PrivilegedAction_288 : PrivilegedAction<MRClientProtocol>
		{
			public _PrivilegedAction_288(YarnRPC rpc, IPEndPoint hsAddress, Configuration conf
				)
			{
				this.rpc = rpc;
				this.hsAddress = hsAddress;
				this.conf = conf;
			}

			public MRClientProtocol Run()
			{
				return (MRClientProtocol)rpc.GetProxy(typeof(HSClientProtocol), hsAddress, conf);
			}

			private readonly YarnRPC rpc;

			private readonly IPEndPoint hsAddress;

			private readonly Configuration conf;
		}
	}
}
