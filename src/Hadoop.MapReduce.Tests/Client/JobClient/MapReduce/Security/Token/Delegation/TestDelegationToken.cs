using NUnit.Framework;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Security.Token.Delegation
{
	public class TestDelegationToken
	{
		private MiniMRCluster cluster;

		private UserGroupInformation user1;

		private UserGroupInformation user2;

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			user1 = UserGroupInformation.CreateUserForTesting("alice", new string[] { "users"
				 });
			user2 = UserGroupInformation.CreateUserForTesting("bob", new string[] { "users" }
				);
			cluster = new MiniMRCluster(0, 0, 1, "file:///", 1);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDelegationToken()
		{
			JobClient client;
			client = user1.DoAs(new _PrivilegedExceptionAction_56(this));
			JobClient bobClient;
			bobClient = user2.DoAs(new _PrivilegedExceptionAction_64(this));
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = client.
				GetDelegationToken(new Text(user1.GetUserName()));
			DataInputBuffer inBuf = new DataInputBuffer();
			byte[] bytes = token.GetIdentifier();
			inBuf.Reset(bytes, bytes.Length);
			DelegationTokenIdentifier ident = new DelegationTokenIdentifier();
			ident.ReadFields(inBuf);
			NUnit.Framework.Assert.AreEqual("alice", ident.GetUser().GetUserName());
			long createTime = ident.GetIssueDate();
			long maxTime = ident.GetMaxDate();
			long currentTime = Runtime.CurrentTimeMillis();
			System.Console.Out.WriteLine("create time: " + createTime);
			System.Console.Out.WriteLine("current time: " + currentTime);
			System.Console.Out.WriteLine("max time: " + maxTime);
			NUnit.Framework.Assert.IsTrue("createTime < current", createTime < currentTime);
			NUnit.Framework.Assert.IsTrue("current < maxTime", currentTime < maxTime);
			// renew should work as user alice
			user1.DoAs(new _PrivilegedExceptionAction_91(client, token));
			// bob should fail to renew
			user2.DoAs(new _PrivilegedExceptionAction_101(bobClient, token));
			// PASS
			// bob should fail to cancel
			user2.DoAs(new _PrivilegedExceptionAction_115(bobClient, token));
			// PASS
			// alice should be able to cancel but only cancel once
			user1.DoAs(new _PrivilegedExceptionAction_129(client, token));
		}

		private sealed class _PrivilegedExceptionAction_56 : PrivilegedExceptionAction<JobClient
			>
		{
			public _PrivilegedExceptionAction_56(TestDelegationToken _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public JobClient Run()
			{
				return new JobClient(this._enclosing.cluster.CreateJobConf());
			}

			private readonly TestDelegationToken _enclosing;
		}

		private sealed class _PrivilegedExceptionAction_64 : PrivilegedExceptionAction<JobClient
			>
		{
			public _PrivilegedExceptionAction_64(TestDelegationToken _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public JobClient Run()
			{
				return new JobClient(this._enclosing.cluster.CreateJobConf());
			}

			private readonly TestDelegationToken _enclosing;
		}

		private sealed class _PrivilegedExceptionAction_91 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_91(JobClient client, Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier> token)
			{
				this.client = client;
				this.token = token;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				client.RenewDelegationToken(token);
				client.RenewDelegationToken(token);
				return null;
			}

			private readonly JobClient client;

			private readonly Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> token;
		}

		private sealed class _PrivilegedExceptionAction_101 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_101(JobClient bobClient, Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier> token)
			{
				this.bobClient = bobClient;
				this.token = token;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				try
				{
					bobClient.RenewDelegationToken(token);
					NUnit.Framework.Assert.Fail("bob renew");
				}
				catch (AccessControlException)
				{
				}
				return null;
			}

			private readonly JobClient bobClient;

			private readonly Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> token;
		}

		private sealed class _PrivilegedExceptionAction_115 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_115(JobClient bobClient, Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier> token)
			{
				this.bobClient = bobClient;
				this.token = token;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				try
				{
					bobClient.CancelDelegationToken(token);
					NUnit.Framework.Assert.Fail("bob cancel");
				}
				catch (AccessControlException)
				{
				}
				return null;
			}

			private readonly JobClient bobClient;

			private readonly Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> token;
		}

		private sealed class _PrivilegedExceptionAction_129 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_129(JobClient client, Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier> token)
			{
				this.client = client;
				this.token = token;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				client.CancelDelegationToken(token);
				try
				{
					client.CancelDelegationToken(token);
					NUnit.Framework.Assert.Fail("second alice cancel");
				}
				catch (SecretManager.InvalidToken)
				{
				}
				// PASS
				return null;
			}

			private readonly JobClient client;

			private readonly Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> token;
		}
	}
}
