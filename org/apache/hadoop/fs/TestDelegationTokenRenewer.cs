using Sharpen;

namespace org.apache.hadoop.fs
{
	public class TestDelegationTokenRenewer
	{
		public abstract class RenewableFileSystem : org.apache.hadoop.fs.FileSystem, org.apache.hadoop.fs.DelegationTokenRenewer.Renewable
		{
			public abstract org.apache.hadoop.security.token.Token<object> getRenewToken();

			public abstract void setDelegationToken(org.apache.hadoop.security.token.Token<T>
				 arg1);

			internal RenewableFileSystem(TestDelegationTokenRenewer _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestDelegationTokenRenewer _enclosing;
		}

		private const long RENEW_CYCLE = 1000;

		private org.apache.hadoop.fs.DelegationTokenRenewer renewer;

		internal org.apache.hadoop.conf.Configuration conf;

		internal org.apache.hadoop.fs.FileSystem fs;

		[NUnit.Framework.SetUp]
		public virtual void setup()
		{
			org.apache.hadoop.fs.DelegationTokenRenewer.renewCycle = RENEW_CYCLE;
			org.apache.hadoop.fs.DelegationTokenRenewer.reset();
			renewer = org.apache.hadoop.fs.DelegationTokenRenewer.getInstance();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testAddRemoveRenewAction()
		{
			org.apache.hadoop.io.Text service = new org.apache.hadoop.io.Text("myservice");
			org.apache.hadoop.conf.Configuration conf = org.mockito.Mockito.mock<org.apache.hadoop.conf.Configuration
				>();
			org.apache.hadoop.security.token.Token<object> token = org.mockito.Mockito.mock<org.apache.hadoop.security.token.Token
				>();
			org.mockito.Mockito.doReturn(service).when(token).getService();
			org.mockito.Mockito.doAnswer(new _Answer_61()).when(token).renew(any<org.apache.hadoop.conf.Configuration
				>());
			org.apache.hadoop.fs.TestDelegationTokenRenewer.RenewableFileSystem fs = org.mockito.Mockito.mock
				<org.apache.hadoop.fs.TestDelegationTokenRenewer.RenewableFileSystem>();
			org.mockito.Mockito.doReturn(conf).when(fs).getConf();
			org.mockito.Mockito.doReturn(token).when(fs).getRenewToken();
			renewer.addRenewAction(fs);
			NUnit.Framework.Assert.AreEqual("FileSystem not added to DelegationTokenRenewer", 
				1, renewer.getRenewQueueLength());
			java.lang.Thread.sleep(RENEW_CYCLE * 2);
			org.mockito.Mockito.verify(token, org.mockito.Mockito.atLeast(2)).renew(eq(conf));
			org.mockito.Mockito.verify(token, org.mockito.Mockito.atMost(3)).renew(eq(conf));
			org.mockito.Mockito.verify(token, org.mockito.Mockito.never()).cancel(any<org.apache.hadoop.conf.Configuration
				>());
			renewer.removeRenewAction(fs);
			org.mockito.Mockito.verify(token).cancel(eq(conf));
			org.mockito.Mockito.verify(fs, org.mockito.Mockito.never()).getDelegationToken(null
				);
			org.mockito.Mockito.verify(fs, org.mockito.Mockito.never()).setDelegationToken(any
				<org.apache.hadoop.security.token.Token>());
			NUnit.Framework.Assert.AreEqual("FileSystem not removed from DelegationTokenRenewer"
				, 0, renewer.getRenewQueueLength());
		}

		private sealed class _Answer_61 : org.mockito.stubbing.Answer<long>
		{
			public _Answer_61()
			{
			}

			public long answer(org.mockito.invocation.InvocationOnMock invocation)
			{
				return org.apache.hadoop.util.Time.now() + org.apache.hadoop.fs.TestDelegationTokenRenewer
					.RENEW_CYCLE;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testAddRenewActionWithNoToken()
		{
			org.apache.hadoop.conf.Configuration conf = org.mockito.Mockito.mock<org.apache.hadoop.conf.Configuration
				>();
			org.apache.hadoop.fs.TestDelegationTokenRenewer.RenewableFileSystem fs = org.mockito.Mockito.mock
				<org.apache.hadoop.fs.TestDelegationTokenRenewer.RenewableFileSystem>();
			org.mockito.Mockito.doReturn(conf).when(fs).getConf();
			org.mockito.Mockito.doReturn(null).when(fs).getRenewToken();
			renewer.addRenewAction(fs);
			org.mockito.Mockito.verify(fs).getRenewToken();
			NUnit.Framework.Assert.AreEqual(0, renewer.getRenewQueueLength());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGetNewTokenOnRenewFailure()
		{
			org.apache.hadoop.io.Text service = new org.apache.hadoop.io.Text("myservice");
			org.apache.hadoop.conf.Configuration conf = org.mockito.Mockito.mock<org.apache.hadoop.conf.Configuration
				>();
			org.apache.hadoop.security.token.Token<object> token1 = org.mockito.Mockito.mock<
				org.apache.hadoop.security.token.Token>();
			org.mockito.Mockito.doReturn(service).when(token1).getService();
			org.mockito.Mockito.doThrow(new System.IO.IOException("boom")).when(token1).renew
				(eq(conf));
			org.apache.hadoop.security.token.Token<object> token2 = org.mockito.Mockito.mock<
				org.apache.hadoop.security.token.Token>();
			org.mockito.Mockito.doReturn(service).when(token2).getService();
			org.mockito.Mockito.doAnswer(new _Answer_117()).when(token2).renew(eq(conf));
			org.apache.hadoop.fs.TestDelegationTokenRenewer.RenewableFileSystem fs = org.mockito.Mockito.mock
				<org.apache.hadoop.fs.TestDelegationTokenRenewer.RenewableFileSystem>();
			org.mockito.Mockito.doReturn(conf).when(fs).getConf();
			org.mockito.Mockito.doReturn(token1).doReturn(token2).when(fs).getRenewToken();
			org.mockito.Mockito.doReturn(token2).when(fs).getDelegationToken(null);
			org.mockito.Mockito.doAnswer(new _Answer_128(token2)).when(fs).addDelegationTokens
				(null, null);
			renewer.addRenewAction(fs);
			NUnit.Framework.Assert.AreEqual(1, renewer.getRenewQueueLength());
			java.lang.Thread.sleep(RENEW_CYCLE);
			org.mockito.Mockito.verify(fs).getRenewToken();
			org.mockito.Mockito.verify(token1, org.mockito.Mockito.atLeast(1)).renew(eq(conf)
				);
			org.mockito.Mockito.verify(token1, org.mockito.Mockito.atMost(2)).renew(eq(conf));
			org.mockito.Mockito.verify(fs).addDelegationTokens(null, null);
			org.mockito.Mockito.verify(fs).setDelegationToken(eq(token2));
			NUnit.Framework.Assert.AreEqual(1, renewer.getRenewQueueLength());
			renewer.removeRenewAction(fs);
			org.mockito.Mockito.verify(token2).cancel(eq(conf));
			NUnit.Framework.Assert.AreEqual(0, renewer.getRenewQueueLength());
		}

		private sealed class _Answer_117 : org.mockito.stubbing.Answer<long>
		{
			public _Answer_117()
			{
			}

			public long answer(org.mockito.invocation.InvocationOnMock invocation)
			{
				return org.apache.hadoop.util.Time.now() + org.apache.hadoop.fs.TestDelegationTokenRenewer
					.RENEW_CYCLE;
			}
		}

		private sealed class _Answer_128 : org.mockito.stubbing.Answer<org.apache.hadoop.security.token.Token
			<object>[]>
		{
			public _Answer_128(org.apache.hadoop.security.token.Token<object> token2)
			{
				this.token2 = token2;
			}

			public org.apache.hadoop.security.token.Token<object>[] answer(org.mockito.invocation.InvocationOnMock
				 invocation)
			{
				return new org.apache.hadoop.security.token.Token<object>[] { token2 };
			}

			private readonly org.apache.hadoop.security.token.Token<object> token2;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testStopRenewalWhenFsGone()
		{
			org.apache.hadoop.conf.Configuration conf = org.mockito.Mockito.mock<org.apache.hadoop.conf.Configuration
				>();
			org.apache.hadoop.security.token.Token<object> token = org.mockito.Mockito.mock<org.apache.hadoop.security.token.Token
				>();
			org.mockito.Mockito.doReturn(new org.apache.hadoop.io.Text("myservice")).when(token
				).getService();
			org.mockito.Mockito.doAnswer(new _Answer_157()).when(token).renew(any<org.apache.hadoop.conf.Configuration
				>());
			org.apache.hadoop.fs.TestDelegationTokenRenewer.RenewableFileSystem fs = org.mockito.Mockito.mock
				<org.apache.hadoop.fs.TestDelegationTokenRenewer.RenewableFileSystem>();
			org.mockito.Mockito.doReturn(conf).when(fs).getConf();
			org.mockito.Mockito.doReturn(token).when(fs).getRenewToken();
			renewer.addRenewAction(fs);
			NUnit.Framework.Assert.AreEqual(1, renewer.getRenewQueueLength());
			java.lang.Thread.sleep(RENEW_CYCLE);
			org.mockito.Mockito.verify(token, org.mockito.Mockito.atLeast(1)).renew(eq(conf));
			org.mockito.Mockito.verify(token, org.mockito.Mockito.atMost(2)).renew(eq(conf));
			// drop weak ref
			fs = null;
			Sharpen.Runtime.gc();
			Sharpen.Runtime.gc();
			Sharpen.Runtime.gc();
			// next renew should detect the fs as gone
			java.lang.Thread.sleep(RENEW_CYCLE);
			org.mockito.Mockito.verify(token, org.mockito.Mockito.atLeast(1)).renew(eq(conf));
			org.mockito.Mockito.verify(token, org.mockito.Mockito.atMost(2)).renew(eq(conf));
			NUnit.Framework.Assert.AreEqual(0, renewer.getRenewQueueLength());
		}

		private sealed class _Answer_157 : org.mockito.stubbing.Answer<long>
		{
			public _Answer_157()
			{
			}

			public long answer(org.mockito.invocation.InvocationOnMock invocation)
			{
				return org.apache.hadoop.util.Time.now() + org.apache.hadoop.fs.TestDelegationTokenRenewer
					.RENEW_CYCLE;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void testMultipleTokensDoNotDeadlock()
		{
			org.apache.hadoop.conf.Configuration conf = org.mockito.Mockito.mock<org.apache.hadoop.conf.Configuration
				>();
			org.apache.hadoop.fs.FileSystem fs = org.mockito.Mockito.mock<org.apache.hadoop.fs.FileSystem
				>();
			org.mockito.Mockito.doReturn(conf).when(fs).getConf();
			long distantFuture = org.apache.hadoop.util.Time.now() + 3600 * 1000;
			// 1h
			org.apache.hadoop.security.token.Token<object> token1 = org.mockito.Mockito.mock<
				org.apache.hadoop.security.token.Token>();
			org.mockito.Mockito.doReturn(new org.apache.hadoop.io.Text("myservice1")).when(token1
				).getService();
			org.mockito.Mockito.doReturn(distantFuture).when(token1).renew(eq(conf));
			org.apache.hadoop.security.token.Token<object> token2 = org.mockito.Mockito.mock<
				org.apache.hadoop.security.token.Token>();
			org.mockito.Mockito.doReturn(new org.apache.hadoop.io.Text("myservice2")).when(token2
				).getService();
			org.mockito.Mockito.doReturn(distantFuture).when(token2).renew(eq(conf));
			org.apache.hadoop.fs.TestDelegationTokenRenewer.RenewableFileSystem fs1 = org.mockito.Mockito.mock
				<org.apache.hadoop.fs.TestDelegationTokenRenewer.RenewableFileSystem>();
			org.mockito.Mockito.doReturn(conf).when(fs1).getConf();
			org.mockito.Mockito.doReturn(token1).when(fs1).getRenewToken();
			org.apache.hadoop.fs.TestDelegationTokenRenewer.RenewableFileSystem fs2 = org.mockito.Mockito.mock
				<org.apache.hadoop.fs.TestDelegationTokenRenewer.RenewableFileSystem>();
			org.mockito.Mockito.doReturn(conf).when(fs2).getConf();
			org.mockito.Mockito.doReturn(token2).when(fs2).getRenewToken();
			renewer.addRenewAction(fs1);
			renewer.addRenewAction(fs2);
			NUnit.Framework.Assert.AreEqual(2, renewer.getRenewQueueLength());
			renewer.removeRenewAction(fs1);
			NUnit.Framework.Assert.AreEqual(1, renewer.getRenewQueueLength());
			renewer.removeRenewAction(fs2);
			NUnit.Framework.Assert.AreEqual(0, renewer.getRenewQueueLength());
			org.mockito.Mockito.verify(token1).cancel(eq(conf));
			org.mockito.Mockito.verify(token2).cancel(eq(conf));
		}
	}
}
