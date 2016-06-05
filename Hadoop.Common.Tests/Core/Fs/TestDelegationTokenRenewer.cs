using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;


namespace Org.Apache.Hadoop.FS
{
	public class TestDelegationTokenRenewer
	{
		public abstract class RenewableFileSystem : FileSystem, DelegationTokenRenewer.Renewable
		{
			public abstract Org.Apache.Hadoop.Security.Token.Token<object> GetRenewToken();

			public abstract void SetDelegationToken(Org.Apache.Hadoop.Security.Token.Token<T>
				 arg1);

			internal RenewableFileSystem(TestDelegationTokenRenewer _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestDelegationTokenRenewer _enclosing;
		}

		private const long RenewCycle = 1000;

		private DelegationTokenRenewer renewer;

		internal Configuration conf;

		internal FileSystem fs;

		[SetUp]
		public virtual void Setup()
		{
			DelegationTokenRenewer.renewCycle = RenewCycle;
			DelegationTokenRenewer.Reset();
			renewer = DelegationTokenRenewer.GetInstance();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestAddRemoveRenewAction()
		{
			Text service = new Text("myservice");
			Configuration conf = Org.Mockito.Mockito.Mock<Configuration>();
			Org.Apache.Hadoop.Security.Token.Token<object> token = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Security.Token.Token
				>();
			Org.Mockito.Mockito.DoReturn(service).When(token).GetService();
			Org.Mockito.Mockito.DoAnswer(new _Answer_61()).When(token).Renew(Any<Configuration
				>());
			TestDelegationTokenRenewer.RenewableFileSystem fs = Org.Mockito.Mockito.Mock<TestDelegationTokenRenewer.RenewableFileSystem
				>();
			Org.Mockito.Mockito.DoReturn(conf).When(fs).GetConf();
			Org.Mockito.Mockito.DoReturn(token).When(fs).GetRenewToken();
			renewer.AddRenewAction(fs);
			Assert.Equal("FileSystem not added to DelegationTokenRenewer", 
				1, renewer.GetRenewQueueLength());
			Thread.Sleep(RenewCycle * 2);
			Org.Mockito.Mockito.Verify(token, Org.Mockito.Mockito.AtLeast(2)).Renew(Eq(conf));
			Org.Mockito.Mockito.Verify(token, Org.Mockito.Mockito.AtMost(3)).Renew(Eq(conf));
			Org.Mockito.Mockito.Verify(token, Org.Mockito.Mockito.Never()).Cancel(Any<Configuration
				>());
			renewer.RemoveRenewAction(fs);
			Org.Mockito.Mockito.Verify(token).Cancel(Eq(conf));
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).GetDelegationToken(null
				);
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).SetDelegationToken(Any
				<Org.Apache.Hadoop.Security.Token.Token>());
			Assert.Equal("FileSystem not removed from DelegationTokenRenewer"
				, 0, renewer.GetRenewQueueLength());
		}

		private sealed class _Answer_61 : Answer<long>
		{
			public _Answer_61()
			{
			}

			public long Answer(InvocationOnMock invocation)
			{
				return Time.Now() + TestDelegationTokenRenewer.RenewCycle;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestAddRenewActionWithNoToken()
		{
			Configuration conf = Org.Mockito.Mockito.Mock<Configuration>();
			TestDelegationTokenRenewer.RenewableFileSystem fs = Org.Mockito.Mockito.Mock<TestDelegationTokenRenewer.RenewableFileSystem
				>();
			Org.Mockito.Mockito.DoReturn(conf).When(fs).GetConf();
			Org.Mockito.Mockito.DoReturn(null).When(fs).GetRenewToken();
			renewer.AddRenewAction(fs);
			Org.Mockito.Mockito.Verify(fs).GetRenewToken();
			Assert.Equal(0, renewer.GetRenewQueueLength());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestGetNewTokenOnRenewFailure()
		{
			Text service = new Text("myservice");
			Configuration conf = Org.Mockito.Mockito.Mock<Configuration>();
			Org.Apache.Hadoop.Security.Token.Token<object> token1 = Org.Mockito.Mockito.Mock<
				Org.Apache.Hadoop.Security.Token.Token>();
			Org.Mockito.Mockito.DoReturn(service).When(token1).GetService();
			Org.Mockito.Mockito.DoThrow(new IOException("boom")).When(token1).Renew(Eq(conf));
			Org.Apache.Hadoop.Security.Token.Token<object> token2 = Org.Mockito.Mockito.Mock<
				Org.Apache.Hadoop.Security.Token.Token>();
			Org.Mockito.Mockito.DoReturn(service).When(token2).GetService();
			Org.Mockito.Mockito.DoAnswer(new _Answer_117()).When(token2).Renew(Eq(conf));
			TestDelegationTokenRenewer.RenewableFileSystem fs = Org.Mockito.Mockito.Mock<TestDelegationTokenRenewer.RenewableFileSystem
				>();
			Org.Mockito.Mockito.DoReturn(conf).When(fs).GetConf();
			Org.Mockito.Mockito.DoReturn(token1).DoReturn(token2).When(fs).GetRenewToken();
			Org.Mockito.Mockito.DoReturn(token2).When(fs).GetDelegationToken(null);
			Org.Mockito.Mockito.DoAnswer(new _Answer_128(token2)).When(fs).AddDelegationTokens
				(null, null);
			renewer.AddRenewAction(fs);
			Assert.Equal(1, renewer.GetRenewQueueLength());
			Thread.Sleep(RenewCycle);
			Org.Mockito.Mockito.Verify(fs).GetRenewToken();
			Org.Mockito.Mockito.Verify(token1, Org.Mockito.Mockito.AtLeast(1)).Renew(Eq(conf)
				);
			Org.Mockito.Mockito.Verify(token1, Org.Mockito.Mockito.AtMost(2)).Renew(Eq(conf));
			Org.Mockito.Mockito.Verify(fs).AddDelegationTokens(null, null);
			Org.Mockito.Mockito.Verify(fs).SetDelegationToken(Eq(token2));
			Assert.Equal(1, renewer.GetRenewQueueLength());
			renewer.RemoveRenewAction(fs);
			Org.Mockito.Mockito.Verify(token2).Cancel(Eq(conf));
			Assert.Equal(0, renewer.GetRenewQueueLength());
		}

		private sealed class _Answer_117 : Answer<long>
		{
			public _Answer_117()
			{
			}

			public long Answer(InvocationOnMock invocation)
			{
				return Time.Now() + TestDelegationTokenRenewer.RenewCycle;
			}
		}

		private sealed class _Answer_128 : Answer<Org.Apache.Hadoop.Security.Token.Token<
			object>[]>
		{
			public _Answer_128(Org.Apache.Hadoop.Security.Token.Token<object> token2)
			{
				this.token2 = token2;
			}

			public Org.Apache.Hadoop.Security.Token.Token<object>[] Answer(InvocationOnMock invocation
				)
			{
				return new Org.Apache.Hadoop.Security.Token.Token<object>[] { token2 };
			}

			private readonly Org.Apache.Hadoop.Security.Token.Token<object> token2;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestStopRenewalWhenFsGone()
		{
			Configuration conf = Org.Mockito.Mockito.Mock<Configuration>();
			Org.Apache.Hadoop.Security.Token.Token<object> token = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Security.Token.Token
				>();
			Org.Mockito.Mockito.DoReturn(new Text("myservice")).When(token).GetService();
			Org.Mockito.Mockito.DoAnswer(new _Answer_157()).When(token).Renew(Any<Configuration
				>());
			TestDelegationTokenRenewer.RenewableFileSystem fs = Org.Mockito.Mockito.Mock<TestDelegationTokenRenewer.RenewableFileSystem
				>();
			Org.Mockito.Mockito.DoReturn(conf).When(fs).GetConf();
			Org.Mockito.Mockito.DoReturn(token).When(fs).GetRenewToken();
			renewer.AddRenewAction(fs);
			Assert.Equal(1, renewer.GetRenewQueueLength());
			Thread.Sleep(RenewCycle);
			Org.Mockito.Mockito.Verify(token, Org.Mockito.Mockito.AtLeast(1)).Renew(Eq(conf));
			Org.Mockito.Mockito.Verify(token, Org.Mockito.Mockito.AtMost(2)).Renew(Eq(conf));
			// drop weak ref
			fs = null;
			System.GC.Collect();
			System.GC.Collect();
			System.GC.Collect();
			// next renew should detect the fs as gone
			Thread.Sleep(RenewCycle);
			Org.Mockito.Mockito.Verify(token, Org.Mockito.Mockito.AtLeast(1)).Renew(Eq(conf));
			Org.Mockito.Mockito.Verify(token, Org.Mockito.Mockito.AtMost(2)).Renew(Eq(conf));
			Assert.Equal(0, renewer.GetRenewQueueLength());
		}

		private sealed class _Answer_157 : Answer<long>
		{
			public _Answer_157()
			{
			}

			public long Answer(InvocationOnMock invocation)
			{
				return Time.Now() + TestDelegationTokenRenewer.RenewCycle;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestMultipleTokensDoNotDeadlock()
		{
			Configuration conf = Org.Mockito.Mockito.Mock<Configuration>();
			FileSystem fs = Org.Mockito.Mockito.Mock<FileSystem>();
			Org.Mockito.Mockito.DoReturn(conf).When(fs).GetConf();
			long distantFuture = Time.Now() + 3600 * 1000;
			// 1h
			Org.Apache.Hadoop.Security.Token.Token<object> token1 = Org.Mockito.Mockito.Mock<
				Org.Apache.Hadoop.Security.Token.Token>();
			Org.Mockito.Mockito.DoReturn(new Text("myservice1")).When(token1).GetService();
			Org.Mockito.Mockito.DoReturn(distantFuture).When(token1).Renew(Eq(conf));
			Org.Apache.Hadoop.Security.Token.Token<object> token2 = Org.Mockito.Mockito.Mock<
				Org.Apache.Hadoop.Security.Token.Token>();
			Org.Mockito.Mockito.DoReturn(new Text("myservice2")).When(token2).GetService();
			Org.Mockito.Mockito.DoReturn(distantFuture).When(token2).Renew(Eq(conf));
			TestDelegationTokenRenewer.RenewableFileSystem fs1 = Org.Mockito.Mockito.Mock<TestDelegationTokenRenewer.RenewableFileSystem
				>();
			Org.Mockito.Mockito.DoReturn(conf).When(fs1).GetConf();
			Org.Mockito.Mockito.DoReturn(token1).When(fs1).GetRenewToken();
			TestDelegationTokenRenewer.RenewableFileSystem fs2 = Org.Mockito.Mockito.Mock<TestDelegationTokenRenewer.RenewableFileSystem
				>();
			Org.Mockito.Mockito.DoReturn(conf).When(fs2).GetConf();
			Org.Mockito.Mockito.DoReturn(token2).When(fs2).GetRenewToken();
			renewer.AddRenewAction(fs1);
			renewer.AddRenewAction(fs2);
			Assert.Equal(2, renewer.GetRenewQueueLength());
			renewer.RemoveRenewAction(fs1);
			Assert.Equal(1, renewer.GetRenewQueueLength());
			renewer.RemoveRenewAction(fs2);
			Assert.Equal(0, renewer.GetRenewQueueLength());
			Org.Mockito.Mockito.Verify(token1).Cancel(Eq(conf));
			Org.Mockito.Mockito.Verify(token2).Cancel(Eq(conf));
		}
	}
}
