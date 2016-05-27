using NUnit.Framework;
using Org.Apache.Curator.Framework;
using Org.Apache.Curator.Framework.Recipes.Locks;
using Org.Apache.Curator.Retry;
using Org.Apache.Curator.Test;
using Org.Apache.Curator.Utils;
using Org.Apache.Zookeeper.Data;
using Sharpen;

namespace Org.Apache.Hadoop.Util.Curator
{
	/// <summary>
	/// This is a copy of Curator 2.7.1's TestChildReaper class, with minor
	/// modifications to make it work with JUnit (some setup code taken from
	/// Curator's BaseClassForTests).
	/// </summary>
	/// <remarks>
	/// This is a copy of Curator 2.7.1's TestChildReaper class, with minor
	/// modifications to make it work with JUnit (some setup code taken from
	/// Curator's BaseClassForTests).  This is to ensure that the ChildReaper
	/// class we modified is still correct.
	/// </remarks>
	public class TestChildReaper
	{
		protected internal TestingServer server;

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			while (this.server == null)
			{
				try
				{
					this.server = new TestingServer();
				}
				catch (BindException)
				{
					System.Console.Error.WriteLine("Getting bind exception - retrying to allocate server"
						);
					this.server = null;
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void Teardown()
		{
			this.server.Close();
			this.server = null;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSomeNodes()
		{
			Timing timing = new Timing();
			ChildReaper reaper = null;
			CuratorFramework client = CuratorFrameworkFactory.NewClient(server.GetConnectString
				(), timing.Session(), timing.Connection(), new RetryOneTime(1));
			try
			{
				client.Start();
				Random r = new Random();
				int nonEmptyNodes = 0;
				for (int i = 0; i < 10; ++i)
				{
					client.Create().CreatingParentsIfNeeded().ForPath("/test/" + Sharpen.Extensions.ToString
						(i));
					if (r.NextBoolean())
					{
						client.Create().ForPath("/test/" + Sharpen.Extensions.ToString(i) + "/foo");
						++nonEmptyNodes;
					}
				}
				reaper = new ChildReaper(client, "/test", Reaper.Mode.ReapUntilDelete, 1);
				reaper.Start();
				timing.ForWaiting().SleepABit();
				Stat stat = client.CheckExists().ForPath("/test");
				NUnit.Framework.Assert.AreEqual(stat.GetNumChildren(), nonEmptyNodes);
			}
			finally
			{
				CloseableUtils.CloseQuietly(reaper);
				CloseableUtils.CloseQuietly(client);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSimple()
		{
			Timing timing = new Timing();
			ChildReaper reaper = null;
			CuratorFramework client = CuratorFrameworkFactory.NewClient(server.GetConnectString
				(), timing.Session(), timing.Connection(), new RetryOneTime(1));
			try
			{
				client.Start();
				for (int i = 0; i < 10; ++i)
				{
					client.Create().CreatingParentsIfNeeded().ForPath("/test/" + Sharpen.Extensions.ToString
						(i));
				}
				reaper = new ChildReaper(client, "/test", Reaper.Mode.ReapUntilDelete, 1);
				reaper.Start();
				timing.ForWaiting().SleepABit();
				Stat stat = client.CheckExists().ForPath("/test");
				NUnit.Framework.Assert.AreEqual(stat.GetNumChildren(), 0);
			}
			finally
			{
				CloseableUtils.CloseQuietly(reaper);
				CloseableUtils.CloseQuietly(client);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultiPath()
		{
			Timing timing = new Timing();
			ChildReaper reaper = null;
			CuratorFramework client = CuratorFrameworkFactory.NewClient(server.GetConnectString
				(), timing.Session(), timing.Connection(), new RetryOneTime(1));
			try
			{
				client.Start();
				for (int i = 0; i < 10; ++i)
				{
					client.Create().CreatingParentsIfNeeded().ForPath("/test1/" + Sharpen.Extensions.ToString
						(i));
					client.Create().CreatingParentsIfNeeded().ForPath("/test2/" + Sharpen.Extensions.ToString
						(i));
					client.Create().CreatingParentsIfNeeded().ForPath("/test3/" + Sharpen.Extensions.ToString
						(i));
				}
				reaper = new ChildReaper(client, "/test2", Reaper.Mode.ReapUntilDelete, 1);
				reaper.Start();
				reaper.AddPath("/test1");
				timing.ForWaiting().SleepABit();
				Stat stat = client.CheckExists().ForPath("/test1");
				NUnit.Framework.Assert.AreEqual(stat.GetNumChildren(), 0);
				stat = client.CheckExists().ForPath("/test2");
				NUnit.Framework.Assert.AreEqual(stat.GetNumChildren(), 0);
				stat = client.CheckExists().ForPath("/test3");
				NUnit.Framework.Assert.AreEqual(stat.GetNumChildren(), 10);
			}
			finally
			{
				CloseableUtils.CloseQuietly(reaper);
				CloseableUtils.CloseQuietly(client);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNamespace()
		{
			Timing timing = new Timing();
			ChildReaper reaper = null;
			CuratorFramework client = CuratorFrameworkFactory.Builder().ConnectString(server.
				GetConnectString()).SessionTimeoutMs(timing.Session()).ConnectionTimeoutMs(timing
				.Connection()).RetryPolicy(new RetryOneTime(1)).Namespace("foo").Build();
			try
			{
				client.Start();
				for (int i = 0; i < 10; ++i)
				{
					client.Create().CreatingParentsIfNeeded().ForPath("/test/" + Sharpen.Extensions.ToString
						(i));
				}
				reaper = new ChildReaper(client, "/test", Reaper.Mode.ReapUntilDelete, 1);
				reaper.Start();
				timing.ForWaiting().SleepABit();
				Stat stat = client.CheckExists().ForPath("/test");
				NUnit.Framework.Assert.AreEqual(stat.GetNumChildren(), 0);
				stat = client.UsingNamespace(null).CheckExists().ForPath("/foo/test");
				NUnit.Framework.Assert.IsNotNull(stat);
				NUnit.Framework.Assert.AreEqual(stat.GetNumChildren(), 0);
			}
			finally
			{
				CloseableUtils.CloseQuietly(reaper);
				CloseableUtils.CloseQuietly(client);
			}
		}
	}
}
