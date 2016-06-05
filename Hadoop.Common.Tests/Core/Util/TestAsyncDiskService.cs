using System;
using NUnit.Framework;
using Org.Apache.Commons.Logging;


namespace Org.Apache.Hadoop.Util
{
	/// <summary>A test for AsyncDiskService.</summary>
	public class TestAsyncDiskService : TestCase
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Util.TestAsyncDiskService
			));

		internal volatile int count;

		/// <summary>An example task for incrementing a counter.</summary>
		internal class ExampleTask : Runnable
		{
			internal ExampleTask(TestAsyncDiskService _enclosing)
			{
				this._enclosing = _enclosing;
			}

			// Access by multiple threads from the ThreadPools in AsyncDiskService.
			public virtual void Run()
			{
				lock (this._enclosing)
				{
					this._enclosing.count++;
				}
			}

			private readonly TestAsyncDiskService _enclosing;
		}

		/// <summary>This test creates some ExampleTasks and runs them.</summary>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestAsyncDiskService()
		{
			string[] vols = new string[] { "/0", "/1" };
			AsyncDiskService service = new AsyncDiskService(vols);
			int total = 100;
			for (int i = 0; i < total; i++)
			{
				service.Execute(vols[i % 2], new TestAsyncDiskService.ExampleTask(this));
			}
			Exception e = null;
			try
			{
				service.Execute("no_such_volume", new TestAsyncDiskService.ExampleTask(this));
			}
			catch (RuntimeException ex)
			{
				e = ex;
			}
			NUnit.Framework.Assert.IsNotNull("Executing a task on a non-existing volume should throw an "
				 + "Exception.", e);
			service.Shutdown();
			if (!service.AwaitTermination(5000))
			{
				Fail("AsyncDiskService didn't shutdown in 5 seconds.");
			}
			Assert.Equal(total, count);
		}
	}
}
