using System;
using NUnit.Framework;


namespace Org.Apache.Hadoop.Util
{
	public class TestStopWatch
	{
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestStartAndStop()
		{
			using (StopWatch sw = new StopWatch())
			{
				NUnit.Framework.Assert.IsFalse(sw.IsRunning());
				sw.Start();
				Assert.True(sw.IsRunning());
				sw.Stop();
				NUnit.Framework.Assert.IsFalse(sw.IsRunning());
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestStopInTryWithResource()
		{
			using (StopWatch sw = new StopWatch())
			{
			}
		}

		// make sure that no exception is thrown.
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestExceptions()
		{
			StopWatch sw = new StopWatch();
			try
			{
				sw.Stop();
			}
			catch (Exception e)
			{
				Assert.True("IllegalStateException is expected", e is InvalidOperationException
					);
			}
			sw.Reset();
			sw.Start();
			try
			{
				sw.Start();
			}
			catch (Exception e)
			{
				Assert.True("IllegalStateException is expected", e is InvalidOperationException
					);
			}
		}
	}
}
