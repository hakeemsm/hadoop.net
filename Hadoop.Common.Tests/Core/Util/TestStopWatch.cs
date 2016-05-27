using System;
using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	public class TestStopWatch
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStartAndStop()
		{
			using (StopWatch sw = new StopWatch())
			{
				NUnit.Framework.Assert.IsFalse(sw.IsRunning());
				sw.Start();
				NUnit.Framework.Assert.IsTrue(sw.IsRunning());
				sw.Stop();
				NUnit.Framework.Assert.IsFalse(sw.IsRunning());
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStopInTryWithResource()
		{
			using (StopWatch sw = new StopWatch())
			{
			}
		}

		// make sure that no exception is thrown.
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestExceptions()
		{
			StopWatch sw = new StopWatch();
			try
			{
				sw.Stop();
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.IsTrue("IllegalStateException is expected", e is InvalidOperationException
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
				NUnit.Framework.Assert.IsTrue("IllegalStateException is expected", e is InvalidOperationException
					);
			}
		}
	}
}
