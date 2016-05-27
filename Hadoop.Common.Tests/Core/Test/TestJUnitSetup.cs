using System;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Test
{
	public class TestJUnitSetup
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestJUnitSetup));

		[NUnit.Framework.Test]
		public virtual void TestJavaAssert()
		{
			try
			{
				System.Diagnostics.Debug.Assert(false, "Good! Java assert is on.");
			}
			catch (Exception ae)
			{
				Log.Info("The AssertionError is expected.", ae);
				return;
			}
			NUnit.Framework.Assert.Fail("Java assert does not work.");
		}
	}
}
