using System;
using NUnit.Framework;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	public class TestSignalLogger
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestSignalLogger));

		/// <exception cref="System.Exception"/>
		public virtual void TestInstall()
		{
			Assume.AssumeTrue(SystemUtils.IsOsUnix);
			SignalLogger.Instance.Register(Log);
			try
			{
				SignalLogger.Instance.Register(Log);
				NUnit.Framework.Assert.Fail("expected IllegalStateException from double registration"
					);
			}
			catch (InvalidOperationException)
			{
			}
		}
		// fall through
	}
}
