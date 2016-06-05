using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	public class TestHdfsNativeCodeLoader
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(TestHdfsNativeCodeLoader
			));

		private static bool RequireTestJni()
		{
			string rtj = Runtime.GetProperty("require.test.libhadoop");
			if (rtj == null)
			{
				return false;
			}
			if (rtj.CompareToIgnoreCase("false") == 0)
			{
				return false;
			}
			return true;
		}

		[NUnit.Framework.Test]
		[Ignore]
		public virtual void TestNativeCodeLoaded()
		{
			if (RequireTestJni() == false)
			{
				Log.Info("TestNativeCodeLoader: libhadoop.so testing is not required.");
				return;
			}
			if (!NativeCodeLoader.IsNativeCodeLoaded())
			{
				string LdLibraryPath = Sharpen.Runtime.GetEnv()["LD_LIBRARY_PATH"];
				if (LdLibraryPath == null)
				{
					LdLibraryPath = string.Empty;
				}
				NUnit.Framework.Assert.Fail("TestNativeCodeLoader: libhadoop.so testing was required, but "
					 + "libhadoop.so was not loaded.  LD_LIBRARY_PATH = " + LdLibraryPath);
			}
			Log.Info("TestHdfsNativeCodeLoader: libhadoop.so is loaded.");
		}
	}
}
