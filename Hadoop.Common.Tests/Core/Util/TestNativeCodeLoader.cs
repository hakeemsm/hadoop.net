using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Crypto;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.IO.Compress.Zlib;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	public class TestNativeCodeLoader
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(TestNativeCodeLoader)
			);

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
		public virtual void TestNativeCodeLoaded()
		{
			if (RequireTestJni() == false)
			{
				Log.Info("TestNativeCodeLoader: libhadoop.so testing is not required.");
				return;
			}
			if (!NativeCodeLoader.IsNativeCodeLoaded())
			{
				NUnit.Framework.Assert.Fail("TestNativeCodeLoader: libhadoop.so testing was required, but "
					 + "libhadoop.so was not loaded.");
			}
			NUnit.Framework.Assert.IsFalse(NativeCodeLoader.GetLibraryName().IsEmpty());
			// library names are depended on platform and build envs
			// so just check names are available
			NUnit.Framework.Assert.IsFalse(ZlibFactory.GetLibraryName().IsEmpty());
			if (NativeCodeLoader.BuildSupportsSnappy())
			{
				NUnit.Framework.Assert.IsFalse(SnappyCodec.GetLibraryName().IsEmpty());
			}
			if (NativeCodeLoader.BuildSupportsOpenssl())
			{
				NUnit.Framework.Assert.IsFalse(OpensslCipher.GetLibraryName().IsEmpty());
			}
			NUnit.Framework.Assert.IsFalse(Lz4Codec.GetLibraryName().IsEmpty());
			Log.Info("TestNativeCodeLoader: libhadoop.so is loaded.");
		}
	}
}
