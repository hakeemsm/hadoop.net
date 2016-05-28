using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Common
{
	/// <summary>
	/// This is a unit test, which tests
	/// <see cref="Util.StringAsURI(string)"/>
	/// for Windows and Unix style file paths.
	/// </summary>
	public class TestGetUriFromString
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestGetUriFromString));

		private const string RelativeFilePath = "relativeFilePath";

		private const string AbsolutePathUnix = "/tmp/file1";

		private const string AbsolutePathWindows = "C:\\Documents and Settings\\All Users";

		private const string UriFileSchema = "file";

		private const string UriPathUnix = "/var/www";

		private const string UriPathWindows = "/C:/Documents%20and%20Settings/All%20Users";

		private const string UriUnix = UriFileSchema + "://" + UriPathUnix;

		private const string UriWindows = UriFileSchema + "://" + UriPathWindows;

		/// <summary>Test for a relative path, os independent</summary>
		/// <exception cref="System.IO.IOException"></exception>
		[NUnit.Framework.Test]
		public virtual void TestRelativePathAsURI()
		{
			URI u = Util.StringAsURI(RelativeFilePath);
			Log.Info("Uri: " + u);
			NUnit.Framework.Assert.IsNotNull(u);
		}

		/// <summary>Test for an OS dependent absolute paths.</summary>
		/// <exception cref="System.IO.IOException"></exception>
		[NUnit.Framework.Test]
		public virtual void TestAbsolutePathAsURI()
		{
			URI u = null;
			u = Util.StringAsURI(AbsolutePathWindows);
			NUnit.Framework.Assert.IsNotNull("Uri should not be null for Windows path" + AbsolutePathWindows
				, u);
			NUnit.Framework.Assert.AreEqual(UriFileSchema, u.GetScheme());
			u = Util.StringAsURI(AbsolutePathUnix);
			NUnit.Framework.Assert.IsNotNull("Uri should not be null for Unix path" + AbsolutePathUnix
				, u);
			NUnit.Framework.Assert.AreEqual(UriFileSchema, u.GetScheme());
		}

		/// <summary>Test for a URI</summary>
		/// <exception cref="System.IO.IOException"></exception>
		[NUnit.Framework.Test]
		public virtual void TestURI()
		{
			Log.Info("Testing correct Unix URI: " + UriUnix);
			URI u = Util.StringAsURI(UriUnix);
			Log.Info("Uri: " + u);
			NUnit.Framework.Assert.IsNotNull("Uri should not be null at this point", u);
			NUnit.Framework.Assert.AreEqual(UriFileSchema, u.GetScheme());
			NUnit.Framework.Assert.AreEqual(UriPathUnix, u.GetPath());
			Log.Info("Testing correct windows URI: " + UriWindows);
			u = Util.StringAsURI(UriWindows);
			Log.Info("Uri: " + u);
			NUnit.Framework.Assert.IsNotNull("Uri should not be null at this point", u);
			NUnit.Framework.Assert.AreEqual(UriFileSchema, u.GetScheme());
			NUnit.Framework.Assert.AreEqual(UriPathWindows.Replace("%20", " "), u.GetPath());
		}
	}
}
