using Sharpen;

namespace org.apache.hadoop.fs
{
	public class TestDelegateToFileSystem
	{
		private const string FTP_DUMMYHOST = "ftp://dummyhost";

		private static readonly java.net.URI FTP_URI_NO_PORT = java.net.URI.create(FTP_DUMMYHOST
			);

		private static readonly java.net.URI FTP_URI_WITH_PORT = java.net.URI.create(FTP_DUMMYHOST
			 + ":" + org.apache.commons.net.ftp.FTP.DEFAULT_PORT);

		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		private void testDefaultUriInternal(string defaultUri)
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.fs.FileSystem.setDefaultUri(conf, defaultUri);
			org.apache.hadoop.fs.AbstractFileSystem ftpFs = org.apache.hadoop.fs.AbstractFileSystem
				.get(FTP_URI_NO_PORT, conf);
			NUnit.Framework.Assert.AreEqual(FTP_URI_WITH_PORT, ftpFs.getUri());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testDefaultURIwithOutPort()
		{
			testDefaultUriInternal("hdfs://dummyhost");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testDefaultURIwithPort()
		{
			testDefaultUriInternal("hdfs://dummyhost:8020");
		}
	}
}
