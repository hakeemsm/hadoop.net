using NUnit.Framework;
using Org.Apache.Commons.Net.Ftp;
using Org.Apache.Hadoop.Conf;


namespace Org.Apache.Hadoop.FS
{
	public class TestDelegateToFileSystem
	{
		private const string FtpDummyhost = "ftp://dummyhost";

		private static readonly URI FtpUriNoPort = URI.Create(FtpDummyhost);

		private static readonly URI FtpUriWithPort = URI.Create(FtpDummyhost + ":" + FTP.
			DefaultPort);

		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		private void TestDefaultUriInternal(string defaultUri)
		{
			Configuration conf = new Configuration();
			FileSystem.SetDefaultUri(conf, defaultUri);
			AbstractFileSystem ftpFs = AbstractFileSystem.Get(FtpUriNoPort, conf);
			Assert.Equal(FtpUriWithPort, ftpFs.GetUri());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestDefaultURIwithOutPort()
		{
			TestDefaultUriInternal("hdfs://dummyhost");
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestDefaultURIwithPort()
		{
			TestDefaultUriInternal("hdfs://dummyhost:8020");
		}
	}
}
