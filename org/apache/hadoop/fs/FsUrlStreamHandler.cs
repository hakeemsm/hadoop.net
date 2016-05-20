using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>
	/// URLStream handler relying on FileSystem and on a given Configuration to
	/// handle URL protocols.
	/// </summary>
	internal class FsUrlStreamHandler : java.net.URLStreamHandler
	{
		private org.apache.hadoop.conf.Configuration conf;

		internal FsUrlStreamHandler(org.apache.hadoop.conf.Configuration conf)
		{
			this.conf = conf;
		}

		internal FsUrlStreamHandler()
		{
			this.conf = new org.apache.hadoop.conf.Configuration();
		}

		/// <exception cref="System.IO.IOException"/>
		protected override java.net.URLConnection openConnection(java.net.URL url)
		{
			return new org.apache.hadoop.fs.FsUrlConnection(conf, url);
		}
	}
}
