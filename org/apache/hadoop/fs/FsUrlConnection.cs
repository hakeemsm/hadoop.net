using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>Representation of a URL connection to open InputStreams.</summary>
	internal class FsUrlConnection : java.net.URLConnection
	{
		private org.apache.hadoop.conf.Configuration conf;

		private java.io.InputStream @is;

		internal FsUrlConnection(org.apache.hadoop.conf.Configuration conf, java.net.URL 
			url)
			: base(url)
		{
			this.conf = conf;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void connect()
		{
			try
			{
				org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(url.toURI
					(), conf);
				@is = fs.open(new org.apache.hadoop.fs.Path(url.getPath()));
			}
			catch (java.net.URISyntaxException e)
			{
				throw new System.IO.IOException(e.ToString());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override java.io.InputStream getInputStream()
		{
			if (@is == null)
			{
				connect();
			}
			return @is;
		}
	}
}
