using System;
using System.IO;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.Fs;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Representation of a URL connection to open InputStreams.</summary>
	internal class FsUrlConnection : URLConnection
	{
		private Configuration conf;

		private InputStream @is;

		internal FsUrlConnection(Configuration conf, Uri url)
			: base(url)
		{
			this.conf = conf;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Connect()
		{
			try
			{
				FileSystem fs = FileSystem.Get(url.ToURI(), conf);
				@is = fs.Open(new Path(url.AbsolutePath));
			}
			catch (URISyntaxException e)
			{
				throw new IOException(e.ToString());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override InputStream GetInputStream()
		{
			if (@is == null)
			{
				Connect();
			}
			return @is;
		}
	}
}
