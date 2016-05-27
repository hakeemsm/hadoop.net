using System;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>
	/// URLStream handler relying on FileSystem and on a given Configuration to
	/// handle URL protocols.
	/// </summary>
	internal class FsUrlStreamHandler : URLStreamHandler
	{
		private Configuration conf;

		internal FsUrlStreamHandler(Configuration conf)
		{
			this.conf = conf;
		}

		internal FsUrlStreamHandler()
		{
			this.conf = new Configuration();
		}

		/// <exception cref="System.IO.IOException"/>
		protected override URLConnection OpenConnection(Uri url)
		{
			return new FsUrlConnection(conf, url);
		}
	}
}
