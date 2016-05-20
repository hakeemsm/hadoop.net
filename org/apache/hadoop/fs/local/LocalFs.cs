using Sharpen;

namespace org.apache.hadoop.fs.local
{
	/// <summary>The LocalFs implementation of ChecksumFs.</summary>
	public class LocalFs : org.apache.hadoop.fs.ChecksumFs
	{
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.net.URISyntaxException"/>
		internal LocalFs(org.apache.hadoop.conf.Configuration conf)
			: base(new org.apache.hadoop.fs.local.RawLocalFs(conf))
		{
		}

		/// <summary>
		/// This constructor has the signature needed by
		/// <see cref="org.apache.hadoop.fs.AbstractFileSystem.createFileSystem(java.net.URI, org.apache.hadoop.conf.Configuration)
		/// 	"/>
		/// .
		/// </summary>
		/// <param name="theUri">which must be that of localFs</param>
		/// <param name="conf"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.net.URISyntaxException"></exception>
		internal LocalFs(java.net.URI theUri, org.apache.hadoop.conf.Configuration conf)
			: this(conf)
		{
		}
		/*Evolving for a release,to be changed to Stable */
	}
}
