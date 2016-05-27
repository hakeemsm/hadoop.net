using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Local
{
	/// <summary>The LocalFs implementation of ChecksumFs.</summary>
	public class LocalFs : ChecksumFs
	{
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		internal LocalFs(Configuration conf)
			: base(new RawLocalFs(conf))
		{
		}

		/// <summary>
		/// This constructor has the signature needed by
		/// <see cref="Org.Apache.Hadoop.FS.AbstractFileSystem.CreateFileSystem(Sharpen.URI, Org.Apache.Hadoop.Conf.Configuration)
		/// 	"/>
		/// .
		/// </summary>
		/// <param name="theUri">which must be that of localFs</param>
		/// <param name="conf"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"></exception>
		internal LocalFs(URI theUri, Configuration conf)
			: this(conf)
		{
		}
		/*Evolving for a release,to be changed to Stable */
	}
}
