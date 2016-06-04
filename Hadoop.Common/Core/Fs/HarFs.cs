using Hadoop.Common.Core.Conf;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	public class HarFs : DelegateToFileSystem
	{
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		internal HarFs(URI theUri, Configuration conf)
			: base(theUri, new HarFileSystem(), conf, "har", false)
		{
		}

		public override int GetUriDefaultPort()
		{
			return -1;
		}
	}
}
