using Hadoop.Common.Core.Conf;
using Org.Apache.Hadoop.Conf;


namespace Org.Apache.Hadoop.FS
{
	public class HarFs : DelegateToFileSystem
	{
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="URISyntaxException"/>
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
