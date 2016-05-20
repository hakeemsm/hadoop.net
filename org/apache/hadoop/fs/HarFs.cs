using Sharpen;

namespace org.apache.hadoop.fs
{
	public class HarFs : org.apache.hadoop.fs.DelegateToFileSystem
	{
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.net.URISyntaxException"/>
		internal HarFs(java.net.URI theUri, org.apache.hadoop.conf.Configuration conf)
			: base(theUri, new org.apache.hadoop.fs.HarFileSystem(), conf, "har", false)
		{
		}

		public override int getUriDefaultPort()
		{
			return -1;
		}
	}
}
