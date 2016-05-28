using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	[System.Serializable]
	public class SaveNamespaceCancelledException : IOException
	{
		private const long serialVersionUID = 1L;

		internal SaveNamespaceCancelledException(string cancelReason)
			: base(cancelReason)
		{
		}
	}
}
