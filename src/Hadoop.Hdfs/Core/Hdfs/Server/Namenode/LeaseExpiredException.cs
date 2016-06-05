using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>The lease that was being used to create this file has expired.</summary>
	[System.Serializable]
	public class LeaseExpiredException : IOException
	{
		private const long serialVersionUID = 1L;

		public LeaseExpiredException(string msg)
			: base(msg)
		{
		}
	}
}
