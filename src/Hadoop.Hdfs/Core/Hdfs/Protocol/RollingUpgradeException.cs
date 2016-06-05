using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>Exception related to rolling upgrade.</summary>
	[System.Serializable]
	public class RollingUpgradeException : IOException
	{
		private const long serialVersionUID = 1L;

		public RollingUpgradeException(string msg)
			: base(msg)
		{
		}
	}
}
