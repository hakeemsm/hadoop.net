using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	/// <summary>
	/// If a previous user of a resource tries to use a shared resource, after
	/// fenced by another user, this exception is thrown.
	/// </summary>
	[System.Serializable]
	public class FencedException : IOException
	{
		private const long serialVersionUID = 1L;

		public FencedException(string errorMsg)
			: base(errorMsg)
		{
		}
	}
}
