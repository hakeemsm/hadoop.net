using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>The file has not finished being written to enough datanodes yet.</summary>
	[System.Serializable]
	public class NotReplicatedYetException : IOException
	{
		private const long serialVersionUID = 1L;

		public NotReplicatedYetException(string msg)
			: base(msg)
		{
		}
	}
}
