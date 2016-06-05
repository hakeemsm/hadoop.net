using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	[System.Serializable]
	public class BPServiceActorActionException : IOException
	{
		/// <summary>An exception for BPSerivceActor call related issues</summary>
		private const long serialVersionUID = 1L;

		public BPServiceActorActionException(string message)
			: base(message)
		{
		}
	}
}
