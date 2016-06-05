using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>
	/// This exception is thrown when the name node runs out of V1 generation
	/// stamps.
	/// </summary>
	[System.Serializable]
	public class OutOfV1GenerationStampsException : IOException
	{
		private const long serialVersionUID = 1L;

		public OutOfV1GenerationStampsException()
			: base("Out of V1 (legacy) generation stamps\n")
		{
		}
	}
}
