using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>
	/// The exception that happens when you ask to create a file that already
	/// is being created, but is not closed yet.
	/// </summary>
	[System.Serializable]
	public class AlreadyBeingCreatedException : IOException
	{
		internal const long serialVersionUID = unchecked((long)(0x12308AD009L));

		public AlreadyBeingCreatedException(string msg)
			: base(msg)
		{
		}
	}
}
