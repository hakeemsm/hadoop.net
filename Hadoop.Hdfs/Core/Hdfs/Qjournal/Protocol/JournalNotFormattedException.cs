using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal.Protocol
{
	/// <summary>
	/// Exception indicating that a call has been made to a JournalNode
	/// which is not yet formatted.
	/// </summary>
	[System.Serializable]
	public class JournalNotFormattedException : IOException
	{
		private const long serialVersionUID = 1L;

		public JournalNotFormattedException(string msg)
			: base(msg)
		{
		}
	}
}
