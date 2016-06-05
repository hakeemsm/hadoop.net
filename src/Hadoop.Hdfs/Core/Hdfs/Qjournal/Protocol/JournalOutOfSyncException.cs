using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal.Protocol
{
	[System.Serializable]
	public class JournalOutOfSyncException : IOException
	{
		private const long serialVersionUID = 1L;

		public JournalOutOfSyncException(string msg)
			: base(msg)
		{
		}
	}
}
