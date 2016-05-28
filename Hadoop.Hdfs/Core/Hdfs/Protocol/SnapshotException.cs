using System;
using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>Snapshot related exception.</summary>
	[System.Serializable]
	public class SnapshotException : IOException
	{
		private const long serialVersionUID = 1L;

		public SnapshotException(string message)
			: base(message)
		{
		}

		public SnapshotException(Exception cause)
			: base(cause)
		{
		}
	}
}
