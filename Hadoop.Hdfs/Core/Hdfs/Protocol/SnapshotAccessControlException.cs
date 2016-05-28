using System;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>Snapshot access related exception.</summary>
	[System.Serializable]
	public class SnapshotAccessControlException : AccessControlException
	{
		private const long serialVersionUID = 1L;

		public SnapshotAccessControlException(string message)
			: base(message)
		{
		}

		public SnapshotAccessControlException(Exception cause)
			: base(cause)
		{
		}
	}
}
