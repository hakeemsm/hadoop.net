using System.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>An id which uniquely identifies an inode.</summary>
	/// <remarks>
	/// An id which uniquely identifies an inode. Id 1 to 1000 are reserved for
	/// potential future usage. The id won't be recycled and is not expected to wrap
	/// around in a very long time. Root inode id is always 1001. Id 0 is used for
	/// backward compatibility support.
	/// </remarks>
	public class INodeId : SequentialNumber
	{
		/// <summary>The last reserved inode id.</summary>
		/// <remarks>
		/// The last reserved inode id. InodeIDs are allocated from LAST_RESERVED_ID +
		/// 1.
		/// </remarks>
		public const long LastReservedId = 2 << 14 - 1;

		public const long RootInodeId = LastReservedId + 1;

		/// <summary>
		/// The inode id validation of lease check will be skipped when the request
		/// uses GRANDFATHER_INODE_ID for backward compatibility.
		/// </summary>
		public const long GrandfatherInodeId = 0;

		/// <summary>To check if the request id is the same as saved id.</summary>
		/// <remarks>
		/// To check if the request id is the same as saved id. Don't check fileId
		/// with GRANDFATHER_INODE_ID for backward compatibility.
		/// </remarks>
		/// <exception cref="System.IO.FileNotFoundException"/>
		public static void CheckId(long requestId, INode inode)
		{
			if (requestId != GrandfatherInodeId && requestId != inode.GetId())
			{
				throw new FileNotFoundException("ID mismatch. Request id and saved id: " + requestId
					 + " , " + inode.GetId() + " for file " + inode.GetFullPathName());
			}
		}

		internal INodeId()
			: base(RootInodeId)
		{
		}
	}
}
