using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>Class to contain Lastblock and HdfsFileStatus for the Append operation</summary>
	public class LastBlockWithStatus
	{
		private readonly LocatedBlock lastBlock;

		private readonly HdfsFileStatus fileStatus;

		public LastBlockWithStatus(LocatedBlock lastBlock, HdfsFileStatus fileStatus)
		{
			this.lastBlock = lastBlock;
			this.fileStatus = fileStatus;
		}

		public virtual LocatedBlock GetLastBlock()
		{
			return lastBlock;
		}

		public virtual HdfsFileStatus GetFileStatus()
		{
			return fileStatus;
		}
	}
}
