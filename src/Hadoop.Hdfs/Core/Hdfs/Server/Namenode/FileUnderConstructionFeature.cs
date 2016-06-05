using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Feature for under-construction file.</summary>
	public class FileUnderConstructionFeature : INode.Feature
	{
		private string clientName;

		private readonly string clientMachine;

		public FileUnderConstructionFeature(string clientName, string clientMachine)
		{
			// lease holder
			this.clientName = clientName;
			this.clientMachine = clientMachine;
		}

		public virtual string GetClientName()
		{
			return clientName;
		}

		internal virtual void SetClientName(string clientName)
		{
			this.clientName = clientName;
		}

		public virtual string GetClientMachine()
		{
			return clientMachine;
		}

		/// <summary>Update the length for the last block</summary>
		/// <param name="lastBlockLength">The length of the last block reported from client</param>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void UpdateLengthOfLastBlock(INodeFile f, long lastBlockLength)
		{
			BlockInfoContiguous lastBlock = f.GetLastBlock();
			System.Diagnostics.Debug.Assert((lastBlock != null), "The last block for path " +
				 f.GetFullPathName() + " is null when updating its length");
			System.Diagnostics.Debug.Assert((lastBlock is BlockInfoContiguousUnderConstruction
				), "The last block for path " + f.GetFullPathName() + " is not a BlockInfoUnderConstruction when updating its length"
				);
			lastBlock.SetNumBytes(lastBlockLength);
		}

		/// <summary>
		/// When deleting a file in the current fs directory, and the file is contained
		/// in a snapshot, we should delete the last block if it's under construction
		/// and its size is 0.
		/// </summary>
		internal virtual void CleanZeroSizeBlock(INodeFile f, INode.BlocksMapUpdateInfo collectedBlocks
			)
		{
			BlockInfoContiguous[] blocks = f.GetBlocks();
			if (blocks != null && blocks.Length > 0 && blocks[blocks.Length - 1] is BlockInfoContiguousUnderConstruction)
			{
				BlockInfoContiguousUnderConstruction lastUC = (BlockInfoContiguousUnderConstruction
					)blocks[blocks.Length - 1];
				if (lastUC.GetNumBytes() == 0)
				{
					// this is a 0-sized block. do not need check its UC state here
					collectedBlocks.AddDeleteBlock(lastUC);
					f.RemoveLastBlock(lastUC);
				}
			}
		}
	}
}
