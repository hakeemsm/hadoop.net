using System.Collections.Generic;
using System.Text;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>Collection of blocks with their locations and the file length.</summary>
	public class LocatedBlocks
	{
		private readonly long fileLength;

		private readonly IList<LocatedBlock> blocks;

		private readonly bool underConstruction;

		private readonly LocatedBlock lastLocatedBlock;

		private readonly bool isLastBlockComplete;

		private readonly FileEncryptionInfo fileEncryptionInfo;

		public LocatedBlocks()
		{
			// array of blocks with prioritized locations
			fileLength = 0;
			blocks = null;
			underConstruction = false;
			lastLocatedBlock = null;
			isLastBlockComplete = false;
			fileEncryptionInfo = null;
		}

		public LocatedBlocks(long flength, bool isUnderConstuction, IList<LocatedBlock> blks
			, LocatedBlock lastBlock, bool isLastBlockCompleted, FileEncryptionInfo feInfo)
		{
			fileLength = flength;
			blocks = blks;
			underConstruction = isUnderConstuction;
			this.lastLocatedBlock = lastBlock;
			this.isLastBlockComplete = isLastBlockCompleted;
			this.fileEncryptionInfo = feInfo;
		}

		/// <summary>Get located blocks.</summary>
		public virtual IList<LocatedBlock> GetLocatedBlocks()
		{
			return blocks;
		}

		/// <summary>Get the last located block.</summary>
		public virtual LocatedBlock GetLastLocatedBlock()
		{
			return lastLocatedBlock;
		}

		/// <summary>Is the last block completed?</summary>
		public virtual bool IsLastBlockComplete()
		{
			return isLastBlockComplete;
		}

		/// <summary>Get located block.</summary>
		public virtual LocatedBlock Get(int index)
		{
			return blocks[index];
		}

		/// <summary>Get number of located blocks.</summary>
		public virtual int LocatedBlockCount()
		{
			return blocks == null ? 0 : blocks.Count;
		}

		public virtual long GetFileLength()
		{
			return this.fileLength;
		}

		/// <summary>
		/// Return true if file was under construction when this LocatedBlocks was
		/// constructed, false otherwise.
		/// </summary>
		public virtual bool IsUnderConstruction()
		{
			return underConstruction;
		}

		/// <returns>the FileEncryptionInfo for the LocatedBlocks</returns>
		public virtual FileEncryptionInfo GetFileEncryptionInfo()
		{
			return fileEncryptionInfo;
		}

		/// <summary>Find block containing specified offset.</summary>
		/// <returns>block if found, or null otherwise.</returns>
		public virtual int FindBlock(long offset)
		{
			// create fake block of size 0 as a key
			LocatedBlock key = new LocatedBlock(new ExtendedBlock(), new DatanodeInfo[0], 0L, 
				false);
			key.SetStartOffset(offset);
			key.GetBlock().SetNumBytes(1);
			IComparer<LocatedBlock> comp = new _IComparer_126();
			// Returns 0 iff a is inside b or b is inside a
			// one of the blocks is inside the other
			// a's left bound is to the left of the b's
			return Sharpen.Collections.BinarySearch(blocks, key, comp);
		}

		private sealed class _IComparer_126 : IComparer<LocatedBlock>
		{
			public _IComparer_126()
			{
			}

			public int Compare(LocatedBlock a, LocatedBlock b)
			{
				long aBeg = a.GetStartOffset();
				long bBeg = b.GetStartOffset();
				long aEnd = aBeg + a.GetBlockSize();
				long bEnd = bBeg + b.GetBlockSize();
				if (aBeg <= bBeg && bEnd <= aEnd || bBeg <= aBeg && aEnd <= bEnd)
				{
					return 0;
				}
				if (aBeg < bBeg)
				{
					return -1;
				}
				return 1;
			}
		}

		public virtual void InsertRange(int blockIdx, IList<LocatedBlock> newBlocks)
		{
			int oldIdx = blockIdx;
			int insStart = 0;
			int insEnd = 0;
			for (int newIdx = 0; newIdx < newBlocks.Count && oldIdx < blocks.Count; newIdx++)
			{
				long newOff = newBlocks[newIdx].GetStartOffset();
				long oldOff = blocks[oldIdx].GetStartOffset();
				if (newOff < oldOff)
				{
					insEnd++;
				}
				else
				{
					if (newOff == oldOff)
					{
						// replace old cached block by the new one
						blocks.Set(oldIdx, newBlocks[newIdx]);
						if (insStart < insEnd)
						{
							// insert new blocks
							blocks.AddRange(oldIdx, newBlocks.SubList(insStart, insEnd));
							oldIdx += insEnd - insStart;
						}
						insStart = insEnd = newIdx + 1;
						oldIdx++;
					}
					else
					{
						// newOff > oldOff
						System.Diagnostics.Debug.Assert(false, "List of LocatedBlock must be sorted by startOffset"
							);
					}
				}
			}
			insEnd = newBlocks.Count;
			if (insStart < insEnd)
			{
				// insert new blocks
				blocks.AddRange(oldIdx, newBlocks.SubList(insStart, insEnd));
			}
		}

		public static int GetInsertIndex(int binSearchResult)
		{
			return binSearchResult >= 0 ? binSearchResult : -(binSearchResult + 1);
		}

		public override string ToString()
		{
			StringBuilder b = new StringBuilder(GetType().Name);
			b.Append("{").Append("\n  fileLength=").Append(fileLength).Append("\n  underConstruction="
				).Append(underConstruction).Append("\n  blocks=").Append(blocks).Append("\n  lastLocatedBlock="
				).Append(lastLocatedBlock).Append("\n  isLastBlockComplete=").Append(isLastBlockComplete
				).Append("}");
			return b.ToString();
		}
	}
}
