using System.Collections.Generic;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Balancer
{
	/// <summary>
	/// This window makes sure to keep blocks that have been moved within a fixed
	/// time interval (default is 1.5 hour).
	/// </summary>
	/// <remarks>
	/// This window makes sure to keep blocks that have been moved within a fixed
	/// time interval (default is 1.5 hour). Old window has blocks that are older;
	/// Current window has blocks that are more recent; Cleanup method triggers the
	/// check if blocks in the old window are more than the fixed time interval. If
	/// yes, purge the old window and then move blocks in current window to old
	/// window.
	/// </remarks>
	/// <?/>
	public class MovedBlocks<L>
	{
		/// <summary>A class for keeping track of a block and its locations</summary>
		public class Locations<L>
		{
			private readonly Block block;

			/// <summary>The locations of the replicas of the block.</summary>
			protected internal readonly IList<L> locations = new AList<L>(3);

			public Locations(Block block)
			{
				// the block
				this.block = block;
			}

			/// <summary>clean block locations</summary>
			public virtual void ClearLocations()
			{
				lock (this)
				{
					locations.Clear();
				}
			}

			/// <summary>add a location</summary>
			public virtual void AddLocation(L loc)
			{
				lock (this)
				{
					if (!locations.Contains(loc))
					{
						locations.AddItem(loc);
					}
				}
			}

			/// <returns>if the block is located on the given location.</returns>
			public virtual bool IsLocatedOn(L loc)
			{
				lock (this)
				{
					return locations.Contains(loc);
				}
			}

			/// <returns>its locations</returns>
			public virtual IList<L> GetLocations()
			{
				lock (this)
				{
					return locations;
				}
			}

			/* @return the block */
			public virtual Block GetBlock()
			{
				return block;
			}

			/* Return the length of the block */
			public virtual long GetNumBytes()
			{
				return block.GetNumBytes();
			}
		}

		private const int CurWin = 0;

		private const int OldWin = 1;

		private const int NumWins = 2;

		private readonly long winTimeInterval;

		private long lastCleanupTime = Time.MonotonicNow();

		private readonly IList<IDictionary<Block, MovedBlocks.Locations<L>>> movedBlocks = 
			new AList<IDictionary<Block, MovedBlocks.Locations<L>>>(NumWins);

		/// <summary>initialize the moved blocks collection</summary>
		public MovedBlocks(long winTimeInterval)
		{
			this.winTimeInterval = winTimeInterval;
			movedBlocks.AddItem(NewMap());
			movedBlocks.AddItem(NewMap());
		}

		private IDictionary<Block, MovedBlocks.Locations<L>> NewMap()
		{
			return new Dictionary<Block, MovedBlocks.Locations<L>>();
		}

		/// <summary>add a block thus marking a block to be moved</summary>
		public virtual void Put(MovedBlocks.Locations<L> block)
		{
			lock (this)
			{
				movedBlocks[CurWin][block.GetBlock()] = block;
			}
		}

		/// <returns>if a block is marked as moved</returns>
		public virtual bool Contains(Block block)
		{
			lock (this)
			{
				return movedBlocks[CurWin].Contains(block) || movedBlocks[OldWin].Contains(block);
			}
		}

		/// <summary>remove old blocks</summary>
		public virtual void Cleanup()
		{
			lock (this)
			{
				long curTime = Time.MonotonicNow();
				// check if old win is older than winWidth
				if (lastCleanupTime + winTimeInterval <= curTime)
				{
					// purge the old window
					movedBlocks.Set(OldWin, movedBlocks[CurWin]);
					movedBlocks.Set(CurWin, NewMap());
					lastCleanupTime = curTime;
				}
			}
		}
	}
}
