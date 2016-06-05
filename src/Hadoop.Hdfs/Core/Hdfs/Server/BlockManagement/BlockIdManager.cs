using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>BlockIdManager allocates the generation stamps and the block ID.</summary>
	/// <remarks>
	/// BlockIdManager allocates the generation stamps and the block ID. The
	/// <seealso>FSNamesystem</seealso>
	/// is responsible for persisting the allocations in the
	/// <seealso>EditLog</seealso>
	/// .
	/// </remarks>
	public class BlockIdManager
	{
		/// <summary>
		/// The global generation stamp for legacy blocks with randomly
		/// generated block IDs.
		/// </summary>
		private readonly GenerationStamp generationStampV1 = new GenerationStamp();

		/// <summary>The global generation stamp for this file system.</summary>
		private readonly GenerationStamp generationStampV2 = new GenerationStamp();

		/// <summary>
		/// The value of the generation stamp when the first switch to sequential
		/// block IDs was made.
		/// </summary>
		/// <remarks>
		/// The value of the generation stamp when the first switch to sequential
		/// block IDs was made. Blocks with generation stamps below this value
		/// have randomly allocated block IDs. Blocks with generation stamps above
		/// this value had sequentially allocated block IDs. Read from the fsImage
		/// (or initialized as an offset from the V1 (legacy) generation stamp on
		/// upgrade).
		/// </remarks>
		private long generationStampV1Limit;

		/// <summary>The global block ID space for this file system.</summary>
		private readonly SequentialBlockIdGenerator blockIdGenerator;

		public BlockIdManager(BlockManager blockManager)
		{
			this.generationStampV1Limit = GenerationStamp.GrandfatherGenerationStamp;
			this.blockIdGenerator = new SequentialBlockIdGenerator(blockManager);
		}

		/// <summary>
		/// Upgrades the generation stamp for the filesystem
		/// by reserving a sufficient range for all existing blocks.
		/// </summary>
		/// <remarks>
		/// Upgrades the generation stamp for the filesystem
		/// by reserving a sufficient range for all existing blocks.
		/// Should be invoked only during the first upgrade to
		/// sequential block IDs.
		/// </remarks>
		public virtual long UpgradeGenerationStampToV2()
		{
			Preconditions.CheckState(generationStampV2.GetCurrentValue() == GenerationStamp.LastReservedStamp
				);
			generationStampV2.SkipTo(generationStampV1.GetCurrentValue() + HdfsConstants.ReservedGenerationStampsV1
				);
			generationStampV1Limit = generationStampV2.GetCurrentValue();
			return generationStampV2.GetCurrentValue();
		}

		/// <summary>
		/// Sets the generation stamp that delineates random and sequentially
		/// allocated block IDs.
		/// </summary>
		/// <param name="stamp">set generation stamp limit to this value</param>
		public virtual void SetGenerationStampV1Limit(long stamp)
		{
			Preconditions.CheckState(generationStampV1Limit == GenerationStamp.GrandfatherGenerationStamp
				);
			generationStampV1Limit = stamp;
		}

		/// <summary>
		/// Gets the value of the generation stamp that delineates sequential
		/// and random block IDs.
		/// </summary>
		public virtual long GetGenerationStampAtblockIdSwitch()
		{
			return generationStampV1Limit;
		}

		[VisibleForTesting]
		internal virtual SequentialBlockIdGenerator GetBlockIdGenerator()
		{
			return blockIdGenerator;
		}

		/// <summary>Sets the maximum allocated block ID for this filesystem.</summary>
		/// <remarks>
		/// Sets the maximum allocated block ID for this filesystem. This is
		/// the basis for allocating new block IDs.
		/// </remarks>
		public virtual void SetLastAllocatedBlockId(long blockId)
		{
			blockIdGenerator.SkipTo(blockId);
		}

		/// <summary>Gets the maximum sequentially allocated block ID for this filesystem</summary>
		public virtual long GetLastAllocatedBlockId()
		{
			return blockIdGenerator.GetCurrentValue();
		}

		/// <summary>Sets the current generation stamp for legacy blocks</summary>
		public virtual void SetGenerationStampV1(long stamp)
		{
			generationStampV1.SetCurrentValue(stamp);
		}

		/// <summary>Gets the current generation stamp for legacy blocks</summary>
		public virtual long GetGenerationStampV1()
		{
			return generationStampV1.GetCurrentValue();
		}

		/// <summary>Gets the current generation stamp for this filesystem</summary>
		public virtual void SetGenerationStampV2(long stamp)
		{
			generationStampV2.SetCurrentValue(stamp);
		}

		public virtual long GetGenerationStampV2()
		{
			return generationStampV2.GetCurrentValue();
		}

		/// <summary>Increments, logs and then returns the stamp</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual long NextGenerationStamp(bool legacyBlock)
		{
			return legacyBlock ? GetNextGenerationStampV1() : GetNextGenerationStampV2();
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual long GetNextGenerationStampV1()
		{
			long genStampV1 = generationStampV1.NextValue();
			if (genStampV1 >= generationStampV1Limit)
			{
				// We ran out of generation stamps for legacy blocks. In practice, it
				// is extremely unlikely as we reserved 1T v1 generation stamps. The
				// result is that we can no longer append to the legacy blocks that
				// were created before the upgrade to sequential block IDs.
				throw new OutOfV1GenerationStampsException();
			}
			return genStampV1;
		}

		[VisibleForTesting]
		internal virtual long GetNextGenerationStampV2()
		{
			return generationStampV2.NextValue();
		}

		public virtual long GetGenerationStampV1Limit()
		{
			return generationStampV1Limit;
		}

		/// <summary>
		/// Determine whether the block ID was randomly generated (legacy) or
		/// sequentially generated.
		/// </summary>
		/// <remarks>
		/// Determine whether the block ID was randomly generated (legacy) or
		/// sequentially generated. The generation stamp value is used to
		/// make the distinction.
		/// </remarks>
		/// <returns>true if the block ID was randomly generated, false otherwise.</returns>
		public virtual bool IsLegacyBlock(Block block)
		{
			return block.GetGenerationStamp() < GetGenerationStampV1Limit();
		}

		/// <summary>Increments, logs and then returns the block ID</summary>
		public virtual long NextBlockId()
		{
			return blockIdGenerator.NextValue();
		}

		public virtual bool IsGenStampInFuture(Block block)
		{
			if (IsLegacyBlock(block))
			{
				return block.GetGenerationStamp() > GetGenerationStampV1();
			}
			else
			{
				return block.GetGenerationStamp() > GetGenerationStampV2();
			}
		}

		public virtual void Clear()
		{
			generationStampV1.SetCurrentValue(GenerationStamp.LastReservedStamp);
			generationStampV2.SetCurrentValue(GenerationStamp.LastReservedStamp);
			GetBlockIdGenerator().SetCurrentValue(SequentialBlockIdGenerator.LastReservedBlockId
				);
			generationStampV1Limit = GenerationStamp.GrandfatherGenerationStamp;
		}
	}
}
