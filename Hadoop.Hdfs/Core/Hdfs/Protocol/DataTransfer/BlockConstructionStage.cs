using System;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer
{
	/// <summary>Block Construction Stage</summary>
	[System.Serializable]
	public sealed class BlockConstructionStage
	{
		/// <summary>
		/// The enumerates are always listed as regular stage followed by the
		/// recovery stage.
		/// </summary>
		/// <remarks>
		/// The enumerates are always listed as regular stage followed by the
		/// recovery stage.
		/// Changing this order will make getRecoveryStage not working.
		/// </remarks>
		public static readonly BlockConstructionStage PipelineSetupAppend = new BlockConstructionStage
			();

		public static readonly BlockConstructionStage PipelineSetupAppendRecovery = new BlockConstructionStage
			();

		public static readonly BlockConstructionStage DataStreaming = new BlockConstructionStage
			();

		public static readonly BlockConstructionStage PipelineSetupStreamingRecovery = new 
			BlockConstructionStage();

		public static readonly BlockConstructionStage PipelineClose = new BlockConstructionStage
			();

		public static readonly BlockConstructionStage PipelineCloseRecovery = new BlockConstructionStage
			();

		public static readonly BlockConstructionStage PipelineSetupCreate = new BlockConstructionStage
			();

		public static readonly BlockConstructionStage TransferRbw = new BlockConstructionStage
			();

		public static readonly BlockConstructionStage TransferFinalized = new BlockConstructionStage
			();

		private const byte RecoveryBit = unchecked((byte)1);

		// pipeline set up for block append
		// pipeline set up for failed PIPELINE_SETUP_APPEND recovery
		// data streaming
		// pipeline setup for failed data streaming recovery
		// close the block and pipeline
		// Recover a failed PIPELINE_CLOSE
		// pipeline set up for block creation
		// transfer RBW for adding datanodes
		// transfer Finalized for adding datanodes
		/// <summary>get the recovery stage of this stage</summary>
		public BlockConstructionStage GetRecoveryStage()
		{
			if (this == BlockConstructionStage.PipelineSetupCreate)
			{
				throw new ArgumentException("Unexpected blockStage " + this);
			}
			else
			{
				return Values()[Ordinal() | BlockConstructionStage.RecoveryBit];
			}
		}
	}
}
