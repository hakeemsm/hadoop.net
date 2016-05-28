using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	/// <summary>Checkpoint command.</summary>
	/// <remarks>
	/// Checkpoint command.
	/// <p>
	/// Returned to the backup node by the name-node as a reply to the
	/// <see cref="NamenodeProtocol.StartCheckpoint(NamenodeRegistration)"/>
	/// request.<br />
	/// Contains:
	/// <ul>
	/// <li>
	/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.CheckpointSignature"/>
	/// identifying the particular checkpoint</li>
	/// <li>indicator whether the backup image should be discarded before starting
	/// the checkpoint</li>
	/// <li>indicator whether the image should be transfered back to the name-node
	/// upon completion of the checkpoint.</li>
	/// </ul>
	/// </remarks>
	public class CheckpointCommand : NamenodeCommand
	{
		private readonly CheckpointSignature cSig;

		private readonly bool needToReturnImage;

		public CheckpointCommand()
			: this(null, false)
		{
		}

		public CheckpointCommand(CheckpointSignature sig, bool needToReturnImg)
			: base(NamenodeProtocol.ActCheckpoint)
		{
			this.cSig = sig;
			this.needToReturnImage = needToReturnImg;
		}

		/// <summary>
		/// Checkpoint signature is used to ensure
		/// that nodes are talking about the same checkpoint.
		/// </summary>
		public virtual CheckpointSignature GetSignature()
		{
			return cSig;
		}

		/// <summary>
		/// Indicates whether the new checkpoint image needs to be transfered
		/// back to the name-node after the checkpoint is done.
		/// </summary>
		/// <returns>true if the checkpoint should be returned back.</returns>
		public virtual bool NeedToReturnImage()
		{
			return needToReturnImage;
		}
	}
}
