using System.IO;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Ipc;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>
	/// ReportBadBlockAction is an instruction issued by {{BPOfferService}} to
	/// {{BPServiceActor}} to report bad block to namenode
	/// </summary>
	public class ReportBadBlockAction : BPServiceActorAction
	{
		private readonly ExtendedBlock block;

		private readonly string storageUuid;

		private readonly StorageType storageType;

		public ReportBadBlockAction(ExtendedBlock block, string storageUuid, StorageType 
			storageType)
		{
			this.block = block;
			this.storageUuid = storageUuid;
			this.storageType = storageType;
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Datanode.BPServiceActorActionException
		/// 	"/>
		public virtual void ReportTo(DatanodeProtocolClientSideTranslatorPB bpNamenode, DatanodeRegistration
			 bpRegistration)
		{
			if (bpRegistration == null)
			{
				return;
			}
			DatanodeInfo[] dnArr = new DatanodeInfo[] { new DatanodeInfo(bpRegistration) };
			string[] uuids = new string[] { storageUuid };
			StorageType[] types = new StorageType[] { storageType };
			LocatedBlock[] locatedBlock = new LocatedBlock[] { new LocatedBlock(block, dnArr, 
				uuids, types) };
			try
			{
				bpNamenode.ReportBadBlocks(locatedBlock);
			}
			catch (RemoteException re)
			{
				DataNode.Log.Info("reportBadBlock encountered RemoteException for " + "block:  " 
					+ block, re);
			}
			catch (IOException)
			{
				throw new BPServiceActorActionException("Failed to report bad block " + block + " to namenode: "
					);
			}
		}

		public override int GetHashCode()
		{
			int prime = 31;
			int result = 1;
			result = prime * result + ((block == null) ? 0 : block.GetHashCode());
			result = prime * result + ((storageType == null) ? 0 : storageType.GetHashCode());
			result = prime * result + ((storageUuid == null) ? 0 : storageUuid.GetHashCode());
			return result;
		}

		public override bool Equals(object obj)
		{
			if (this == obj)
			{
				return true;
			}
			if (obj == null || !(obj is Org.Apache.Hadoop.Hdfs.Server.Datanode.ReportBadBlockAction
				))
			{
				return false;
			}
			Org.Apache.Hadoop.Hdfs.Server.Datanode.ReportBadBlockAction other = (Org.Apache.Hadoop.Hdfs.Server.Datanode.ReportBadBlockAction
				)obj;
			if (block == null)
			{
				if (other.block != null)
				{
					return false;
				}
			}
			else
			{
				if (!block.Equals(other.block))
				{
					return false;
				}
			}
			if (storageType != other.storageType)
			{
				return false;
			}
			if (storageUuid == null)
			{
				if (other.storageUuid != null)
				{
					return false;
				}
			}
			else
			{
				if (!storageUuid.Equals(other.storageUuid))
				{
					return false;
				}
			}
			return true;
		}
	}
}
