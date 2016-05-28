using System.IO;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>A unique signature intended to identify checkpoint transactions.</summary>
	public class CheckpointSignature : StorageInfo, Comparable<Org.Apache.Hadoop.Hdfs.Server.Namenode.CheckpointSignature
		>
	{
		private const string FieldSeparator = ":";

		private const int NumFields = 7;

		internal string blockpoolID = string.Empty;

		internal long mostRecentCheckpointTxId;

		internal long curSegmentTxId;

		internal CheckpointSignature(FSImage fsImage)
			: base(fsImage.GetStorage())
		{
			blockpoolID = fsImage.GetBlockPoolID();
			mostRecentCheckpointTxId = fsImage.GetStorage().GetMostRecentCheckpointTxId();
			curSegmentTxId = fsImage.GetEditLog().GetCurSegmentTxId();
		}

		internal CheckpointSignature(string str)
			: base(HdfsServerConstants.NodeType.NameNode)
		{
			string[] fields = str.Split(FieldSeparator);
			System.Diagnostics.Debug.Assert(fields.Length == NumFields, "Must be " + NumFields
				 + " fields in CheckpointSignature");
			int i = 0;
			layoutVersion = System.Convert.ToInt32(fields[i++]);
			namespaceID = System.Convert.ToInt32(fields[i++]);
			cTime = long.Parse(fields[i++]);
			mostRecentCheckpointTxId = long.Parse(fields[i++]);
			curSegmentTxId = long.Parse(fields[i++]);
			clusterID = fields[i++];
			blockpoolID = fields[i];
		}

		public CheckpointSignature(StorageInfo info, string blockpoolID, long mostRecentCheckpointTxId
			, long curSegmentTxId)
			: base(info)
		{
			this.blockpoolID = blockpoolID;
			this.mostRecentCheckpointTxId = mostRecentCheckpointTxId;
			this.curSegmentTxId = curSegmentTxId;
		}

		/// <summary>Get the cluster id from CheckpointSignature</summary>
		/// <returns>the cluster id</returns>
		public override string GetClusterID()
		{
			return clusterID;
		}

		/// <summary>Get the block pool id from CheckpointSignature</summary>
		/// <returns>the block pool id</returns>
		public virtual string GetBlockpoolID()
		{
			return blockpoolID;
		}

		public virtual long GetMostRecentCheckpointTxId()
		{
			return mostRecentCheckpointTxId;
		}

		public virtual long GetCurSegmentTxId()
		{
			return curSegmentTxId;
		}

		/// <summary>Set the block pool id of CheckpointSignature.</summary>
		/// <param name="blockpoolID">the new blockpool id</param>
		public virtual void SetBlockpoolID(string blockpoolID)
		{
			this.blockpoolID = blockpoolID;
		}

		public override string ToString()
		{
			return layoutVersion.ToString() + FieldSeparator + namespaceID.ToString() + FieldSeparator
				 + cTime.ToString() + FieldSeparator + mostRecentCheckpointTxId.ToString() + FieldSeparator
				 + curSegmentTxId.ToString() + FieldSeparator + clusterID + FieldSeparator + blockpoolID;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual bool StorageVersionMatches(StorageInfo si)
		{
			return (layoutVersion == si.layoutVersion) && (cTime == si.cTime);
		}

		internal virtual bool IsSameCluster(FSImage si)
		{
			return namespaceID == si.GetStorage().namespaceID && clusterID.Equals(si.GetClusterID
				()) && blockpoolID.Equals(si.GetBlockPoolID());
		}

		internal virtual bool NamespaceIdMatches(FSImage si)
		{
			return namespaceID == si.GetStorage().namespaceID;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void ValidateStorageInfo(FSImage si)
		{
			if (!IsSameCluster(si) || !StorageVersionMatches(si.GetStorage()))
			{
				throw new IOException("Inconsistent checkpoint fields.\n" + "LV = " + layoutVersion
					 + " namespaceID = " + namespaceID + " cTime = " + cTime + " ; clusterId = " + clusterID
					 + " ; blockpoolId = " + blockpoolID + ".\nExpecting respectively: " + si.GetStorage
					().layoutVersion + "; " + si.GetStorage().namespaceID + "; " + si.GetStorage().cTime
					 + "; " + si.GetClusterID() + "; " + si.GetBlockPoolID() + ".");
			}
		}

		//
		// Comparable interface
		//
		public virtual int CompareTo(Org.Apache.Hadoop.Hdfs.Server.Namenode.CheckpointSignature
			 o)
		{
			return ComparisonChain.Start().Compare(layoutVersion, o.layoutVersion).Compare(namespaceID
				, o.namespaceID).Compare(cTime, o.cTime).Compare(mostRecentCheckpointTxId, o.mostRecentCheckpointTxId
				).Compare(curSegmentTxId, o.curSegmentTxId).Compare(clusterID, o.clusterID).Compare
				(blockpoolID, o.blockpoolID).Result();
		}

		public override bool Equals(object o)
		{
			if (!(o is Org.Apache.Hadoop.Hdfs.Server.Namenode.CheckpointSignature))
			{
				return false;
			}
			return CompareTo((Org.Apache.Hadoop.Hdfs.Server.Namenode.CheckpointSignature)o) ==
				 0;
		}

		public override int GetHashCode()
		{
			return layoutVersion ^ namespaceID ^ (int)(cTime ^ mostRecentCheckpointTxId ^ curSegmentTxId
				) ^ clusterID.GetHashCode() ^ blockpoolID.GetHashCode();
		}
	}
}
