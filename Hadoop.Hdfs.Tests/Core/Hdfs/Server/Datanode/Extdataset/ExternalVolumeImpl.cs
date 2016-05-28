using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Extdataset
{
	public class ExternalVolumeImpl : FsVolumeSpi
	{
		public override string[] GetBlockPoolList()
		{
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public override long GetAvailable()
		{
			return 0;
		}

		public override string GetBasePath()
		{
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public override string GetPath(string bpid)
		{
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public override FilePath GetFinalizedDir(string bpid)
		{
			return null;
		}

		/// <exception cref="Sharpen.ClosedChannelException"/>
		public override FsVolumeReference ObtainReference()
		{
			return null;
		}

		public override string GetStorageID()
		{
			return null;
		}

		public override StorageType GetStorageType()
		{
			return StorageType.Default;
		}

		public override bool IsTransientStorage()
		{
			return false;
		}

		public override void ReserveSpaceForRbw(long bytesToReserve)
		{
		}

		public override void ReleaseReservedSpace(long bytesToRelease)
		{
		}

		public override FsVolumeSpi.BlockIterator NewBlockIterator(string bpid, string name
			)
		{
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public override FsVolumeSpi.BlockIterator LoadBlockIterator(string bpid, string name
			)
		{
			return null;
		}

		public override FsDatasetSpi GetDataset()
		{
			return null;
		}
	}
}
