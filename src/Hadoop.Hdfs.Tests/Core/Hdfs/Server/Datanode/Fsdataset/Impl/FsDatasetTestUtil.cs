using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl
{
	public class FsDatasetTestUtil
	{
		public static FilePath GetFile<_T0>(FsDatasetSpi<_T0> fsd, string bpid, long bid)
			where _T0 : FsVolumeSpi
		{
			return ((FsDatasetImpl)fsd).GetFile(bpid, bid, false);
		}

		/// <exception cref="System.IO.IOException"/>
		public static FilePath GetBlockFile<_T0>(FsDatasetSpi<_T0> fsd, string bpid, Block
			 b)
			where _T0 : FsVolumeSpi
		{
			return ((FsDatasetImpl)fsd).GetBlockFile(bpid, b.GetBlockId());
		}

		/// <exception cref="System.IO.IOException"/>
		public static FilePath GetMetaFile<_T0>(FsDatasetSpi<_T0> fsd, string bpid, Block
			 b)
			where _T0 : FsVolumeSpi
		{
			return FsDatasetUtil.GetMetaFile(GetBlockFile(fsd, bpid, b), b.GetGenerationStamp
				());
		}

		/// <exception cref="System.IO.IOException"/>
		public static bool UnlinkBlock<_T0>(FsDatasetSpi<_T0> fsd, ExtendedBlock block, int
			 numLinks)
			where _T0 : FsVolumeSpi
		{
			ReplicaInfo info = ((FsDatasetImpl)fsd).GetReplicaInfo(block);
			return info.UnlinkBlock(numLinks);
		}

		public static ReplicaInfo FetchReplicaInfo<_T0>(FsDatasetSpi<_T0> fsd, string bpid
			, long blockId)
			where _T0 : FsVolumeSpi
		{
			return ((FsDatasetImpl)fsd).FetchReplicaInfo(bpid, blockId);
		}

		public static long GetPendingAsyncDeletions<_T0>(FsDatasetSpi<_T0> fsd)
			where _T0 : FsVolumeSpi
		{
			return ((FsDatasetImpl)fsd).asyncDiskService.CountPendingDeletions();
		}

		public static ICollection<ReplicaInfo> GetReplicas<_T0>(FsDatasetSpi<_T0> fsd, string
			 bpid)
			where _T0 : FsVolumeSpi
		{
			return ((FsDatasetImpl)fsd).volumeMap.Replicas(bpid);
		}

		/// <summary>Stop the lazy writer daemon that saves RAM disk files to persistent storage.
		/// 	</summary>
		/// <param name="dn"/>
		public static void StopLazyWriter(DataNode dn)
		{
			FsDatasetImpl fsDataset = ((FsDatasetImpl)dn.GetFSDataset());
			((FsDatasetImpl.LazyWriter)fsDataset.lazyWriter.GetRunnable()).Stop();
		}

		/// <summary>
		/// Asserts that the storage lock file in the given directory has been
		/// released.
		/// </summary>
		/// <remarks>
		/// Asserts that the storage lock file in the given directory has been
		/// released.  This method works by trying to acquire the lock file itself.  If
		/// locking fails here, then the main code must have failed to release it.
		/// </remarks>
		/// <param name="dir">the storage directory to check</param>
		/// <exception cref="System.IO.IOException">if there is an unexpected I/O error</exception>
		public static void AssertFileLockReleased(string dir)
		{
			StorageLocation sl = StorageLocation.Parse(dir);
			FilePath lockFile = new FilePath(sl.GetFile(), Storage.StorageFileLock);
			try
			{
				using (RandomAccessFile raf = new RandomAccessFile(lockFile, "rws"))
				{
					using (FileChannel channel = raf.GetChannel())
					{
						FileLock Lock = channel.TryLock();
						NUnit.Framework.Assert.IsNotNull(string.Format("Lock file at %s appears to be held by a different process."
							, lockFile.GetAbsolutePath()), Lock);
						if (Lock != null)
						{
							try
							{
								Lock.Release();
							}
							catch (IOException e)
							{
								FsDatasetImpl.Log.Warn(string.Format("I/O error releasing file lock %s.", lockFile
									.GetAbsolutePath()), e);
								throw;
							}
						}
					}
				}
			}
			catch (OverlappingFileLockException)
			{
				NUnit.Framework.Assert.Fail(string.Format("Must release lock file at %s.", lockFile
					.GetAbsolutePath()));
			}
		}
	}
}
