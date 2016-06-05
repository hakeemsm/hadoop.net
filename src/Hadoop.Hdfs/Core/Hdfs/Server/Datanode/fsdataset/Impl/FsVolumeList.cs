using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl
{
	internal class FsVolumeList
	{
		private readonly AtomicReference<FsVolumeImpl[]> volumes = new AtomicReference<FsVolumeImpl
			[]>(new FsVolumeImpl[0]);

		private readonly IDictionary<string, VolumeFailureInfo> volumeFailureInfos = Sharpen.Collections
			.SynchronizedMap(new SortedDictionary<string, VolumeFailureInfo>());

		private object checkDirsMutex = new object();

		private readonly VolumeChoosingPolicy<FsVolumeImpl> blockChooser;

		private readonly BlockScanner blockScanner;

		internal FsVolumeList(IList<VolumeFailureInfo> initialVolumeFailureInfos, BlockScanner
			 blockScanner, VolumeChoosingPolicy<FsVolumeImpl> blockChooser)
		{
			// Tracks volume failures, sorted by volume path.
			this.blockChooser = blockChooser;
			this.blockScanner = blockScanner;
			foreach (VolumeFailureInfo volumeFailureInfo in initialVolumeFailureInfos)
			{
				volumeFailureInfos[volumeFailureInfo.GetFailedStorageLocation()] = volumeFailureInfo;
			}
		}

		/// <summary>Return an immutable list view of all the volumes.</summary>
		internal virtual IList<FsVolumeImpl> GetVolumes()
		{
			return Sharpen.Collections.UnmodifiableList(Arrays.AsList(volumes.Get()));
		}

		/// <exception cref="System.IO.IOException"/>
		private FsVolumeReference ChooseVolume(IList<FsVolumeImpl> list, long blockSize)
		{
			while (true)
			{
				FsVolumeImpl volume = blockChooser.ChooseVolume(list, blockSize);
				try
				{
					return volume.ObtainReference();
				}
				catch (ClosedChannelException)
				{
					FsDatasetImpl.Log.Warn("Chosen a closed volume: " + volume);
					// blockChooser.chooseVolume returns DiskOutOfSpaceException when the list
					// is empty, indicating that all volumes are closed.
					list.Remove(volume);
				}
			}
		}

		/// <summary>Get next volume.</summary>
		/// <param name="blockSize">free space needed on the volume</param>
		/// <param name="storageType">
		/// the desired
		/// <see cref="Org.Apache.Hadoop.FS.StorageType"/>
		/// 
		/// </param>
		/// <returns>next volume to store the block in.</returns>
		/// <exception cref="System.IO.IOException"/>
		internal virtual FsVolumeReference GetNextVolume(StorageType storageType, long blockSize
			)
		{
			// Get a snapshot of currently available volumes.
			FsVolumeImpl[] curVolumes = volumes.Get();
			IList<FsVolumeImpl> list = new AList<FsVolumeImpl>(curVolumes.Length);
			foreach (FsVolumeImpl v in curVolumes)
			{
				if (v.GetStorageType() == storageType)
				{
					list.AddItem(v);
				}
			}
			return ChooseVolume(list, blockSize);
		}

		/// <summary>Get next volume.</summary>
		/// <param name="blockSize">free space needed on the volume</param>
		/// <returns>next volume to store the block in.</returns>
		/// <exception cref="System.IO.IOException"/>
		internal virtual FsVolumeReference GetNextTransientVolume(long blockSize)
		{
			// Get a snapshot of currently available volumes.
			IList<FsVolumeImpl> curVolumes = GetVolumes();
			IList<FsVolumeImpl> list = new AList<FsVolumeImpl>(curVolumes.Count);
			foreach (FsVolumeImpl v in curVolumes)
			{
				if (v.IsTransientStorage())
				{
					list.AddItem(v);
				}
			}
			return ChooseVolume(list, blockSize);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual long GetDfsUsed()
		{
			long dfsUsed = 0L;
			foreach (FsVolumeImpl v in volumes.Get())
			{
				try
				{
					using (FsVolumeReference @ref = v.ObtainReference())
					{
						dfsUsed += v.GetDfsUsed();
					}
				}
				catch (ClosedChannelException)
				{
				}
			}
			// ignore.
			return dfsUsed;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual long GetBlockPoolUsed(string bpid)
		{
			long dfsUsed = 0L;
			foreach (FsVolumeImpl v in volumes.Get())
			{
				try
				{
					using (FsVolumeReference @ref = v.ObtainReference())
					{
						dfsUsed += v.GetBlockPoolUsed(bpid);
					}
				}
				catch (ClosedChannelException)
				{
				}
			}
			// ignore.
			return dfsUsed;
		}

		internal virtual long GetCapacity()
		{
			long capacity = 0L;
			foreach (FsVolumeImpl v in volumes.Get())
			{
				try
				{
					using (FsVolumeReference @ref = v.ObtainReference())
					{
						capacity += v.GetCapacity();
					}
				}
				catch (IOException)
				{
				}
			}
			// ignore.
			return capacity;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual long GetRemaining()
		{
			long remaining = 0L;
			foreach (FsVolumeSpi vol in volumes.Get())
			{
				try
				{
					using (FsVolumeReference @ref = vol.ObtainReference())
					{
						remaining += vol.GetAvailable();
					}
				}
				catch (ClosedChannelException)
				{
				}
			}
			// ignore
			return remaining;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void GetAllVolumesMap(string bpid, ReplicaMap volumeMap, RamDiskReplicaTracker
			 ramDiskReplicaMap)
		{
			long totalStartTime = Time.MonotonicNow();
			IList<IOException> exceptions = Sharpen.Collections.SynchronizedList(new AList<IOException
				>());
			IList<Sharpen.Thread> replicaAddingThreads = new AList<Sharpen.Thread>();
			foreach (FsVolumeImpl v in volumes.Get())
			{
				Sharpen.Thread t = new _Thread_186(v, bpid, volumeMap, ramDiskReplicaMap, exceptions
					);
				replicaAddingThreads.AddItem(t);
				t.Start();
			}
			foreach (Sharpen.Thread t_1 in replicaAddingThreads)
			{
				try
				{
					t_1.Join();
				}
				catch (Exception ie)
				{
					throw new IOException(ie);
				}
			}
			if (!exceptions.IsEmpty())
			{
				throw exceptions[0];
			}
			long totalTimeTaken = Time.MonotonicNow() - totalStartTime;
			FsDatasetImpl.Log.Info("Total time to add all replicas to map: " + totalTimeTaken
				 + "ms");
		}

		private sealed class _Thread_186 : Sharpen.Thread
		{
			public _Thread_186(FsVolumeImpl v, string bpid, ReplicaMap volumeMap, RamDiskReplicaTracker
				 ramDiskReplicaMap, IList<IOException> exceptions)
			{
				this.v = v;
				this.bpid = bpid;
				this.volumeMap = volumeMap;
				this.ramDiskReplicaMap = ramDiskReplicaMap;
				this.exceptions = exceptions;
			}

			public override void Run()
			{
				try
				{
					using (FsVolumeReference @ref = v.ObtainReference())
					{
						FsDatasetImpl.Log.Info("Adding replicas to map for block pool " + bpid + " on volume "
							 + v + "...");
						long startTime = Time.MonotonicNow();
						v.GetVolumeMap(bpid, volumeMap, ramDiskReplicaMap);
						long timeTaken = Time.MonotonicNow() - startTime;
						FsDatasetImpl.Log.Info("Time to add replicas to map for block pool" + " " + bpid 
							+ " on volume " + v + ": " + timeTaken + "ms");
					}
				}
				catch (ClosedChannelException)
				{
					FsDatasetImpl.Log.Info("The volume " + v + " is closed while " + "addng replicas, ignored."
						);
				}
				catch (IOException ioe)
				{
					FsDatasetImpl.Log.Info("Caught exception while adding replicas " + "from " + v + 
						". Will throw later.", ioe);
					exceptions.AddItem(ioe);
				}
			}

			private readonly FsVolumeImpl v;

			private readonly string bpid;

			private readonly ReplicaMap volumeMap;

			private readonly RamDiskReplicaTracker ramDiskReplicaMap;

			private readonly IList<IOException> exceptions;
		}

		/// <summary>
		/// Calls
		/// <see cref="FsVolumeImpl.CheckDirs()"/>
		/// on each volume.
		/// Use checkDirsMutext to allow only one instance of checkDirs() call
		/// </summary>
		/// <returns>list of all the failed volumes.</returns>
		internal virtual ICollection<FilePath> CheckDirs()
		{
			lock (checkDirsMutex)
			{
				ICollection<FilePath> failedVols = null;
				// Make a copy of volumes for performing modification 
				IList<FsVolumeImpl> volumeList = GetVolumes();
				for (IEnumerator<FsVolumeImpl> i = volumeList.GetEnumerator(); i.HasNext(); )
				{
					FsVolumeImpl fsv = i.Next();
					try
					{
						using (FsVolumeReference @ref = fsv.ObtainReference())
						{
							fsv.CheckDirs();
						}
					}
					catch (DiskChecker.DiskErrorException e)
					{
						FsDatasetImpl.Log.Warn("Removing failed volume " + fsv + ": ", e);
						if (failedVols == null)
						{
							failedVols = new HashSet<FilePath>(1);
						}
						failedVols.AddItem(new FilePath(fsv.GetBasePath()).GetAbsoluteFile());
						AddVolumeFailureInfo(fsv);
						RemoveVolume(fsv);
					}
					catch (ClosedChannelException e)
					{
						FsDatasetImpl.Log.Debug("Caught exception when obtaining " + "reference count on closed volume"
							, e);
					}
					catch (IOException e)
					{
						FsDatasetImpl.Log.Error("Unexpected IOException", e);
					}
				}
				if (failedVols != null && failedVols.Count > 0)
				{
					FsDatasetImpl.Log.Warn("Completed checkDirs. Found " + failedVols.Count + " failure volumes."
						);
				}
				return failedVols;
			}
		}

		public override string ToString()
		{
			return Arrays.ToString(volumes.Get());
		}

		/// <summary>Dynamically add new volumes to the existing volumes that this DN manages.
		/// 	</summary>
		/// <param name="ref">a reference to the new FsVolumeImpl instance.</param>
		internal virtual void AddVolume(FsVolumeReference @ref)
		{
			while (true)
			{
				FsVolumeImpl[] curVolumes = volumes.Get();
				IList<FsVolumeImpl> volumeList = Lists.NewArrayList(curVolumes);
				volumeList.AddItem((FsVolumeImpl)@ref.GetVolume());
				if (volumes.CompareAndSet(curVolumes, Sharpen.Collections.ToArray(volumeList, new 
					FsVolumeImpl[volumeList.Count])))
				{
					break;
				}
				else
				{
					if (FsDatasetImpl.Log.IsDebugEnabled())
					{
						FsDatasetImpl.Log.Debug("The volume list has been changed concurrently, " + "retry to remove volume: "
							 + @ref.GetVolume().GetStorageID());
					}
				}
			}
			if (blockScanner != null)
			{
				blockScanner.AddVolumeScanner(@ref);
			}
			else
			{
				// If the volume is not put into a volume scanner, it does not need to
				// hold the reference.
				IOUtils.Cleanup(FsDatasetImpl.Log, @ref);
			}
			// If the volume is used to replace a failed volume, it needs to reset the
			// volume failure info for this volume.
			RemoveVolumeFailureInfo(new FilePath(@ref.GetVolume().GetBasePath()));
			FsDatasetImpl.Log.Info("Added new volume: " + @ref.GetVolume().GetStorageID());
		}

		/// <summary>Dynamically remove a volume in the list.</summary>
		/// <param name="target">the volume instance to be removed.</param>
		private void RemoveVolume(FsVolumeImpl target)
		{
			while (true)
			{
				FsVolumeImpl[] curVolumes = volumes.Get();
				IList<FsVolumeImpl> volumeList = Lists.NewArrayList(curVolumes);
				if (volumeList.Remove(target))
				{
					if (volumes.CompareAndSet(curVolumes, Sharpen.Collections.ToArray(volumeList, new 
						FsVolumeImpl[volumeList.Count])))
					{
						if (blockScanner != null)
						{
							blockScanner.RemoveVolumeScanner(target);
						}
						try
						{
							target.CloseAndWait();
						}
						catch (IOException e)
						{
							FsDatasetImpl.Log.Warn("Error occurs when waiting volume to close: " + target, e);
						}
						target.Shutdown();
						FsDatasetImpl.Log.Info("Removed volume: " + target);
						break;
					}
					else
					{
						if (FsDatasetImpl.Log.IsDebugEnabled())
						{
							FsDatasetImpl.Log.Debug("The volume list has been changed concurrently, " + "retry to remove volume: "
								 + target);
						}
					}
				}
				else
				{
					if (FsDatasetImpl.Log.IsDebugEnabled())
					{
						FsDatasetImpl.Log.Debug("Volume " + target + " does not exist or is removed by others."
							);
					}
					break;
				}
			}
		}

		/// <summary>Dynamically remove volume in the list.</summary>
		/// <param name="volume">the volume to be removed.</param>
		/// <param name="clearFailure">set true to remove failure info for this volume.</param>
		internal virtual void RemoveVolume(FilePath volume, bool clearFailure)
		{
			// Make a copy of volumes to remove one volume.
			FsVolumeImpl[] curVolumes = volumes.Get();
			IList<FsVolumeImpl> volumeList = Lists.NewArrayList(curVolumes);
			for (IEnumerator<FsVolumeImpl> it = volumeList.GetEnumerator(); it.HasNext(); )
			{
				FsVolumeImpl fsVolume = it.Next();
				string basePath;
				string targetPath;
				basePath = new FilePath(fsVolume.GetBasePath()).GetAbsolutePath();
				targetPath = volume.GetAbsolutePath();
				if (basePath.Equals(targetPath))
				{
					// Make sure the removed volume is the one in the curVolumes.
					RemoveVolume(fsVolume);
				}
			}
			if (clearFailure)
			{
				RemoveVolumeFailureInfo(volume);
			}
		}

		internal virtual VolumeFailureInfo[] GetVolumeFailureInfos()
		{
			ICollection<VolumeFailureInfo> infos = volumeFailureInfos.Values;
			return Sharpen.Collections.ToArray(infos, new VolumeFailureInfo[infos.Count]);
		}

		internal virtual void AddVolumeFailureInfo(VolumeFailureInfo volumeFailureInfo)
		{
			volumeFailureInfos[volumeFailureInfo.GetFailedStorageLocation()] = volumeFailureInfo;
		}

		private void AddVolumeFailureInfo(FsVolumeImpl vol)
		{
			AddVolumeFailureInfo(new VolumeFailureInfo(new FilePath(vol.GetBasePath()).GetAbsolutePath
				(), Time.Now(), vol.GetCapacity()));
		}

		private void RemoveVolumeFailureInfo(FilePath vol)
		{
			Sharpen.Collections.Remove(volumeFailureInfos, vol.GetAbsolutePath());
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void AddBlockPool(string bpid, Configuration conf)
		{
			long totalStartTime = Time.MonotonicNow();
			IList<IOException> exceptions = Sharpen.Collections.SynchronizedList(new AList<IOException
				>());
			IList<Sharpen.Thread> blockPoolAddingThreads = new AList<Sharpen.Thread>();
			foreach (FsVolumeImpl v in volumes.Get())
			{
				Sharpen.Thread t = new _Thread_399(v, bpid, conf, exceptions);
				// ignore.
				blockPoolAddingThreads.AddItem(t);
				t.Start();
			}
			foreach (Sharpen.Thread t_1 in blockPoolAddingThreads)
			{
				try
				{
					t_1.Join();
				}
				catch (Exception ie)
				{
					throw new IOException(ie);
				}
			}
			if (!exceptions.IsEmpty())
			{
				throw exceptions[0];
			}
			long totalTimeTaken = Time.MonotonicNow() - totalStartTime;
			FsDatasetImpl.Log.Info("Total time to scan all replicas for block pool " + bpid +
				 ": " + totalTimeTaken + "ms");
		}

		private sealed class _Thread_399 : Sharpen.Thread
		{
			public _Thread_399(FsVolumeImpl v, string bpid, Configuration conf, IList<IOException
				> exceptions)
			{
				this.v = v;
				this.bpid = bpid;
				this.conf = conf;
				this.exceptions = exceptions;
			}

			public override void Run()
			{
				try
				{
					using (FsVolumeReference @ref = v.ObtainReference())
					{
						FsDatasetImpl.Log.Info("Scanning block pool " + bpid + " on volume " + v + "...");
						long startTime = Time.MonotonicNow();
						v.AddBlockPool(bpid, conf);
						long timeTaken = Time.MonotonicNow() - startTime;
						FsDatasetImpl.Log.Info("Time taken to scan block pool " + bpid + " on " + v + ": "
							 + timeTaken + "ms");
					}
				}
				catch (ClosedChannelException)
				{
				}
				catch (IOException ioe)
				{
					FsDatasetImpl.Log.Info("Caught exception while scanning " + v + ". Will throw later."
						, ioe);
					exceptions.AddItem(ioe);
				}
			}

			private readonly FsVolumeImpl v;

			private readonly string bpid;

			private readonly Configuration conf;

			private readonly IList<IOException> exceptions;
		}

		internal virtual void RemoveBlockPool(string bpid)
		{
			foreach (FsVolumeImpl v in volumes.Get())
			{
				v.ShutdownBlockPool(bpid);
			}
		}

		internal virtual void Shutdown()
		{
			foreach (FsVolumeImpl volume in volumes.Get())
			{
				if (volume != null)
				{
					volume.Shutdown();
				}
			}
		}
	}
}
