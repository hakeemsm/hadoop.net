using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>This class is used by datanodes to maintain meta data of its replicas.</summary>
	/// <remarks>
	/// This class is used by datanodes to maintain meta data of its replicas.
	/// It provides a general interface for meta information of a replica.
	/// </remarks>
	public abstract class ReplicaInfo : Block, Replica
	{
		/// <summary>volume where the replica belongs</summary>
		private FsVolumeSpi volume;

		/// <summary>
		/// Base directory containing numerically-identified sub directories and
		/// possibly blocks.
		/// </summary>
		private FilePath baseDir;

		/// <summary>
		/// Whether or not this replica's parent directory includes subdirs, in which
		/// case we can generate them based on the replica's block ID
		/// </summary>
		private bool hasSubdirs;

		private static readonly IDictionary<string, FilePath> internedBaseDirs = new Dictionary
			<string, FilePath>();

		/// <summary>Constructor</summary>
		/// <param name="block">a block</param>
		/// <param name="vol">volume where replica is located</param>
		/// <param name="dir">directory path where block and meta files are located</param>
		internal ReplicaInfo(Block block, FsVolumeSpi vol, FilePath dir)
			: this(block.GetBlockId(), block.GetNumBytes(), block.GetGenerationStamp(), vol, 
				dir)
		{
		}

		/// <summary>Constructor</summary>
		/// <param name="blockId">block id</param>
		/// <param name="len">replica length</param>
		/// <param name="genStamp">replica generation stamp</param>
		/// <param name="vol">volume where replica is located</param>
		/// <param name="dir">directory path where block and meta files are located</param>
		internal ReplicaInfo(long blockId, long len, long genStamp, FsVolumeSpi vol, FilePath
			 dir)
			: base(blockId, len, genStamp)
		{
			this.volume = vol;
			SetDirInternal(dir);
		}

		/// <summary>Copy constructor.</summary>
		/// <param name="from">where to copy from</param>
		internal ReplicaInfo(Org.Apache.Hadoop.Hdfs.Server.Datanode.ReplicaInfo from)
			: this(from, from.GetVolume(), from.GetDir())
		{
		}

		/// <summary>Get the full path of this replica's data file</summary>
		/// <returns>the full path of this replica's data file</returns>
		public virtual FilePath GetBlockFile()
		{
			return new FilePath(GetDir(), GetBlockName());
		}

		/// <summary>Get the full path of this replica's meta file</summary>
		/// <returns>the full path of this replica's meta file</returns>
		public virtual FilePath GetMetaFile()
		{
			return new FilePath(GetDir(), DatanodeUtil.GetMetaName(GetBlockName(), GetGenerationStamp
				()));
		}

		/// <summary>Get the volume where this replica is located on disk</summary>
		/// <returns>the volume where this replica is located on disk</returns>
		public virtual FsVolumeSpi GetVolume()
		{
			return volume;
		}

		/// <summary>Set the volume where this replica is located on disk</summary>
		internal virtual void SetVolume(FsVolumeSpi vol)
		{
			this.volume = vol;
		}

		/// <summary>Get the storageUuid of the volume that stores this replica.</summary>
		public virtual string GetStorageUuid()
		{
			return volume.GetStorageID();
		}

		/// <summary>Return the parent directory path where this replica is located</summary>
		/// <returns>the parent directory path where this replica is located</returns>
		internal virtual FilePath GetDir()
		{
			return hasSubdirs ? DatanodeUtil.IdToBlockDir(baseDir, GetBlockId()) : baseDir;
		}

		/// <summary>Set the parent directory where this replica is located</summary>
		/// <param name="dir">the parent directory where the replica is located</param>
		public virtual void SetDir(FilePath dir)
		{
			SetDirInternal(dir);
		}

		private void SetDirInternal(FilePath dir)
		{
			if (dir == null)
			{
				baseDir = null;
				return;
			}
			ReplicaInfo.ReplicaDirInfo dirInfo = ParseBaseDir(dir);
			this.hasSubdirs = dirInfo.hasSubidrs;
			lock (internedBaseDirs)
			{
				if (!internedBaseDirs.Contains(dirInfo.baseDirPath))
				{
					// Create a new String path of this file and make a brand new File object
					// to guarantee we drop the reference to the underlying char[] storage.
					FilePath baseDir = new FilePath(dirInfo.baseDirPath);
					internedBaseDirs[dirInfo.baseDirPath] = baseDir;
				}
				this.baseDir = internedBaseDirs[dirInfo.baseDirPath];
			}
		}

		public class ReplicaDirInfo
		{
			public string baseDirPath;

			public bool hasSubidrs;

			public ReplicaDirInfo(string baseDirPath, bool hasSubidrs)
			{
				this.baseDirPath = baseDirPath;
				this.hasSubidrs = hasSubidrs;
			}
		}

		[VisibleForTesting]
		public static ReplicaInfo.ReplicaDirInfo ParseBaseDir(FilePath dir)
		{
			FilePath currentDir = dir;
			bool hasSubdirs = false;
			while (currentDir.GetName().StartsWith(DataStorage.BlockSubdirPrefix))
			{
				hasSubdirs = true;
				currentDir = currentDir.GetParentFile();
			}
			return new ReplicaInfo.ReplicaDirInfo(currentDir.GetAbsolutePath(), hasSubdirs);
		}

		/// <summary>check if this replica has already been unlinked.</summary>
		/// <returns>
		/// true if the replica has already been unlinked
		/// or no need to be detached; false otherwise
		/// </returns>
		public virtual bool IsUnlinked()
		{
			return true;
		}

		// no need to be unlinked
		/// <summary>set that this replica is unlinked</summary>
		public virtual void SetUnlinked()
		{
		}

		// no need to be unlinked
		/// <summary>Number of bytes reserved for this replica on disk.</summary>
		public virtual long GetBytesReserved()
		{
			return 0;
		}

		/// <summary>Copy specified file into a temporary file.</summary>
		/// <remarks>
		/// Copy specified file into a temporary file. Then rename the
		/// temporary file to the original name. This will cause any
		/// hardlinks to the original file to be removed. The temporary
		/// files are created in the same directory. The temporary files will
		/// be recovered (especially on Windows) on datanode restart.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private void UnlinkFile(FilePath file, Block b)
		{
			FilePath tmpFile = DatanodeUtil.CreateTmpFile(b, DatanodeUtil.GetUnlinkTmpFile(file
				));
			try
			{
				FileInputStream @in = new FileInputStream(file);
				try
				{
					FileOutputStream @out = new FileOutputStream(tmpFile);
					try
					{
						IOUtils.CopyBytes(@in, @out, 16 * 1024);
					}
					finally
					{
						@out.Close();
					}
				}
				finally
				{
					@in.Close();
				}
				if (file.Length() != tmpFile.Length())
				{
					throw new IOException("Copy of file " + file + " size " + file.Length() + " into file "
						 + tmpFile + " resulted in a size of " + tmpFile.Length());
				}
				FileUtil.ReplaceFile(tmpFile, file);
			}
			catch (IOException e)
			{
				bool done = tmpFile.Delete();
				if (!done)
				{
					DataNode.Log.Info("detachFile failed to delete temporary file " + tmpFile);
				}
				throw;
			}
		}

		/// <summary>
		/// Remove a hard link by copying the block to a temporary place and
		/// then moving it back
		/// </summary>
		/// <param name="numLinks">number of hard links</param>
		/// <returns>
		/// true if copy is successful;
		/// false if it is already detached or no need to be detached
		/// </returns>
		/// <exception cref="System.IO.IOException">if there is any copy error</exception>
		public virtual bool UnlinkBlock(int numLinks)
		{
			if (IsUnlinked())
			{
				return false;
			}
			FilePath file = GetBlockFile();
			if (file == null || GetVolume() == null)
			{
				throw new IOException("detachBlock:Block not found. " + this);
			}
			FilePath meta = GetMetaFile();
			if (HardLink.GetLinkCount(file) > numLinks)
			{
				DataNode.Log.Info("CopyOnWrite for block " + this);
				UnlinkFile(file, this);
			}
			if (HardLink.GetLinkCount(meta) > numLinks)
			{
				UnlinkFile(meta, this);
			}
			SetUnlinked();
			return true;
		}

		public override string ToString()
		{
			//Object
			return GetType().Name + ", " + base.ToString() + ", " + GetState() + "\n  getNumBytes()     = "
				 + GetNumBytes() + "\n  getBytesOnDisk()  = " + GetBytesOnDisk() + "\n  getVisibleLength()= "
				 + GetVisibleLength() + "\n  getVolume()       = " + GetVolume() + "\n  getBlockFile()    = "
				 + GetBlockFile();
		}

		public virtual bool IsOnTransientStorage()
		{
			return volume.IsTransientStorage();
		}

		public abstract long GetBytesOnDisk();

		public abstract HdfsServerConstants.ReplicaState GetState();

		public abstract long GetVisibleLength();
	}
}
