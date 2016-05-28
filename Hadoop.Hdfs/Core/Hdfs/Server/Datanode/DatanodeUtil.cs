using System.IO;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>Provide utility methods for Datanode.</summary>
	public class DatanodeUtil
	{
		public const string UnlinkBlockSuffix = ".unlinked";

		public const string DiskError = "Possible disk error: ";

		private static readonly string Sep = Runtime.GetProperty("file.separator");

		/// <summary>Get the cause of an I/O exception if caused by a possible disk error</summary>
		/// <param name="ioe">an I/O exception</param>
		/// <returns>
		/// cause if the I/O exception is caused by a possible disk error;
		/// null otherwise.
		/// </returns>
		internal static IOException GetCauseIfDiskError(IOException ioe)
		{
			if (ioe.Message != null && ioe.Message.StartsWith(DiskError))
			{
				return (IOException)ioe.InnerException;
			}
			else
			{
				return null;
			}
		}

		/// <summary>Create a new file.</summary>
		/// <exception cref="System.IO.IOException">
		/// 
		/// if the file already exists or if the file cannot be created.
		/// </exception>
		public static FilePath CreateTmpFile(Block b, FilePath f)
		{
			if (f.Exists())
			{
				throw new IOException("Failed to create temporary file for " + b + ".  File " + f
					 + " should not be present, but is.");
			}
			// Create the zero-length temp file
			bool fileCreated;
			try
			{
				fileCreated = f.CreateNewFile();
			}
			catch (IOException ioe)
			{
				throw new IOException(DiskError + "Failed to create " + f, ioe);
			}
			if (!fileCreated)
			{
				throw new IOException("Failed to create temporary file for " + b + ".  File " + f
					 + " should be creatable, but is already present.");
			}
			return f;
		}

		/// <returns>the meta name given the block name and generation stamp.</returns>
		public static string GetMetaName(string blockName, long generationStamp)
		{
			return blockName + "_" + generationStamp + Block.MetadataExtension;
		}

		/// <returns>the unlink file.</returns>
		public static FilePath GetUnlinkTmpFile(FilePath f)
		{
			return new FilePath(f.GetParentFile(), f.GetName() + UnlinkBlockSuffix);
		}

		/// <summary>
		/// Checks whether there are any files anywhere in the directory tree rooted
		/// at dir (directories don't count as files).
		/// </summary>
		/// <remarks>
		/// Checks whether there are any files anywhere in the directory tree rooted
		/// at dir (directories don't count as files). dir must exist
		/// </remarks>
		/// <returns>true if there are no files</returns>
		/// <exception cref="System.IO.IOException">if unable to list subdirectories</exception>
		public static bool DirNoFilesRecursive(FilePath dir)
		{
			FilePath[] contents = dir.ListFiles();
			if (contents == null)
			{
				throw new IOException("Cannot list contents of " + dir);
			}
			foreach (FilePath f in contents)
			{
				if (!f.IsDirectory() || (f.IsDirectory() && !DirNoFilesRecursive(f)))
				{
					return false;
				}
			}
			return true;
		}

		/// <summary>Get the directory where a finalized block with this ID should be stored.
		/// 	</summary>
		/// <remarks>
		/// Get the directory where a finalized block with this ID should be stored.
		/// Do not attempt to create the directory.
		/// </remarks>
		/// <param name="root">the root directory where finalized blocks are stored</param>
		/// <param name="blockId"/>
		/// <returns/>
		public static FilePath IdToBlockDir(FilePath root, long blockId)
		{
			int d1 = (int)((blockId >> 16) & unchecked((int)(0xff)));
			int d2 = (int)((blockId >> 8) & unchecked((int)(0xff)));
			string path = DataStorage.BlockSubdirPrefix + d1 + Sep + DataStorage.BlockSubdirPrefix
				 + d2;
			return new FilePath(root, path);
		}

		/// <returns>the FileInputStream for the meta data of the given block.</returns>
		/// <exception cref="System.IO.FileNotFoundException">if the file not found.</exception>
		/// <exception cref="System.InvalidCastException">if the underlying input stream is not a FileInputStream.
		/// 	</exception>
		/// <exception cref="System.IO.IOException"/>
		public static FileInputStream GetMetaDataInputStream<_T0>(ExtendedBlock b, FsDatasetSpi
			<_T0> data)
			where _T0 : FsVolumeSpi
		{
			LengthInputStream lin = data.GetMetaDataInputStream(b);
			if (lin == null)
			{
				throw new FileNotFoundException("Meta file for " + b + " not found.");
			}
			return (FileInputStream)lin.GetWrappedStream();
		}
	}
}
