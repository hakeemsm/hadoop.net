using System;
using System.IO;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl
{
	/// <summary>Utility methods.</summary>
	public class FsDatasetUtil
	{
		internal static bool IsUnlinkTmpFile(FilePath f)
		{
			return f.GetName().EndsWith(DatanodeUtil.UnlinkBlockSuffix);
		}

		internal static FilePath GetOrigFile(FilePath unlinkTmpFile)
		{
			string name = unlinkTmpFile.GetName();
			if (!name.EndsWith(DatanodeUtil.UnlinkBlockSuffix))
			{
				throw new ArgumentException("unlinkTmpFile=" + unlinkTmpFile + " does not end with "
					 + DatanodeUtil.UnlinkBlockSuffix);
			}
			int n = name.Length - DatanodeUtil.UnlinkBlockSuffix.Length;
			return new FilePath(unlinkTmpFile.GetParentFile(), Sharpen.Runtime.Substring(name
				, 0, n));
		}

		internal static FilePath GetMetaFile(FilePath f, long gs)
		{
			return new FilePath(f.GetParent(), DatanodeUtil.GetMetaName(f.GetName(), gs));
		}

		/// <summary>Find the corresponding meta data file from a given block file</summary>
		/// <exception cref="System.IO.IOException"/>
		public static FilePath FindMetaFile(FilePath blockFile)
		{
			string prefix = blockFile.GetName() + "_";
			FilePath parent = blockFile.GetParentFile();
			FilePath[] matches = parent.ListFiles(new _FilenameFilter_56(parent, prefix));
			if (matches == null || matches.Length == 0)
			{
				throw new IOException("Meta file not found, blockFile=" + blockFile);
			}
			if (matches.Length > 1)
			{
				throw new IOException("Found more than one meta files: " + Arrays.AsList(matches)
					);
			}
			return matches[0];
		}

		private sealed class _FilenameFilter_56 : FilenameFilter
		{
			public _FilenameFilter_56(FilePath parent, string prefix)
			{
				this.parent = parent;
				this.prefix = prefix;
			}

			public bool Accept(FilePath dir, string name)
			{
				return dir.Equals(parent) && name.StartsWith(prefix) && name.EndsWith(Block.MetadataExtension
					);
			}

			private readonly FilePath parent;

			private readonly string prefix;
		}

		/// <summary>
		/// Find the meta-file for the specified block file
		/// and then return the generation stamp from the name of the meta-file.
		/// </summary>
		internal static long GetGenerationStampFromFile(FilePath[] listdir, FilePath blockFile
			)
		{
			string blockName = blockFile.GetName();
			for (int j = 0; j < listdir.Length; j++)
			{
				string path = listdir[j].GetName();
				if (!path.StartsWith(blockName))
				{
					continue;
				}
				if (blockFile == listdir[j])
				{
					continue;
				}
				return Block.GetGenerationStamp(listdir[j].GetName());
			}
			FsDatasetImpl.Log.Warn("Block " + blockFile + " does not have a metafile!");
			return GenerationStamp.GrandfatherGenerationStamp;
		}

		/// <summary>Find the corresponding meta data file from a given block file</summary>
		/// <exception cref="System.IO.IOException"/>
		internal static long ParseGenerationStamp(FilePath blockFile, FilePath metaFile)
		{
			string metaname = metaFile.GetName();
			string gs = Sharpen.Runtime.Substring(metaname, blockFile.GetName().Length + 1, metaname
				.Length - Block.MetadataExtension.Length);
			try
			{
				return long.Parse(gs);
			}
			catch (FormatException nfe)
			{
				throw new IOException("Failed to parse generation stamp: blockFile=" + blockFile 
					+ ", metaFile=" + metaFile, nfe);
			}
		}
	}
}
