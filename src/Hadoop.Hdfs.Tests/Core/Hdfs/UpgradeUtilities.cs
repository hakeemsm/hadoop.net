/*
* UpgradeUtilities.java
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
using System;
using System.IO;
using System.Text;
using Com.Google.Common.Base;
using Com.Google.Common.IO;
using Com.Google.Common.Primitives;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// This class defines a number of static helper methods used by the
	/// DFS Upgrade unit tests.
	/// </summary>
	/// <remarks>
	/// This class defines a number of static helper methods used by the
	/// DFS Upgrade unit tests.  By default, a singleton master populated storage
	/// directory is created for a Namenode (contains edits, fsimage,
	/// version, and time files) and a Datanode (contains version and
	/// block files).  The master directories are lazily created.  They are then
	/// copied by the createStorageDirs() method to create new storage
	/// directories of the appropriate type (Namenode or Datanode).
	/// </remarks>
	public class UpgradeUtilities
	{
		private static readonly FilePath TestRootDir = new FilePath(MiniDFSCluster.GetBaseDirectory
			());

		private static readonly FilePath namenodeStorage = new FilePath(TestRootDir, "namenodeMaster"
			);

		private static long namenodeStorageChecksum;

		private static int namenodeStorageNamespaceID;

		private static string namenodeStorageClusterID;

		private static string namenodeStorageBlockPoolID;

		private static long namenodeStorageFsscTime;

		private static readonly FilePath datanodeStorage = new FilePath(TestRootDir, "datanodeMaster"
			);

		private static long datanodeStorageChecksum;

		private static long blockPoolStorageChecksum;

		private static long blockPoolFinalizedStorageChecksum;

		private static long blockPoolRbwStorageChecksum;

		// Root scratch directory on local filesystem 
		// The singleton master storage directory for Namenode
		// A checksum of the contents in namenodeStorage directory
		// The namespaceId of the namenodeStorage directory
		// The clusterId of the namenodeStorage directory
		// The blockpoolId of the namenodeStorage directory
		// The fsscTime of the namenodeStorage directory
		// The singleton master storage directory for Datanode
		// A checksum of the contents in datanodeStorage directory
		// A checksum of the contents in blockpool storage directory
		// A checksum of the contents in blockpool finalize storage directory
		// A checksum of the contents in blockpool rbw storage directory
		/// <summary>Initialize the data structures used by this class.</summary>
		/// <remarks>
		/// Initialize the data structures used by this class.
		/// IMPORTANT NOTE: This method must be called once before calling
		/// any other public method on this class.
		/// <p>
		/// Creates a singleton master populated storage
		/// directory for a Namenode (contains edits, fsimage,
		/// version, and time files) and a Datanode (contains version and
		/// block files).  This can be a lengthy operation.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public static void Initialize()
		{
			CreateEmptyDirs(new string[] { TestRootDir.ToString() });
			Configuration config = new HdfsConfiguration();
			config.Set(DFSConfigKeys.DfsNamenodeNameDirKey, namenodeStorage.ToString());
			config.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, namenodeStorage.ToString());
			config.Set(DFSConfigKeys.DfsDatanodeDataDirKey, datanodeStorage.ToString());
			MiniDFSCluster cluster = null;
			string bpid = null;
			try
			{
				// format data-node
				CreateEmptyDirs(new string[] { datanodeStorage.ToString() });
				// format and start NameNode and start DataNode
				DFSTestUtil.FormatNameNode(config);
				cluster = new MiniDFSCluster.Builder(config).NumDataNodes(1).StartupOption(HdfsServerConstants.StartupOption
					.Regular).Format(false).ManageDataDfsDirs(false).ManageNameDfsDirs(false).Build(
					);
				NamenodeProtocols namenode = cluster.GetNameNodeRpc();
				namenodeStorageNamespaceID = namenode.VersionRequest().GetNamespaceID();
				namenodeStorageFsscTime = namenode.VersionRequest().GetCTime();
				namenodeStorageClusterID = namenode.VersionRequest().GetClusterID();
				namenodeStorageBlockPoolID = namenode.VersionRequest().GetBlockPoolID();
				FileSystem fs = FileSystem.Get(config);
				Path baseDir = new Path("/TestUpgrade");
				fs.Mkdirs(baseDir);
				// write some files
				int bufferSize = 4096;
				byte[] buffer = new byte[bufferSize];
				for (int i = 0; i < bufferSize; i++)
				{
					buffer[i] = unchecked((byte)((byte)('0') + i % 50));
				}
				WriteFile(fs, new Path(baseDir, "file1"), buffer, bufferSize);
				WriteFile(fs, new Path(baseDir, "file2"), buffer, bufferSize);
				// save image
				namenode.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter, false);
				namenode.SaveNamespace();
				namenode.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave, false);
				// write more files
				WriteFile(fs, new Path(baseDir, "file3"), buffer, bufferSize);
				WriteFile(fs, new Path(baseDir, "file4"), buffer, bufferSize);
				bpid = cluster.GetNamesystem(0).GetBlockPoolId();
			}
			finally
			{
				// shutdown
				if (cluster != null)
				{
					cluster.Shutdown();
				}
				FileUtil.FullyDelete(new FilePath(namenodeStorage, "in_use.lock"));
				FileUtil.FullyDelete(new FilePath(datanodeStorage, "in_use.lock"));
			}
			namenodeStorageChecksum = ChecksumContents(HdfsServerConstants.NodeType.NameNode, 
				new FilePath(namenodeStorage, "current"), false);
			FilePath dnCurDir = new FilePath(datanodeStorage, "current");
			datanodeStorageChecksum = ChecksumContents(HdfsServerConstants.NodeType.DataNode, 
				dnCurDir, false);
			FilePath bpCurDir = new FilePath(BlockPoolSliceStorage.GetBpRoot(bpid, dnCurDir), 
				"current");
			blockPoolStorageChecksum = ChecksumContents(HdfsServerConstants.NodeType.DataNode
				, bpCurDir, false);
			FilePath bpCurFinalizeDir = new FilePath(BlockPoolSliceStorage.GetBpRoot(bpid, dnCurDir
				), "current/" + DataStorage.StorageDirFinalized);
			blockPoolFinalizedStorageChecksum = ChecksumContents(HdfsServerConstants.NodeType
				.DataNode, bpCurFinalizeDir, true);
			FilePath bpCurRbwDir = new FilePath(BlockPoolSliceStorage.GetBpRoot(bpid, dnCurDir
				), "current/" + DataStorage.StorageDirRbw);
			blockPoolRbwStorageChecksum = ChecksumContents(HdfsServerConstants.NodeType.DataNode
				, bpCurRbwDir, false);
		}

		// Private helper method that writes a file to the given file system.
		/// <exception cref="System.IO.IOException"/>
		private static void WriteFile(FileSystem fs, Path path, byte[] buffer, int bufferSize
			)
		{
			OutputStream @out;
			@out = fs.Create(path, true, bufferSize, (short)1, 1024);
			@out.Write(buffer, 0, bufferSize);
			@out.Close();
		}

		/// <summary>
		/// Initialize
		/// <see cref="DFSConfigKeys.DfsNamenodeNameDirKey"/>
		/// and
		/// <see cref="DFSConfigKeys.DfsDatanodeDataDirKey"/>
		/// with the specified
		/// number of directory entries. Also initialize dfs.blockreport.intervalMsec.
		/// </summary>
		public static Configuration InitializeStorageStateConf(int numDirs, Configuration
			 conf)
		{
			StringBuilder nameNodeDirs = new StringBuilder(new FilePath(TestRootDir, "name1")
				.ToString());
			StringBuilder dataNodeDirs = new StringBuilder(new FilePath(TestRootDir, "data1")
				.ToString());
			for (int i = 2; i <= numDirs; i++)
			{
				nameNodeDirs.Append("," + new FilePath(TestRootDir, "name" + i));
				dataNodeDirs.Append("," + new FilePath(TestRootDir, "data" + i));
			}
			if (conf == null)
			{
				conf = new HdfsConfiguration();
			}
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, nameNodeDirs.ToString());
			conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, nameNodeDirs.ToString());
			conf.Set(DFSConfigKeys.DfsDatanodeDataDirKey, dataNodeDirs.ToString());
			conf.SetInt(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 10000);
			return conf;
		}

		/// <summary>Create empty directories.</summary>
		/// <remarks>
		/// Create empty directories.  If a specified directory already exists
		/// then it is first removed.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public static void CreateEmptyDirs(string[] dirs)
		{
			foreach (string d in dirs)
			{
				FilePath dir = new FilePath(d);
				if (dir.Exists())
				{
					FileUtil.FullyDelete(dir);
				}
				dir.Mkdirs();
			}
		}

		/// <summary>
		/// Return the checksum for the singleton master storage directory
		/// for namenode
		/// </summary>
		public static long ChecksumMasterNameNodeContents()
		{
			return namenodeStorageChecksum;
		}

		/// <summary>
		/// Return the checksum for the singleton master storage directory
		/// for datanode
		/// </summary>
		public static long ChecksumMasterDataNodeContents()
		{
			return datanodeStorageChecksum;
		}

		/// <summary>
		/// Return the checksum for the singleton master storage directory
		/// for block pool.
		/// </summary>
		public static long ChecksumMasterBlockPoolContents()
		{
			return blockPoolStorageChecksum;
		}

		/// <summary>
		/// Return the checksum for the singleton master storage directory
		/// for finalized dir under block pool.
		/// </summary>
		public static long ChecksumMasterBlockPoolFinalizedContents()
		{
			return blockPoolFinalizedStorageChecksum;
		}

		/// <summary>
		/// Return the checksum for the singleton master storage directory
		/// for rbw dir under block pool.
		/// </summary>
		public static long ChecksumMasterBlockPoolRbwContents()
		{
			return blockPoolRbwStorageChecksum;
		}

		/// <summary>Compute the checksum of all the files in the specified directory.</summary>
		/// <remarks>
		/// Compute the checksum of all the files in the specified directory.
		/// This method provides an easy way to ensure equality between the contents
		/// of two directories.
		/// </remarks>
		/// <param name="nodeType">
		/// if DATA_NODE then any file named "VERSION" is ignored.
		/// This is because this file file is changed every time
		/// the Datanode is started.
		/// </param>
		/// <param name="dir">must be a directory</param>
		/// <param name="recursive">whether or not to consider subdirectories</param>
		/// <exception cref="System.ArgumentException">if specified directory is not a directory
		/// 	</exception>
		/// <exception cref="System.IO.IOException">if an IOException occurs while reading the files
		/// 	</exception>
		/// <returns>the computed checksum value</returns>
		public static long ChecksumContents(HdfsServerConstants.NodeType nodeType, FilePath
			 dir, bool recursive)
		{
			CRC32 checksum = new CRC32();
			ChecksumContentsHelper(nodeType, dir, checksum, recursive);
			return checksum.GetValue();
		}

		/// <exception cref="System.IO.IOException"/>
		public static void ChecksumContentsHelper(HdfsServerConstants.NodeType nodeType, 
			FilePath dir, CRC32 checksum, bool recursive)
		{
			if (!dir.IsDirectory())
			{
				throw new ArgumentException("Given argument is not a directory:" + dir);
			}
			FilePath[] list = dir.ListFiles();
			Arrays.Sort(list);
			for (int i = 0; i < list.Length; i++)
			{
				if (!list[i].IsFile())
				{
					if (recursive)
					{
						ChecksumContentsHelper(nodeType, list[i], checksum, recursive);
					}
					continue;
				}
				// skip VERSION and dfsUsed file for DataNodes
				if (nodeType == HdfsServerConstants.NodeType.DataNode && (list[i].GetName().Equals
					("VERSION") || list[i].GetName().Equals("dfsUsed")))
				{
					continue;
				}
				FileInputStream fis = null;
				try
				{
					fis = new FileInputStream(list[i]);
					byte[] buffer = new byte[1024];
					int bytesRead;
					while ((bytesRead = fis.Read(buffer)) != -1)
					{
						checksum.Update(buffer, 0, bytesRead);
					}
				}
				finally
				{
					if (fis != null)
					{
						fis.Close();
					}
				}
			}
		}

		/// <summary>
		/// Simulate the
		/// <see cref="DFSConfigKeys.DfsNamenodeNameDirKey"/>
		/// of a populated
		/// DFS filesystem.
		/// This method populates for each parent directory, <code>parent/dirName</code>
		/// with the content of namenode storage directory that comes from a singleton
		/// namenode master (that contains edits, fsimage, version and time files).
		/// If the destination directory does not exist, it will be created.
		/// If the directory already exists, it will first be deleted.
		/// </summary>
		/// <param name="parents">
		/// parent directory where
		/// <paramref name="dirName"/>
		/// is created
		/// </param>
		/// <param name="dirName">directory under which storage directory is created</param>
		/// <returns>the array of created directories</returns>
		/// <exception cref="System.Exception"/>
		public static FilePath[] CreateNameNodeStorageDirs(string[] parents, string dirName
			)
		{
			FilePath[] retVal = new FilePath[parents.Length];
			for (int i = 0; i < parents.Length; i++)
			{
				FilePath newDir = new FilePath(parents[i], dirName);
				CreateEmptyDirs(new string[] { newDir.ToString() });
				LocalFileSystem localFS = FileSystem.GetLocal(new HdfsConfiguration());
				localFS.CopyToLocalFile(new Path(namenodeStorage.ToString(), "current"), new Path
					(newDir.ToString()), false);
				retVal[i] = newDir;
			}
			return retVal;
		}

		/// <summary>
		/// Simulate the
		/// <see cref="DFSConfigKeys.DfsDatanodeDataDirKey"/>
		/// of a
		/// populated DFS filesystem.
		/// This method populates for each parent directory, <code>parent/dirName</code>
		/// with the content of datanode storage directory that comes from a singleton
		/// datanode master (that contains version and block files). If the destination
		/// directory does not exist, it will be created.  If the directory already
		/// exists, it will first be deleted.
		/// </summary>
		/// <param name="parents">
		/// parent directory where
		/// <paramref name="dirName"/>
		/// is created
		/// </param>
		/// <param name="dirName">directory under which storage directory is created</param>
		/// <returns>the array of created directories</returns>
		/// <exception cref="System.Exception"/>
		public static FilePath[] CreateDataNodeStorageDirs(string[] parents, string dirName
			)
		{
			FilePath[] retVal = new FilePath[parents.Length];
			for (int i = 0; i < parents.Length; i++)
			{
				FilePath newDir = new FilePath(parents[i], dirName);
				CreateEmptyDirs(new string[] { newDir.ToString() });
				LocalFileSystem localFS = FileSystem.GetLocal(new HdfsConfiguration());
				localFS.CopyToLocalFile(new Path(datanodeStorage.ToString(), "current"), new Path
					(newDir.ToString()), false);
				retVal[i] = newDir;
			}
			return retVal;
		}

		/// <summary>
		/// Simulate the
		/// <see cref="DFSConfigKeys.DfsDatanodeDataDirKey"/>
		/// of a
		/// populated DFS filesystem.
		/// This method populates for each parent directory, <code>parent/dirName</code>
		/// with the content of block pool storage directory that comes from a singleton
		/// datanode master (that contains version and block files). If the destination
		/// directory does not exist, it will be created.  If the directory already
		/// exists, it will first be deleted.
		/// </summary>
		/// <param name="parents">
		/// parent directory where
		/// <paramref name="dirName"/>
		/// is created
		/// </param>
		/// <param name="dirName">directory under which storage directory is created</param>
		/// <param name="bpid">block pool id for which the storage directory is created.</param>
		/// <returns>the array of created directories</returns>
		/// <exception cref="System.Exception"/>
		public static FilePath[] CreateBlockPoolStorageDirs(string[] parents, string dirName
			, string bpid)
		{
			FilePath[] retVal = new FilePath[parents.Length];
			Path bpCurDir = new Path(MiniDFSCluster.GetBPDir(datanodeStorage, bpid, Storage.StorageDirCurrent
				));
			for (int i = 0; i < parents.Length; i++)
			{
				FilePath newDir = new FilePath(parents[i] + "/current/" + bpid, dirName);
				CreateEmptyDirs(new string[] { newDir.ToString() });
				LocalFileSystem localFS = FileSystem.GetLocal(new HdfsConfiguration());
				localFS.CopyToLocalFile(bpCurDir, new Path(newDir.ToString()), false);
				retVal[i] = newDir;
			}
			return retVal;
		}

		/// <summary>
		/// Create a <code>version</code> file for namenode inside the specified parent
		/// directory.
		/// </summary>
		/// <remarks>
		/// Create a <code>version</code> file for namenode inside the specified parent
		/// directory.  If such a file already exists, it will be overwritten.
		/// The given version string will be written to the file as the layout
		/// version. None of the parameters may be null.
		/// </remarks>
		/// <param name="parent">directory where namenode VERSION file is stored</param>
		/// <param name="version">StorageInfo to create VERSION file from</param>
		/// <param name="bpid">Block pool Id</param>
		/// <returns>the created version file</returns>
		/// <exception cref="System.IO.IOException"/>
		public static FilePath[] CreateNameNodeVersionFile(Configuration conf, FilePath[]
			 parent, StorageInfo version, string bpid)
		{
			Storage storage = new NNStorage(conf, Collections.EmptyList<URI>(), Collections.EmptyList
				<URI>());
			storage.SetStorageInfo(version);
			FilePath[] versionFiles = new FilePath[parent.Length];
			for (int i = 0; i < parent.Length; i++)
			{
				versionFiles[i] = new FilePath(parent[i], "VERSION");
				Storage.StorageDirectory sd = new Storage.StorageDirectory(parent[i].GetParentFile
					());
				storage.WriteProperties(versionFiles[i], sd);
			}
			return versionFiles;
		}

		/// <summary>
		/// Create a <code>version</code> file for datanode inside the specified parent
		/// directory.
		/// </summary>
		/// <remarks>
		/// Create a <code>version</code> file for datanode inside the specified parent
		/// directory.  If such a file already exists, it will be overwritten.
		/// The given version string will be written to the file as the layout
		/// version. None of the parameters may be null.
		/// </remarks>
		/// <param name="parent">directory where namenode VERSION file is stored</param>
		/// <param name="version">StorageInfo to create VERSION file from</param>
		/// <param name="bpid">Block pool Id</param>
		/// <exception cref="System.IO.IOException"/>
		public static void CreateDataNodeVersionFile(FilePath[] parent, StorageInfo version
			, string bpid)
		{
			CreateDataNodeVersionFile(parent, version, bpid, bpid);
		}

		/// <summary>
		/// Create a <code>version</code> file for datanode inside the specified parent
		/// directory.
		/// </summary>
		/// <remarks>
		/// Create a <code>version</code> file for datanode inside the specified parent
		/// directory.  If such a file already exists, it will be overwritten.
		/// The given version string will be written to the file as the layout
		/// version. None of the parameters may be null.
		/// </remarks>
		/// <param name="parent">directory where namenode VERSION file is stored</param>
		/// <param name="version">StorageInfo to create VERSION file from</param>
		/// <param name="bpid">Block pool Id</param>
		/// <param name="bpidToWrite">Block pool Id to write into the version file</param>
		/// <exception cref="System.IO.IOException"/>
		public static void CreateDataNodeVersionFile(FilePath[] parent, StorageInfo version
			, string bpid, string bpidToWrite)
		{
			DataStorage storage = new DataStorage(version);
			storage.SetDatanodeUuid("FixedDatanodeUuid");
			FilePath[] versionFiles = new FilePath[parent.Length];
			for (int i = 0; i < parent.Length; i++)
			{
				FilePath versionFile = new FilePath(parent[i], "VERSION");
				Storage.StorageDirectory sd = new Storage.StorageDirectory(parent[i].GetParentFile
					());
				storage.CreateStorageID(sd, false);
				storage.WriteProperties(versionFile, sd);
				versionFiles[i] = versionFile;
				FilePath bpDir = BlockPoolSliceStorage.GetBpRoot(bpid, parent[i]);
				CreateBlockPoolVersionFile(bpDir, version, bpidToWrite);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static void CreateBlockPoolVersionFile(FilePath bpDir, StorageInfo version
			, string bpid)
		{
			// Create block pool version files
			if (DataNodeLayoutVersion.Supports(LayoutVersion.Feature.Federation, version.layoutVersion
				))
			{
				FilePath bpCurDir = new FilePath(bpDir, Storage.StorageDirCurrent);
				BlockPoolSliceStorage bpStorage = new BlockPoolSliceStorage(version, bpid);
				FilePath versionFile = new FilePath(bpCurDir, "VERSION");
				Storage.StorageDirectory sd = new Storage.StorageDirectory(bpDir);
				bpStorage.WriteProperties(versionFile, sd);
			}
		}

		/// <summary>Corrupt the specified file.</summary>
		/// <remarks>
		/// Corrupt the specified file.  Some random bytes within the file
		/// will be changed to some random values.
		/// </remarks>
		/// <exception cref="System.ArgumentException">if the given file is not a file</exception>
		/// <exception cref="System.IO.IOException">if an IOException occurs while reading or writing the file
		/// 	</exception>
		public static void CorruptFile(FilePath file, byte[] stringToCorrupt, byte[] replacement
			)
		{
			Preconditions.CheckArgument(replacement.Length == stringToCorrupt.Length);
			if (!file.IsFile())
			{
				throw new ArgumentException("Given argument is not a file:" + file);
			}
			byte[] data = Files.ToByteArray(file);
			int index = Bytes.IndexOf(data, stringToCorrupt);
			if (index == -1)
			{
				throw new IOException("File " + file + " does not contain string " + Sharpen.Runtime.GetStringForBytes
					(stringToCorrupt));
			}
			for (int i = 0; i < stringToCorrupt.Length; i++)
			{
				data[index + i] = replacement[i];
			}
			Files.Write(data, file);
		}

		/// <summary>
		/// Return the layout version inherent in the current version
		/// of the Namenode, whether it is running or not.
		/// </summary>
		public static int GetCurrentNameNodeLayoutVersion()
		{
			return HdfsConstants.NamenodeLayoutVersion;
		}

		/// <summary>
		/// Return the namespace ID inherent in the currently running
		/// Namenode.
		/// </summary>
		/// <remarks>
		/// Return the namespace ID inherent in the currently running
		/// Namenode.  If no Namenode is running, return the namespace ID of
		/// the master Namenode storage directory.
		/// The UpgradeUtilities.initialize() method must be called once before
		/// calling this method.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public static int GetCurrentNamespaceID(MiniDFSCluster cluster)
		{
			if (cluster != null)
			{
				return cluster.GetNameNodeRpc().VersionRequest().GetNamespaceID();
			}
			return namenodeStorageNamespaceID;
		}

		/// <summary>
		/// Return the cluster ID inherent in the currently running
		/// Namenode.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public static string GetCurrentClusterID(MiniDFSCluster cluster)
		{
			if (cluster != null)
			{
				return cluster.GetNameNodeRpc().VersionRequest().GetClusterID();
			}
			return namenodeStorageClusterID;
		}

		/// <summary>
		/// Return the blockpool ID inherent in the currently running
		/// Namenode.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public static string GetCurrentBlockPoolID(MiniDFSCluster cluster)
		{
			if (cluster != null)
			{
				return cluster.GetNameNodeRpc().VersionRequest().GetBlockPoolID();
			}
			return namenodeStorageBlockPoolID;
		}

		/// <summary>
		/// Return the File System State Creation Timestamp (FSSCTime) inherent
		/// in the currently running Namenode.
		/// </summary>
		/// <remarks>
		/// Return the File System State Creation Timestamp (FSSCTime) inherent
		/// in the currently running Namenode.  If no Namenode is running,
		/// return the FSSCTime of the master Namenode storage directory.
		/// The UpgradeUtilities.initialize() method must be called once before
		/// calling this method.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public static long GetCurrentFsscTime(MiniDFSCluster cluster)
		{
			if (cluster != null)
			{
				return cluster.GetNameNodeRpc().VersionRequest().GetCTime();
			}
			return namenodeStorageFsscTime;
		}

		/// <summary>Create empty block pool directories</summary>
		/// <returns>array of block pool directories</returns>
		/// <exception cref="System.IO.IOException"/>
		public static string[] CreateEmptyBPDirs(string[] baseDirs, string bpid)
		{
			string[] bpDirs = new string[baseDirs.Length];
			for (int i = 0; i < baseDirs.Length; i++)
			{
				bpDirs[i] = MiniDFSCluster.GetBPDir(new FilePath(baseDirs[i]), bpid);
			}
			CreateEmptyDirs(bpDirs);
			return bpDirs;
		}
	}
}
