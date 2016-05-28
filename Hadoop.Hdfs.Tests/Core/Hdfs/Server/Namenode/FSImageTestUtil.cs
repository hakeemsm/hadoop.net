using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Com.Google.Common.IO;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.IO;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Utility functions for testing fsimage storage.</summary>
	public abstract class FSImageTestUtil
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(FSImageTestUtil));

		/// <summary>
		/// The position in the fsimage header where the txid is
		/// written.
		/// </summary>
		private const long ImageTxidPos = 24;

		/// <summary>This function returns a md5 hash of a file.</summary>
		/// <param name="file">input file</param>
		/// <returns>The md5 string</returns>
		/// <exception cref="System.IO.IOException"/>
		public static string GetFileMD5(FilePath file)
		{
			return MD5FileUtils.ComputeMd5ForFile(file).ToString();
		}

		/// <summary>
		/// Calculate the md5sum of an image after zeroing out the transaction ID
		/// field in the header.
		/// </summary>
		/// <remarks>
		/// Calculate the md5sum of an image after zeroing out the transaction ID
		/// field in the header. This is useful for tests that want to verify
		/// that two checkpoints have identical namespaces.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public static string GetImageFileMD5IgnoringTxId(FilePath imageFile)
		{
			FilePath tmpFile = FilePath.CreateTempFile("hadoop_imagefile_tmp", "fsimage");
			tmpFile.DeleteOnExit();
			try
			{
				Files.Copy(imageFile, tmpFile);
				RandomAccessFile raf = new RandomAccessFile(tmpFile, "rw");
				try
				{
					raf.Seek(ImageTxidPos);
					raf.WriteLong(0);
				}
				finally
				{
					IOUtils.CloseStream(raf);
				}
				return GetFileMD5(tmpFile);
			}
			finally
			{
				tmpFile.Delete();
			}
		}

		public static Storage.StorageDirectory MockStorageDirectory(FilePath currentDir, 
			NNStorage.NameNodeDirType type)
		{
			// Mock the StorageDirectory interface to just point to this file
			Storage.StorageDirectory sd = Org.Mockito.Mockito.Mock<Storage.StorageDirectory>(
				);
			Org.Mockito.Mockito.DoReturn(type).When(sd).GetStorageDirType();
			Org.Mockito.Mockito.DoReturn(currentDir).When(sd).GetCurrentDir();
			Org.Mockito.Mockito.DoReturn(currentDir).When(sd).GetRoot();
			Org.Mockito.Mockito.DoReturn(MockFile(true)).When(sd).GetVersionFile();
			Org.Mockito.Mockito.DoReturn(MockFile(false)).When(sd).GetPreviousDir();
			return sd;
		}

		/// <summary>Make a mock storage directory that returns some set of file contents.</summary>
		/// <param name="type">type of storage dir</param>
		/// <param name="previousExists">should we mock that the previous/ dir exists?</param>
		/// <param name="fileNames">the names of files contained in current/</param>
		internal static Storage.StorageDirectory MockStorageDirectory(Storage.StorageDirType
			 type, bool previousExists, params string[] fileNames)
		{
			Storage.StorageDirectory sd = Org.Mockito.Mockito.Mock<Storage.StorageDirectory>(
				);
			Org.Mockito.Mockito.DoReturn(type).When(sd).GetStorageDirType();
			// Version file should always exist
			Org.Mockito.Mockito.DoReturn(MockFile(true)).When(sd).GetVersionFile();
			Org.Mockito.Mockito.DoReturn(MockFile(true)).When(sd).GetRoot();
			// Previous dir optionally exists
			Org.Mockito.Mockito.DoReturn(MockFile(previousExists)).When(sd).GetPreviousDir();
			// Return a mock 'current' directory which has the given paths
			FilePath[] files = new FilePath[fileNames.Length];
			for (int i = 0; i < fileNames.Length; i++)
			{
				files[i] = new FilePath(fileNames[i]);
			}
			FilePath mockDir = Org.Mockito.Mockito.Spy(new FilePath("/dir/current"));
			Org.Mockito.Mockito.DoReturn(files).When(mockDir).ListFiles();
			Org.Mockito.Mockito.DoReturn(mockDir).When(sd).GetCurrentDir();
			return sd;
		}

		internal static FilePath MockFile(bool exists)
		{
			FilePath mockFile = Org.Mockito.Mockito.Mock<FilePath>();
			Org.Mockito.Mockito.DoReturn(exists).When(mockFile).Exists();
			return mockFile;
		}

		/// <exception cref="System.IO.IOException"/>
		public static FSImageTransactionalStorageInspector InspectStorageDirectory(FilePath
			 dir, NNStorage.NameNodeDirType dirType)
		{
			FSImageTransactionalStorageInspector inspector = new FSImageTransactionalStorageInspector
				();
			inspector.InspectDirectory(MockStorageDirectory(dir, dirType));
			return inspector;
		}

		/// <summary>
		/// Return a standalone instance of FSEditLog that will log into the given
		/// log directory.
		/// </summary>
		/// <remarks>
		/// Return a standalone instance of FSEditLog that will log into the given
		/// log directory. The returned instance is not yet opened.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public static FSEditLog CreateStandaloneEditLog(FilePath logDir)
		{
			NUnit.Framework.Assert.IsTrue(logDir.Mkdirs() || logDir.Exists());
			if (!FileUtil.FullyDeleteContents(logDir))
			{
				throw new IOException("Unable to delete contents of " + logDir);
			}
			NNStorage storage = Org.Mockito.Mockito.Mock<NNStorage>();
			Storage.StorageDirectory sd = FSImageTestUtil.MockStorageDirectory(logDir, NNStorage.NameNodeDirType
				.Edits);
			IList<Storage.StorageDirectory> sds = Lists.NewArrayList(sd);
			Org.Mockito.Mockito.DoReturn(sds).When(storage).DirIterable(NNStorage.NameNodeDirType
				.Edits);
			Org.Mockito.Mockito.DoReturn(sd).When(storage).GetStorageDirectory(Matchers.AnyObject
				<URI>());
			FSEditLog editLog = new FSEditLog(new Configuration(), storage, ImmutableList.Of(
				logDir.ToURI()));
			editLog.InitJournalsForWrite();
			return editLog;
		}

		/// <summary>
		/// Create an aborted in-progress log in the given directory, containing
		/// only a specified number of "mkdirs" operations.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public static void CreateAbortedLogWithMkdirs(FilePath editsLogDir, int numDirs, 
			long firstTxId, long newInodeId)
		{
			FSEditLog editLog = FSImageTestUtil.CreateStandaloneEditLog(editsLogDir);
			editLog.SetNextTxId(firstTxId);
			editLog.OpenForWrite();
			PermissionStatus perms = PermissionStatus.CreateImmutable("fakeuser", "fakegroup"
				, FsPermission.CreateImmutable((short)0x1ed));
			for (int i = 1; i <= numDirs; i++)
			{
				string dirName = "dir" + i;
				INodeDirectory dir = new INodeDirectory(newInodeId + i - 1, DFSUtil.String2Bytes(
					dirName), perms, 0L);
				editLog.LogMkDir("/" + dirName, dir);
			}
			editLog.LogSync();
			editLog.AbortCurrentLogSegment();
		}

		/// <param name="editLog">a path of an edit log file</param>
		/// <returns>the count of each type of operation in the log file</returns>
		/// <exception cref="System.Exception">if there is an error reading it</exception>
		public static EnumMap<FSEditLogOpCodes, Holder<int>> CountEditLogOpTypes(FilePath
			 editLog)
		{
			EditLogInputStream elis = new EditLogFileInputStream(editLog);
			try
			{
				return CountEditLogOpTypes(elis);
			}
			finally
			{
				IOUtils.CloseStream(elis);
			}
		}

		/// <seealso cref="CountEditLogOpTypes(Sharpen.FilePath)"/>
		/// <exception cref="System.IO.IOException"/>
		public static EnumMap<FSEditLogOpCodes, Holder<int>> CountEditLogOpTypes(EditLogInputStream
			 elis)
		{
			EnumMap<FSEditLogOpCodes, Holder<int>> opCounts = new EnumMap<FSEditLogOpCodes, Holder
				<int>>(typeof(FSEditLogOpCodes));
			FSEditLogOp op;
			while ((op = elis.ReadOp()) != null)
			{
				Holder<int> i = opCounts[op.opCode];
				if (i == null)
				{
					i = new Holder<int>(0);
					opCounts[op.opCode] = i;
				}
				i.held++;
			}
			return opCounts;
		}

		/// <summary>
		/// Assert that all of the given directories have the same newest filename
		/// for fsimage that they hold the same data.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public static void AssertSameNewestImage(IList<FilePath> dirs)
		{
			if (dirs.Count < 2)
			{
				return;
			}
			long imageTxId = -1;
			IList<FilePath> imageFiles = new AList<FilePath>();
			foreach (FilePath dir in dirs)
			{
				FSImageTransactionalStorageInspector inspector = InspectStorageDirectory(dir, NNStorage.NameNodeDirType
					.Image);
				IList<FSImageStorageInspector.FSImageFile> latestImages = inspector.GetLatestImages
					();
				System.Diagnostics.Debug.Assert((!latestImages.IsEmpty()));
				long thisTxId = latestImages[0].GetCheckpointTxId();
				if (imageTxId != -1 && thisTxId != imageTxId)
				{
					NUnit.Framework.Assert.Fail("Storage directory " + dir + " does not have the same "
						 + "last image index " + imageTxId + " as another");
				}
				imageTxId = thisTxId;
				imageFiles.AddItem(inspector.GetLatestImages()[0].GetFile());
			}
			AssertFileContentsSame(Sharpen.Collections.ToArray(imageFiles, new FilePath[0]));
		}

		/// <summary>
		/// Given a list of directories, assert that any files that are named
		/// the same thing have the same contents.
		/// </summary>
		/// <remarks>
		/// Given a list of directories, assert that any files that are named
		/// the same thing have the same contents. For example, if a file
		/// named "fsimage_1" shows up in more than one directory, then it must
		/// be the same.
		/// </remarks>
		/// <exception cref="System.Exception"></exception>
		public static void AssertParallelFilesAreIdentical(IList<FilePath> dirs, ICollection
			<string> ignoredFileNames)
		{
			Dictionary<string, IList<FilePath>> groupedByName = new Dictionary<string, IList<
				FilePath>>();
			foreach (FilePath dir in dirs)
			{
				foreach (FilePath f in dir.ListFiles())
				{
					if (ignoredFileNames.Contains(f.GetName()))
					{
						continue;
					}
					IList<FilePath> fileList = groupedByName[f.GetName()];
					if (fileList == null)
					{
						fileList = new AList<FilePath>();
						groupedByName[f.GetName()] = fileList;
					}
					fileList.AddItem(f);
				}
			}
			foreach (IList<FilePath> sameNameList in groupedByName.Values)
			{
				if (sameNameList[0].IsDirectory())
				{
					// recurse
					AssertParallelFilesAreIdentical(sameNameList, ignoredFileNames);
				}
				else
				{
					if ("VERSION".Equals(sameNameList[0].GetName()))
					{
						AssertPropertiesFilesSame(Sharpen.Collections.ToArray(sameNameList, new FilePath[
							0]));
					}
					else
					{
						AssertFileContentsSame(Sharpen.Collections.ToArray(sameNameList, new FilePath[0])
							);
					}
				}
			}
		}

		/// <summary>Assert that a set of properties files all contain the same data.</summary>
		/// <remarks>
		/// Assert that a set of properties files all contain the same data.
		/// We cannot simply check the md5sums here, since Properties files
		/// contain timestamps -- thus, two properties files from the same
		/// saveNamespace operation may actually differ in md5sum.
		/// </remarks>
		/// <param name="propFiles">the files to compare</param>
		/// <exception cref="System.IO.IOException">if the files cannot be opened or read</exception>
		/// <exception cref="System.Exception">if the files differ</exception>
		public static void AssertPropertiesFilesSame(FilePath[] propFiles)
		{
			ICollection<KeyValuePair<object, object>> prevProps = null;
			foreach (FilePath f in propFiles)
			{
				Properties props;
				FileInputStream @is = new FileInputStream(f);
				try
				{
					props = new Properties();
					props.Load(@is);
				}
				finally
				{
					IOUtils.CloseStream(@is);
				}
				if (prevProps == null)
				{
					prevProps = props;
				}
				else
				{
					ICollection<KeyValuePair<object, object>> diff = Sets.SymmetricDifference(prevProps
						, props);
					if (!diff.IsEmpty())
					{
						NUnit.Framework.Assert.Fail("Properties file " + f + " differs from " + propFiles
							[0]);
					}
				}
			}
		}

		/// <summary>
		/// Assert that all of the given paths have the exact same
		/// contents
		/// </summary>
		/// <exception cref="System.Exception"/>
		public static void AssertFileContentsSame(params FilePath[] files)
		{
			if (files.Length < 2)
			{
				return;
			}
			IDictionary<FilePath, string> md5s = GetFileMD5s(files);
			if (Sets.NewHashSet(md5s.Values).Count > 1)
			{
				NUnit.Framework.Assert.Fail("File contents differed:\n  " + Joiner.On("\n  ").WithKeyValueSeparator
					("=").Join(md5s));
			}
		}

		/// <summary>
		/// Assert that the given files are not all the same, and in fact that
		/// they have <code>expectedUniqueHashes</code> unique contents.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public static void AssertFileContentsDifferent(int expectedUniqueHashes, params FilePath
			[] files)
		{
			IDictionary<FilePath, string> md5s = GetFileMD5s(files);
			if (Sets.NewHashSet(md5s.Values).Count != expectedUniqueHashes)
			{
				NUnit.Framework.Assert.Fail("Expected " + expectedUniqueHashes + " different hashes, got:\n  "
					 + Joiner.On("\n  ").WithKeyValueSeparator("=").Join(md5s));
			}
		}

		/// <exception cref="System.Exception"/>
		public static IDictionary<FilePath, string> GetFileMD5s(params FilePath[] files)
		{
			IDictionary<FilePath, string> ret = Maps.NewHashMap();
			foreach (FilePath f in files)
			{
				NUnit.Framework.Assert.IsTrue("Must exist: " + f, f.Exists());
				ret[f] = GetFileMD5(f);
			}
			return ret;
		}

		/// <returns>
		/// a List which contains the "current" dir for each storage
		/// directory of the given type.
		/// </returns>
		public static IList<FilePath> GetCurrentDirs(NNStorage storage, NNStorage.NameNodeDirType
			 type)
		{
			IList<FilePath> ret = Lists.NewArrayList();
			foreach (Storage.StorageDirectory sd in storage.DirIterable(type))
			{
				ret.AddItem(sd.GetCurrentDir());
			}
			return ret;
		}

		/// <returns>
		/// the fsimage file with the most recent transaction ID in the
		/// given storage directory.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public static FilePath FindLatestImageFile(Storage.StorageDirectory sd)
		{
			FSImageTransactionalStorageInspector inspector = new FSImageTransactionalStorageInspector
				();
			inspector.InspectDirectory(sd);
			return inspector.GetLatestImages()[0].GetFile();
		}

		/// <returns>
		/// the fsimage file with the most recent transaction ID in the
		/// given 'current/' directory.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public static FilePath FindNewestImageFile(string currentDirPath)
		{
			Storage.StorageDirectory sd = FSImageTestUtil.MockStorageDirectory(new FilePath(currentDirPath
				), NNStorage.NameNodeDirType.Image);
			FSImageTransactionalStorageInspector inspector = new FSImageTransactionalStorageInspector
				();
			inspector.InspectDirectory(sd);
			IList<FSImageStorageInspector.FSImageFile> latestImages = inspector.GetLatestImages
				();
			return (latestImages.IsEmpty()) ? null : latestImages[0].GetFile();
		}

		/// <summary>
		/// Assert that the NameNode has checkpoints at the expected
		/// transaction IDs.
		/// </summary>
		public static void AssertNNHasCheckpoints(MiniDFSCluster cluster, IList<int> txids
			)
		{
			AssertNNHasCheckpoints(cluster, 0, txids);
		}

		public static void AssertNNHasCheckpoints(MiniDFSCluster cluster, int nnIdx, IList
			<int> txids)
		{
			foreach (FilePath nameDir in GetNameNodeCurrentDirs(cluster, nnIdx))
			{
				Log.Info("examining name dir with files: " + Joiner.On(",").Join(nameDir.ListFiles
					()));
				// Should have fsimage_N for the three checkpoints
				Log.Info("Examining storage dir " + nameDir + " with contents: " + StringUtils.Join
					(nameDir.ListFiles(), ", "));
				foreach (long checkpointTxId in txids)
				{
					FilePath image = new FilePath(nameDir, NNStorage.GetImageFileName(checkpointTxId)
						);
					NUnit.Framework.Assert.IsTrue("Expected non-empty " + image, image.Length() > 0);
				}
			}
		}

		public static IList<FilePath> GetNameNodeCurrentDirs(MiniDFSCluster cluster, int 
			nnIdx)
		{
			IList<FilePath> nameDirs = Lists.NewArrayList();
			foreach (URI u in cluster.GetNameDirs(nnIdx))
			{
				nameDirs.AddItem(new FilePath(u.GetPath(), "current"));
			}
			return nameDirs;
		}

		/// <returns>
		/// the latest edits log, finalized or otherwise, from the given
		/// storage directory.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public static FileJournalManager.EditLogFile FindLatestEditsLog(Storage.StorageDirectory
			 sd)
		{
			FilePath currentDir = sd.GetCurrentDir();
			IList<FileJournalManager.EditLogFile> foundEditLogs = Lists.NewArrayList(FileJournalManager
				.MatchEditLogs(currentDir));
			return Sharpen.Collections.Max(foundEditLogs, FileJournalManager.EditLogFile.CompareByStartTxid
				);
		}

		/// <summary>
		/// Corrupt the given VERSION file by replacing a given
		/// key with a new value and re-writing the file.
		/// </summary>
		/// <param name="versionFile">the VERSION file to corrupt</param>
		/// <param name="key">the key to replace</param>
		/// <param name="value">the new value for this key</param>
		/// <exception cref="System.IO.IOException"/>
		public static void CorruptVersionFile(FilePath versionFile, string key, string value
			)
		{
			Properties props = new Properties();
			FileInputStream fis = new FileInputStream(versionFile);
			FileOutputStream @out = null;
			try
			{
				props.Load(fis);
				IOUtils.CloseStream(fis);
				if (value == null || value.IsEmpty())
				{
					props.Remove(key);
				}
				else
				{
					props.SetProperty(key, value);
				}
				@out = new FileOutputStream(versionFile);
				props.Store(@out, null);
			}
			finally
			{
				IOUtils.Cleanup(null, fis, @out);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static void AssertReasonableNameCurrentDir(FilePath curDir)
		{
			NUnit.Framework.Assert.IsTrue(curDir.IsDirectory());
			NUnit.Framework.Assert.IsTrue(new FilePath(curDir, "VERSION").IsFile());
			NUnit.Framework.Assert.IsTrue(new FilePath(curDir, "seen_txid").IsFile());
			FilePath image = FindNewestImageFile(curDir.ToString());
			NUnit.Framework.Assert.IsNotNull(image);
		}

		public static void LogStorageContents(Log Log, NNStorage storage)
		{
			Log.Info("current storages and corresponding sizes:");
			foreach (Storage.StorageDirectory sd in storage.DirIterable(null))
			{
				FilePath curDir = sd.GetCurrentDir();
				Log.Info("In directory " + curDir);
				FilePath[] files = curDir.ListFiles();
				Arrays.Sort(files);
				foreach (FilePath f in files)
				{
					Log.Info("  file " + f.GetAbsolutePath() + "; len = " + f.Length());
				}
			}
		}

		/// <summary>get the fsImage</summary>
		public static FSImage GetFSImage(NameNode node)
		{
			return node.GetFSImage();
		}

		/// <summary>get NameSpace quota.</summary>
		public static long GetNSQuota(FSNamesystem ns)
		{
			return ns.dir.rootDir.GetQuotaCounts().GetNameSpace();
		}

		/// <exception cref="System.Exception"/>
		public static void AssertNNFilesMatch(MiniDFSCluster cluster)
		{
			IList<FilePath> curDirs = Lists.NewArrayList();
			Sharpen.Collections.AddAll(curDirs, FSImageTestUtil.GetNameNodeCurrentDirs(cluster
				, 0));
			Sharpen.Collections.AddAll(curDirs, FSImageTestUtil.GetNameNodeCurrentDirs(cluster
				, 1));
			// Ignore seen_txid file, since the newly bootstrapped standby
			// will have a higher seen_txid than the one it bootstrapped from.
			ICollection<string> ignoredFiles = ImmutableSet.Of("seen_txid");
			FSImageTestUtil.AssertParallelFilesAreIdentical(curDirs, ignoredFiles);
		}
	}
}
