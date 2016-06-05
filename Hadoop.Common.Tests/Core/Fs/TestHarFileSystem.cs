using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;

using Reflect;

namespace Org.Apache.Hadoop.FS
{
	public class TestHarFileSystem
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestHarFileSystem));

		/// <summary>
		/// FileSystem methods that must not be overwritten by
		/// <see cref="HarFileSystem"/>
		/// . Either because there is a default implementation
		/// already available or because it is not relevant.
		/// </summary>
		private interface MustNotImplement
		{
			BlockLocation[] GetFileBlockLocations(Path p, long start, long len);

			long GetLength(Path f);

			FSDataOutputStream Append(Path f, int bufferSize);

			void Rename(Path src, Path dst, params Options.Rename[] options);

			bool Exists(Path f);

			bool IsDirectory(Path f);

			bool IsFile(Path f);

			bool CreateNewFile(Path f);

			/// <exception cref="System.IO.IOException"/>
			FSDataOutputStream CreateNonRecursive(Path f, FsPermission permission, bool overwrite
				, int bufferSize, short replication, long blockSize, Progressable progress);

			/// <exception cref="System.IO.IOException"/>
			FSDataOutputStream CreateNonRecursive(Path f, FsPermission permission, EnumSet<CreateFlag
				> flags, int bufferSize, short replication, long blockSize, Progressable progress
				);

			FSDataOutputStream CreateNonRecursive(Path f, FsPermission permission, EnumSet<CreateFlag
				> flags, int bufferSize, short replication, long blockSize, Progressable progress
				, Options.ChecksumOpt checksumOpt);

			bool Mkdirs(Path f);

			FSDataInputStream Open(Path f);

			FSDataOutputStream Create(Path f);

			FSDataOutputStream Create(Path f, bool overwrite);

			FSDataOutputStream Create(Path f, Progressable progress);

			FSDataOutputStream Create(Path f, short replication);

			FSDataOutputStream Create(Path f, short replication, Progressable progress);

			FSDataOutputStream Create(Path f, bool overwrite, int bufferSize);

			FSDataOutputStream Create(Path f, bool overwrite, int bufferSize, Progressable progress
				);

			FSDataOutputStream Create(Path f, bool overwrite, int bufferSize, short replication
				, long blockSize);

			FSDataOutputStream Create(Path f, bool overwrite, int bufferSize, short replication
				, long blockSize, Progressable progress);

			/// <exception cref="System.IO.IOException"/>
			FSDataOutputStream Create(Path f, FsPermission permission, EnumSet<CreateFlag> flags
				, int bufferSize, short replication, long blockSize, Progressable progress);

			/// <exception cref="System.IO.IOException"/>
			FSDataOutputStream Create(Path f, FsPermission permission, EnumSet<CreateFlag> flags
				, int bufferSize, short replication, long blockSize, Progressable progress, Options.ChecksumOpt
				 checksumOpt);

			string GetName();

			bool Delete(Path f);

			short GetReplication(Path src);

			void ProcessDeleteOnExit();

			ContentSummary GetContentSummary(Path f);

			FsStatus GetStatus();

			FileStatus[] ListStatus(Path f, PathFilter filter);

			FileStatus[] ListStatus(Path[] files);

			FileStatus[] ListStatus(Path[] files, PathFilter filter);

			FileStatus[] GlobStatus(Path pathPattern);

			FileStatus[] GlobStatus(Path pathPattern, PathFilter filter);

			IEnumerator<LocatedFileStatus> ListFiles(Path path, bool isRecursive);

			IEnumerator<LocatedFileStatus> ListLocatedStatus(Path f);

			IEnumerator<LocatedFileStatus> ListLocatedStatus(Path f, PathFilter filter);

			IEnumerator<FileStatus> ListStatusIterator(Path f);

			void CopyFromLocalFile(Path src, Path dst);

			void MoveFromLocalFile(Path[] srcs, Path dst);

			void MoveFromLocalFile(Path src, Path dst);

			void CopyToLocalFile(Path src, Path dst);

			void CopyToLocalFile(bool delSrc, Path src, Path dst, bool useRawLocalFileSystem);

			void MoveToLocalFile(Path src, Path dst);

			long GetBlockSize(Path f);

			FSDataOutputStream PrimitiveCreate(Path f, EnumSet<CreateFlag> createFlag, params 
				Options.CreateOpts[] opts);

			void PrimitiveMkdir(Path f, FsPermission absolutePermission, bool createParent);

			int GetDefaultPort();

			string GetCanonicalServiceName();

			/// <exception cref="System.IO.IOException"/>
			Org.Apache.Hadoop.Security.Token.Token<object> GetDelegationToken(string renewer);

			/// <exception cref="System.IO.IOException"/>
			FileChecksum GetFileChecksum(Path f);

			/// <exception cref="System.IO.IOException"/>
			bool DeleteOnExit(Path f);

			/// <exception cref="System.IO.IOException"/>
			bool CancelDeleteOnExit(Path f);

			/// <exception cref="System.IO.IOException"/>
			Org.Apache.Hadoop.Security.Token.Token<object>[] AddDelegationTokens(string renewer
				, Credentials creds);

			Path FixRelativePart(Path p);

			/// <exception cref="System.IO.IOException"/>
			void Concat(Path trg, Path[] psrcs);

			/// <exception cref="System.IO.IOException"/>
			FSDataOutputStream PrimitiveCreate(Path f, FsPermission absolutePermission, EnumSet
				<CreateFlag> flag, int bufferSize, short replication, long blockSize, Progressable
				 progress, Options.ChecksumOpt checksumOpt);

			/// <exception cref="System.IO.IOException"/>
			bool PrimitiveMkdir(Path f, FsPermission absolutePermission);

			/// <exception cref="System.IO.IOException"/>
			RemoteIterator<Path> ListCorruptFileBlocks(Path path);

			/// <exception cref="System.IO.IOException"/>
			void CopyFromLocalFile(bool delSrc, Path src, Path dst);

			/// <exception cref="System.IO.IOException"/>
			void CreateSymlink(Path target, Path link, bool createParent);

			/// <exception cref="System.IO.IOException"/>
			FileStatus GetFileLinkStatus(Path f);

			bool SupportsSymlinks();

			/// <exception cref="System.IO.IOException"/>
			Path GetLinkTarget(Path f);

			/// <exception cref="System.IO.IOException"/>
			Path ResolveLink(Path f);

			void SetVerifyChecksum(bool verifyChecksum);

			void SetWriteChecksum(bool writeChecksum);

			/// <exception cref="System.IO.IOException"/>
			Path CreateSnapshot(Path path, string snapshotName);

			/// <exception cref="System.IO.IOException"/>
			void RenameSnapshot(Path path, string snapshotOldName, string snapshotNewName);

			/// <exception cref="System.IO.IOException"/>
			void DeleteSnapshot(Path path, string snapshotName);

			/// <exception cref="System.IO.IOException"/>
			void ModifyAclEntries(Path path, IList<AclEntry> aclSpec);

			/// <exception cref="System.IO.IOException"/>
			void RemoveAclEntries(Path path, IList<AclEntry> aclSpec);

			/// <exception cref="System.IO.IOException"/>
			void RemoveDefaultAcl(Path path);

			/// <exception cref="System.IO.IOException"/>
			void RemoveAcl(Path path);

			/// <exception cref="System.IO.IOException"/>
			void SetAcl(Path path, IList<AclEntry> aclSpec);

			/// <exception cref="System.IO.IOException"/>
			void SetXAttr(Path path, string name, byte[] value);

			/// <exception cref="System.IO.IOException"/>
			void SetXAttr(Path path, string name, byte[] value, EnumSet<XAttrSetFlag> flag);

			/// <exception cref="System.IO.IOException"/>
			byte[] GetXAttr(Path path, string name);

			/// <exception cref="System.IO.IOException"/>
			IDictionary<string, byte[]> GetXAttrs(Path path);

			/// <exception cref="System.IO.IOException"/>
			IDictionary<string, byte[]> GetXAttrs(Path path, IList<string> names);

			/// <exception cref="System.IO.IOException"/>
			IList<string> ListXAttrs(Path path);

			/// <exception cref="System.IO.IOException"/>
			void RemoveXAttr(Path path, string name);

			/// <exception cref="System.IO.IOException"/>
			AclStatus GetAclStatus(Path path);

			/// <exception cref="System.IO.IOException"/>
			void Access(Path path, FsAction mode);
		}

		[Fact]
		public virtual void TestHarUri()
		{
			Configuration conf = new Configuration();
			CheckInvalidPath("har://hdfs-/foo.har", conf);
			CheckInvalidPath("har://hdfs/foo.har", conf);
			CheckInvalidPath("har://-hdfs/foo.har", conf);
			CheckInvalidPath("har://-/foo.har", conf);
			CheckInvalidPath("har://127.0.0.1-/foo.har", conf);
			CheckInvalidPath("har://127.0.0.1/foo.har", conf);
		}

		internal static void CheckInvalidPath(string s, Configuration conf)
		{
			System.Console.Out.WriteLine("\ncheckInvalidPath: " + s);
			Path p = new Path(s);
			try
			{
				p.GetFileSystem(conf);
				NUnit.Framework.Assert.Fail(p + " is an invalid path.");
			}
			catch (IOException)
			{
			}
		}

		// Expected
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestFileChecksum()
		{
			Path p = new Path("har://file-localhost/foo.har/file1");
			HarFileSystem harfs = new HarFileSystem();
			try
			{
				Assert.Equal(null, harfs.GetFileChecksum(p));
			}
			finally
			{
				if (harfs != null)
				{
					harfs.Close();
				}
			}
		}

		/// <summary>Test how block location offsets and lengths are fixed.</summary>
		[Fact]
		public virtual void TestFixBlockLocations()
		{
			{
				// do some tests where start == 0
				// case 1: range starts before current har block and ends after
				BlockLocation[] b = new BlockLocation[] { new BlockLocation(null, null, 10, 10) };
				HarFileSystem.FixBlockLocations(b, 0, 20, 5);
				Assert.Equal(b[0].GetOffset(), 5);
				Assert.Equal(b[0].GetLength(), 10);
			}
			{
				// case 2: range starts in current har block and ends after
				BlockLocation[] b = new BlockLocation[] { new BlockLocation(null, null, 10, 10) };
				HarFileSystem.FixBlockLocations(b, 0, 20, 15);
				Assert.Equal(b[0].GetOffset(), 0);
				Assert.Equal(b[0].GetLength(), 5);
			}
			{
				// case 3: range starts before current har block and ends in
				// current har block
				BlockLocation[] b = new BlockLocation[] { new BlockLocation(null, null, 10, 10) };
				HarFileSystem.FixBlockLocations(b, 0, 10, 5);
				Assert.Equal(b[0].GetOffset(), 5);
				Assert.Equal(b[0].GetLength(), 5);
			}
			{
				// case 4: range starts and ends in current har block
				BlockLocation[] b = new BlockLocation[] { new BlockLocation(null, null, 10, 10) };
				HarFileSystem.FixBlockLocations(b, 0, 6, 12);
				Assert.Equal(b[0].GetOffset(), 0);
				Assert.Equal(b[0].GetLength(), 6);
			}
			{
				// now try a range where start == 3
				// case 5: range starts before current har block and ends after
				BlockLocation[] b = new BlockLocation[] { new BlockLocation(null, null, 10, 10) };
				HarFileSystem.FixBlockLocations(b, 3, 20, 5);
				Assert.Equal(b[0].GetOffset(), 5);
				Assert.Equal(b[0].GetLength(), 10);
			}
			{
				// case 6: range starts in current har block and ends after
				BlockLocation[] b = new BlockLocation[] { new BlockLocation(null, null, 10, 10) };
				HarFileSystem.FixBlockLocations(b, 3, 20, 15);
				Assert.Equal(b[0].GetOffset(), 3);
				Assert.Equal(b[0].GetLength(), 2);
			}
			{
				// case 7: range starts before current har block and ends in
				// current har block
				BlockLocation[] b = new BlockLocation[] { new BlockLocation(null, null, 10, 10) };
				HarFileSystem.FixBlockLocations(b, 3, 7, 5);
				Assert.Equal(b[0].GetOffset(), 5);
				Assert.Equal(b[0].GetLength(), 5);
			}
			{
				// case 8: range starts and ends in current har block
				BlockLocation[] b = new BlockLocation[] { new BlockLocation(null, null, 10, 10) };
				HarFileSystem.FixBlockLocations(b, 3, 3, 12);
				Assert.Equal(b[0].GetOffset(), 3);
				Assert.Equal(b[0].GetLength(), 3);
			}
			{
				// test case from JIRA MAPREDUCE-1752
				BlockLocation[] b = new BlockLocation[] { new BlockLocation(null, null, 512, 512)
					, new BlockLocation(null, null, 1024, 512) };
				HarFileSystem.FixBlockLocations(b, 0, 512, 896);
				Assert.Equal(b[0].GetOffset(), 0);
				Assert.Equal(b[0].GetLength(), 128);
				Assert.Equal(b[1].GetOffset(), 128);
				Assert.Equal(b[1].GetLength(), 384);
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestInheritedMethodsImplemented()
		{
			int errors = 0;
			foreach (MethodInfo m in Runtime.GetDeclaredMethods(typeof(FileSystem)))
			{
				if (Modifier.IsStatic(m.GetModifiers()) || Modifier.IsPrivate(m.GetModifiers()) ||
					 Modifier.IsFinal(m.GetModifiers()))
				{
					continue;
				}
				try
				{
					typeof(TestHarFileSystem.MustNotImplement).GetMethod(m.Name, Runtime.GetParameterTypes
						(m));
					try
					{
						Runtime.GetDeclaredMethod(typeof(HarFileSystem), m.Name, Runtime.GetParameterTypes
							(m));
						Log.Error("HarFileSystem MUST not implement " + m);
						errors++;
					}
					catch (MissingMethodException)
					{
					}
				}
				catch (MissingMethodException)
				{
					// Expected
					try
					{
						Runtime.GetDeclaredMethod(typeof(HarFileSystem), m.Name, Runtime.GetParameterTypes
							(m));
					}
					catch (MissingMethodException)
					{
						Log.Error("HarFileSystem MUST implement " + m);
						errors++;
					}
				}
			}
			Assert.True((errors + " methods were not overridden correctly - see log"
				), errors <= 0);
		}
	}
}
