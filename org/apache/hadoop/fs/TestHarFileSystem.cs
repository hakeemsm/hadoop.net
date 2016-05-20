using Sharpen;

namespace org.apache.hadoop.fs
{
	public class TestHarFileSystem
	{
		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.TestHarFileSystem
			)));

		/// <summary>
		/// FileSystem methods that must not be overwritten by
		/// <see cref="HarFileSystem"/>
		/// . Either because there is a default implementation
		/// already available or because it is not relevant.
		/// </summary>
		private interface MustNotImplement
		{
			org.apache.hadoop.fs.BlockLocation[] getFileBlockLocations(org.apache.hadoop.fs.Path
				 p, long start, long len);

			long getLength(org.apache.hadoop.fs.Path f);

			org.apache.hadoop.fs.FSDataOutputStream append(org.apache.hadoop.fs.Path f, int bufferSize
				);

			void rename(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path dst, params 
				org.apache.hadoop.fs.Options.Rename[] options);

			bool exists(org.apache.hadoop.fs.Path f);

			bool isDirectory(org.apache.hadoop.fs.Path f);

			bool isFile(org.apache.hadoop.fs.Path f);

			bool createNewFile(org.apache.hadoop.fs.Path f);

			/// <exception cref="System.IO.IOException"/>
			org.apache.hadoop.fs.FSDataOutputStream createNonRecursive(org.apache.hadoop.fs.Path
				 f, org.apache.hadoop.fs.permission.FsPermission permission, bool overwrite, int
				 bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
				 progress);

			/// <exception cref="System.IO.IOException"/>
			org.apache.hadoop.fs.FSDataOutputStream createNonRecursive(org.apache.hadoop.fs.Path
				 f, org.apache.hadoop.fs.permission.FsPermission permission, java.util.EnumSet<org.apache.hadoop.fs.CreateFlag
				> flags, int bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
				 progress);

			org.apache.hadoop.fs.FSDataOutputStream createNonRecursive(org.apache.hadoop.fs.Path
				 f, org.apache.hadoop.fs.permission.FsPermission permission, java.util.EnumSet<org.apache.hadoop.fs.CreateFlag
				> flags, int bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
				 progress, org.apache.hadoop.fs.Options.ChecksumOpt checksumOpt);

			bool mkdirs(org.apache.hadoop.fs.Path f);

			org.apache.hadoop.fs.FSDataInputStream open(org.apache.hadoop.fs.Path f);

			org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path f);

			org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path f, bool 
				overwrite);

			org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path f, org.apache.hadoop.util.Progressable
				 progress);

			org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path f, short
				 replication);

			org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path f, short
				 replication, org.apache.hadoop.util.Progressable progress);

			org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path f, bool 
				overwrite, int bufferSize);

			org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path f, bool 
				overwrite, int bufferSize, org.apache.hadoop.util.Progressable progress);

			org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path f, bool 
				overwrite, int bufferSize, short replication, long blockSize);

			org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path f, bool 
				overwrite, int bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
				 progress);

			/// <exception cref="System.IO.IOException"/>
			org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.permission.FsPermission
				 permission, java.util.EnumSet<org.apache.hadoop.fs.CreateFlag> flags, int bufferSize
				, short replication, long blockSize, org.apache.hadoop.util.Progressable progress
				);

			/// <exception cref="System.IO.IOException"/>
			org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.permission.FsPermission
				 permission, java.util.EnumSet<org.apache.hadoop.fs.CreateFlag> flags, int bufferSize
				, short replication, long blockSize, org.apache.hadoop.util.Progressable progress
				, org.apache.hadoop.fs.Options.ChecksumOpt checksumOpt);

			string getName();

			bool delete(org.apache.hadoop.fs.Path f);

			short getReplication(org.apache.hadoop.fs.Path src);

			void processDeleteOnExit();

			org.apache.hadoop.fs.ContentSummary getContentSummary(org.apache.hadoop.fs.Path f
				);

			org.apache.hadoop.fs.FsStatus getStatus();

			org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.PathFilter
				 filter);

			org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path[] files);

			org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path[] files, org.apache.hadoop.fs.PathFilter
				 filter);

			org.apache.hadoop.fs.FileStatus[] globStatus(org.apache.hadoop.fs.Path pathPattern
				);

			org.apache.hadoop.fs.FileStatus[] globStatus(org.apache.hadoop.fs.Path pathPattern
				, org.apache.hadoop.fs.PathFilter filter);

			System.Collections.Generic.IEnumerator<org.apache.hadoop.fs.LocatedFileStatus> listFiles
				(org.apache.hadoop.fs.Path path, bool isRecursive);

			System.Collections.Generic.IEnumerator<org.apache.hadoop.fs.LocatedFileStatus> listLocatedStatus
				(org.apache.hadoop.fs.Path f);

			System.Collections.Generic.IEnumerator<org.apache.hadoop.fs.LocatedFileStatus> listLocatedStatus
				(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.PathFilter filter);

			System.Collections.Generic.IEnumerator<org.apache.hadoop.fs.FileStatus> listStatusIterator
				(org.apache.hadoop.fs.Path f);

			void copyFromLocalFile(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path dst
				);

			void moveFromLocalFile(org.apache.hadoop.fs.Path[] srcs, org.apache.hadoop.fs.Path
				 dst);

			void moveFromLocalFile(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path dst
				);

			void copyToLocalFile(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path dst
				);

			void copyToLocalFile(bool delSrc, org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
				 dst, bool useRawLocalFileSystem);

			void moveToLocalFile(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path dst
				);

			long getBlockSize(org.apache.hadoop.fs.Path f);

			org.apache.hadoop.fs.FSDataOutputStream primitiveCreate(org.apache.hadoop.fs.Path
				 f, java.util.EnumSet<org.apache.hadoop.fs.CreateFlag> createFlag, params org.apache.hadoop.fs.Options.CreateOpts
				[] opts);

			void primitiveMkdir(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.permission.FsPermission
				 absolutePermission, bool createParent);

			int getDefaultPort();

			string getCanonicalServiceName();

			/// <exception cref="System.IO.IOException"/>
			org.apache.hadoop.security.token.Token<object> getDelegationToken(string renewer);

			/// <exception cref="System.IO.IOException"/>
			org.apache.hadoop.fs.FileChecksum getFileChecksum(org.apache.hadoop.fs.Path f);

			/// <exception cref="System.IO.IOException"/>
			bool deleteOnExit(org.apache.hadoop.fs.Path f);

			/// <exception cref="System.IO.IOException"/>
			bool cancelDeleteOnExit(org.apache.hadoop.fs.Path f);

			/// <exception cref="System.IO.IOException"/>
			org.apache.hadoop.security.token.Token<object>[] addDelegationTokens(string renewer
				, org.apache.hadoop.security.Credentials creds);

			org.apache.hadoop.fs.Path fixRelativePart(org.apache.hadoop.fs.Path p);

			/// <exception cref="System.IO.IOException"/>
			void concat(org.apache.hadoop.fs.Path trg, org.apache.hadoop.fs.Path[] psrcs);

			/// <exception cref="System.IO.IOException"/>
			org.apache.hadoop.fs.FSDataOutputStream primitiveCreate(org.apache.hadoop.fs.Path
				 f, org.apache.hadoop.fs.permission.FsPermission absolutePermission, java.util.EnumSet
				<org.apache.hadoop.fs.CreateFlag> flag, int bufferSize, short replication, long 
				blockSize, org.apache.hadoop.util.Progressable progress, org.apache.hadoop.fs.Options.ChecksumOpt
				 checksumOpt);

			/// <exception cref="System.IO.IOException"/>
			bool primitiveMkdir(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.permission.FsPermission
				 absolutePermission);

			/// <exception cref="System.IO.IOException"/>
			org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.Path> listCorruptFileBlocks
				(org.apache.hadoop.fs.Path path);

			/// <exception cref="System.IO.IOException"/>
			void copyFromLocalFile(bool delSrc, org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
				 dst);

			/// <exception cref="System.IO.IOException"/>
			void createSymlink(org.apache.hadoop.fs.Path target, org.apache.hadoop.fs.Path link
				, bool createParent);

			/// <exception cref="System.IO.IOException"/>
			org.apache.hadoop.fs.FileStatus getFileLinkStatus(org.apache.hadoop.fs.Path f);

			bool supportsSymlinks();

			/// <exception cref="System.IO.IOException"/>
			org.apache.hadoop.fs.Path getLinkTarget(org.apache.hadoop.fs.Path f);

			/// <exception cref="System.IO.IOException"/>
			org.apache.hadoop.fs.Path resolveLink(org.apache.hadoop.fs.Path f);

			void setVerifyChecksum(bool verifyChecksum);

			void setWriteChecksum(bool writeChecksum);

			/// <exception cref="System.IO.IOException"/>
			org.apache.hadoop.fs.Path createSnapshot(org.apache.hadoop.fs.Path path, string snapshotName
				);

			/// <exception cref="System.IO.IOException"/>
			void renameSnapshot(org.apache.hadoop.fs.Path path, string snapshotOldName, string
				 snapshotNewName);

			/// <exception cref="System.IO.IOException"/>
			void deleteSnapshot(org.apache.hadoop.fs.Path path, string snapshotName);

			/// <exception cref="System.IO.IOException"/>
			void modifyAclEntries(org.apache.hadoop.fs.Path path, System.Collections.Generic.IList
				<org.apache.hadoop.fs.permission.AclEntry> aclSpec);

			/// <exception cref="System.IO.IOException"/>
			void removeAclEntries(org.apache.hadoop.fs.Path path, System.Collections.Generic.IList
				<org.apache.hadoop.fs.permission.AclEntry> aclSpec);

			/// <exception cref="System.IO.IOException"/>
			void removeDefaultAcl(org.apache.hadoop.fs.Path path);

			/// <exception cref="System.IO.IOException"/>
			void removeAcl(org.apache.hadoop.fs.Path path);

			/// <exception cref="System.IO.IOException"/>
			void setAcl(org.apache.hadoop.fs.Path path, System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry
				> aclSpec);

			/// <exception cref="System.IO.IOException"/>
			void setXAttr(org.apache.hadoop.fs.Path path, string name, byte[] value);

			/// <exception cref="System.IO.IOException"/>
			void setXAttr(org.apache.hadoop.fs.Path path, string name, byte[] value, java.util.EnumSet
				<org.apache.hadoop.fs.XAttrSetFlag> flag);

			/// <exception cref="System.IO.IOException"/>
			byte[] getXAttr(org.apache.hadoop.fs.Path path, string name);

			/// <exception cref="System.IO.IOException"/>
			System.Collections.Generic.IDictionary<string, byte[]> getXAttrs(org.apache.hadoop.fs.Path
				 path);

			/// <exception cref="System.IO.IOException"/>
			System.Collections.Generic.IDictionary<string, byte[]> getXAttrs(org.apache.hadoop.fs.Path
				 path, System.Collections.Generic.IList<string> names);

			/// <exception cref="System.IO.IOException"/>
			System.Collections.Generic.IList<string> listXAttrs(org.apache.hadoop.fs.Path path
				);

			/// <exception cref="System.IO.IOException"/>
			void removeXAttr(org.apache.hadoop.fs.Path path, string name);

			/// <exception cref="System.IO.IOException"/>
			org.apache.hadoop.fs.permission.AclStatus getAclStatus(org.apache.hadoop.fs.Path 
				path);

			/// <exception cref="System.IO.IOException"/>
			void access(org.apache.hadoop.fs.Path path, org.apache.hadoop.fs.permission.FsAction
				 mode);
		}

		[NUnit.Framework.Test]
		public virtual void testHarUri()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			checkInvalidPath("har://hdfs-/foo.har", conf);
			checkInvalidPath("har://hdfs/foo.har", conf);
			checkInvalidPath("har://-hdfs/foo.har", conf);
			checkInvalidPath("har://-/foo.har", conf);
			checkInvalidPath("har://127.0.0.1-/foo.har", conf);
			checkInvalidPath("har://127.0.0.1/foo.har", conf);
		}

		internal static void checkInvalidPath(string s, org.apache.hadoop.conf.Configuration
			 conf)
		{
			System.Console.Out.WriteLine("\ncheckInvalidPath: " + s);
			org.apache.hadoop.fs.Path p = new org.apache.hadoop.fs.Path(s);
			try
			{
				p.getFileSystem(conf);
				NUnit.Framework.Assert.Fail(p + " is an invalid path.");
			}
			catch (System.IO.IOException)
			{
			}
		}

		// Expected
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFileChecksum()
		{
			org.apache.hadoop.fs.Path p = new org.apache.hadoop.fs.Path("har://file-localhost/foo.har/file1"
				);
			org.apache.hadoop.fs.HarFileSystem harfs = new org.apache.hadoop.fs.HarFileSystem
				();
			try
			{
				NUnit.Framework.Assert.AreEqual(null, harfs.getFileChecksum(p));
			}
			finally
			{
				if (harfs != null)
				{
					harfs.close();
				}
			}
		}

		/// <summary>Test how block location offsets and lengths are fixed.</summary>
		[NUnit.Framework.Test]
		public virtual void testFixBlockLocations()
		{
			{
				// do some tests where start == 0
				// case 1: range starts before current har block and ends after
				org.apache.hadoop.fs.BlockLocation[] b = new org.apache.hadoop.fs.BlockLocation[]
					 { new org.apache.hadoop.fs.BlockLocation(null, null, 10, 10) };
				org.apache.hadoop.fs.HarFileSystem.fixBlockLocations(b, 0, 20, 5);
				NUnit.Framework.Assert.AreEqual(b[0].getOffset(), 5);
				NUnit.Framework.Assert.AreEqual(b[0].getLength(), 10);
			}
			{
				// case 2: range starts in current har block and ends after
				org.apache.hadoop.fs.BlockLocation[] b = new org.apache.hadoop.fs.BlockLocation[]
					 { new org.apache.hadoop.fs.BlockLocation(null, null, 10, 10) };
				org.apache.hadoop.fs.HarFileSystem.fixBlockLocations(b, 0, 20, 15);
				NUnit.Framework.Assert.AreEqual(b[0].getOffset(), 0);
				NUnit.Framework.Assert.AreEqual(b[0].getLength(), 5);
			}
			{
				// case 3: range starts before current har block and ends in
				// current har block
				org.apache.hadoop.fs.BlockLocation[] b = new org.apache.hadoop.fs.BlockLocation[]
					 { new org.apache.hadoop.fs.BlockLocation(null, null, 10, 10) };
				org.apache.hadoop.fs.HarFileSystem.fixBlockLocations(b, 0, 10, 5);
				NUnit.Framework.Assert.AreEqual(b[0].getOffset(), 5);
				NUnit.Framework.Assert.AreEqual(b[0].getLength(), 5);
			}
			{
				// case 4: range starts and ends in current har block
				org.apache.hadoop.fs.BlockLocation[] b = new org.apache.hadoop.fs.BlockLocation[]
					 { new org.apache.hadoop.fs.BlockLocation(null, null, 10, 10) };
				org.apache.hadoop.fs.HarFileSystem.fixBlockLocations(b, 0, 6, 12);
				NUnit.Framework.Assert.AreEqual(b[0].getOffset(), 0);
				NUnit.Framework.Assert.AreEqual(b[0].getLength(), 6);
			}
			{
				// now try a range where start == 3
				// case 5: range starts before current har block and ends after
				org.apache.hadoop.fs.BlockLocation[] b = new org.apache.hadoop.fs.BlockLocation[]
					 { new org.apache.hadoop.fs.BlockLocation(null, null, 10, 10) };
				org.apache.hadoop.fs.HarFileSystem.fixBlockLocations(b, 3, 20, 5);
				NUnit.Framework.Assert.AreEqual(b[0].getOffset(), 5);
				NUnit.Framework.Assert.AreEqual(b[0].getLength(), 10);
			}
			{
				// case 6: range starts in current har block and ends after
				org.apache.hadoop.fs.BlockLocation[] b = new org.apache.hadoop.fs.BlockLocation[]
					 { new org.apache.hadoop.fs.BlockLocation(null, null, 10, 10) };
				org.apache.hadoop.fs.HarFileSystem.fixBlockLocations(b, 3, 20, 15);
				NUnit.Framework.Assert.AreEqual(b[0].getOffset(), 3);
				NUnit.Framework.Assert.AreEqual(b[0].getLength(), 2);
			}
			{
				// case 7: range starts before current har block and ends in
				// current har block
				org.apache.hadoop.fs.BlockLocation[] b = new org.apache.hadoop.fs.BlockLocation[]
					 { new org.apache.hadoop.fs.BlockLocation(null, null, 10, 10) };
				org.apache.hadoop.fs.HarFileSystem.fixBlockLocations(b, 3, 7, 5);
				NUnit.Framework.Assert.AreEqual(b[0].getOffset(), 5);
				NUnit.Framework.Assert.AreEqual(b[0].getLength(), 5);
			}
			{
				// case 8: range starts and ends in current har block
				org.apache.hadoop.fs.BlockLocation[] b = new org.apache.hadoop.fs.BlockLocation[]
					 { new org.apache.hadoop.fs.BlockLocation(null, null, 10, 10) };
				org.apache.hadoop.fs.HarFileSystem.fixBlockLocations(b, 3, 3, 12);
				NUnit.Framework.Assert.AreEqual(b[0].getOffset(), 3);
				NUnit.Framework.Assert.AreEqual(b[0].getLength(), 3);
			}
			{
				// test case from JIRA MAPREDUCE-1752
				org.apache.hadoop.fs.BlockLocation[] b = new org.apache.hadoop.fs.BlockLocation[]
					 { new org.apache.hadoop.fs.BlockLocation(null, null, 512, 512), new org.apache.hadoop.fs.BlockLocation
					(null, null, 1024, 512) };
				org.apache.hadoop.fs.HarFileSystem.fixBlockLocations(b, 0, 512, 896);
				NUnit.Framework.Assert.AreEqual(b[0].getOffset(), 0);
				NUnit.Framework.Assert.AreEqual(b[0].getLength(), 128);
				NUnit.Framework.Assert.AreEqual(b[1].getOffset(), 128);
				NUnit.Framework.Assert.AreEqual(b[1].getLength(), 384);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testInheritedMethodsImplemented()
		{
			int errors = 0;
			foreach (java.lang.reflect.Method m in Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.FileSystem
				)).getDeclaredMethods())
			{
				if (java.lang.reflect.Modifier.isStatic(m.getModifiers()) || java.lang.reflect.Modifier
					.isPrivate(m.getModifiers()) || java.lang.reflect.Modifier.isFinal(m.getModifiers
					()))
				{
					continue;
				}
				try
				{
					Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.TestHarFileSystem.MustNotImplement
						)).getMethod(m.getName(), m.getParameterTypes());
					try
					{
						Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.HarFileSystem)).getDeclaredMethod
							(m.getName(), m.getParameterTypes());
						LOG.error("HarFileSystem MUST not implement " + m);
						errors++;
					}
					catch (System.MissingMethodException)
					{
					}
				}
				catch (System.MissingMethodException)
				{
					// Expected
					try
					{
						Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.HarFileSystem)).getDeclaredMethod
							(m.getName(), m.getParameterTypes());
					}
					catch (System.MissingMethodException)
					{
						LOG.error("HarFileSystem MUST implement " + m);
						errors++;
					}
				}
			}
			NUnit.Framework.Assert.IsTrue((errors + " methods were not overridden correctly - see log"
				), errors <= 0);
		}
	}
}
