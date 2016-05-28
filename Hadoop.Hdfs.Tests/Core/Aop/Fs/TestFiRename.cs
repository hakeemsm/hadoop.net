/*
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
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Rename names src to dst.</summary>
	/// <remarks>
	/// Rename names src to dst. Rename is done using following steps:
	/// <ul>
	/// <li>Checks are made to ensure src exists and appropriate flags are being
	/// passed to overwrite existing destination.
	/// <li>src is removed.
	/// <li>dst if it exists is removed.
	/// <li>src is renamed and added to directory tree as dst.
	/// </ul>
	/// During any of the above steps, the state of src and dst is reverted back to
	/// what it was prior to rename. This test ensures that the state is reverted
	/// back.
	/// This test uses AspectJ to simulate failures.
	/// </remarks>
	public class TestFiRename
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestFiRename));

		private static string removeChild = string.Empty;

		private static string addChild = string.Empty;

		private static byte[] data = new byte[] { 0 };

		private static string TestRootDir = PathUtils.GetTestDirName(typeof(TestFiRename)
			);

		private static Configuration Conf = new Configuration();

		static TestFiRename()
		{
			Conf.SetInt("io.bytes.per.checksum", 1);
		}

		private MiniDFSCluster cluster = null;

		private FileContext fc = null;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			RestartCluster(true);
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void Teardown()
		{
			if (fc != null)
			{
				fc.Delete(GetTestRootPath(), true);
			}
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void RestartCluster(bool format)
		{
			if (cluster != null)
			{
				cluster.Shutdown();
				cluster = null;
			}
			cluster = new MiniDFSCluster.Builder(Conf).Format(format).Build();
			cluster.WaitClusterUp();
			fc = FileContext.GetFileContext(cluster.GetURI(0), Conf);
		}

		/// <summary>
		/// Returns true to indicate an exception should be thrown to simulate failure
		/// during removal of a node from directory tree.
		/// </summary>
		public static bool ThrowExceptionOnRemove(string child)
		{
			bool status = removeChild.EndsWith(child);
			if (status)
			{
				removeChild = string.Empty;
			}
			return status;
		}

		/// <summary>
		/// Returns true to indicate an exception should be thrown to simulate failure
		/// during addition of a node to directory tree.
		/// </summary>
		public static bool ThrowExceptionOnAdd(string child)
		{
			bool status = addChild.EndsWith(child);
			if (status)
			{
				addChild = string.Empty;
			}
			return status;
		}

		/// <summary>Set child name on removal of which failure should be simulated</summary>
		public static void ExceptionOnRemove(string child)
		{
			removeChild = child;
			addChild = string.Empty;
		}

		/// <summary>Set child name on addition of which failure should be simulated</summary>
		public static void ExceptionOnAdd(string child)
		{
			removeChild = string.Empty;
			addChild = child;
		}

		private Path GetTestRootPath()
		{
			return fc.MakeQualified(new Path(TestRootDir));
		}

		private Path GetTestPath(string pathString)
		{
			return fc.MakeQualified(new Path(TestRootDir, pathString));
		}

		/// <exception cref="System.IO.IOException"/>
		private void CreateFile(Path path)
		{
			FSDataOutputStream @out = fc.Create(path, EnumSet.Of(CreateFlag.Create), Options.CreateOpts
				.CreateParent());
			@out.Write(data, 0, data.Length);
			@out.Close();
		}

		/// <summary>Rename test when src exists and dst does not</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailureNonExistentDst()
		{
			Path src = GetTestPath("testFailureNonExistenSrc/dir/src");
			Path dst = GetTestPath("testFailureNonExistenSrc/newdir/dst");
			CreateFile(src);
			// During rename, while removing src, an exception is thrown
			TestFiRename.ExceptionOnRemove(src.ToString());
			Rename(src, dst, true, true, false, Options.Rename.None);
			// During rename, while adding dst an exception is thrown
			TestFiRename.ExceptionOnAdd(dst.ToString());
			Rename(src, dst, true, true, false, Options.Rename.None);
		}

		/// <summary>Rename test when src and dst exist</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailuresExistingDst()
		{
			Path src = GetTestPath("testFailuresExistingDst/dir/src");
			Path dst = GetTestPath("testFailuresExistingDst/newdir/dst");
			CreateFile(src);
			CreateFile(dst);
			// During rename, while removing src, an exception is thrown
			TestFiRename.ExceptionOnRemove(src.ToString());
			Rename(src, dst, true, true, true, Options.Rename.Overwrite);
			// During rename, while removing dst, an exception is thrown
			TestFiRename.ExceptionOnRemove(dst.ToString());
			Rename(src, dst, true, true, true, Options.Rename.Overwrite);
			// During rename, while adding dst an exception is thrown
			TestFiRename.ExceptionOnAdd(dst.ToString());
			Rename(src, dst, true, true, true, Options.Rename.Overwrite);
		}

		/// <summary>Rename test where both src and dst are files</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDeletionOfDstFile()
		{
			Path src = GetTestPath("testDeletionOfDstFile/dir/src");
			Path dst = GetTestPath("testDeletionOfDstFile/newdir/dst");
			CreateFile(src);
			CreateFile(dst);
			FSNamesystem namesystem = cluster.GetNamesystem();
			long blocks = namesystem.GetBlocksTotal();
			long fileCount = namesystem.GetFilesTotal();
			Rename(src, dst, false, false, true, Options.Rename.Overwrite);
			// After successful rename the blocks corresponing dst are deleted
			NUnit.Framework.Assert.AreEqual(blocks - 1, namesystem.GetBlocksTotal());
			// After successful rename dst file is deleted
			NUnit.Framework.Assert.AreEqual(fileCount - 1, namesystem.GetFilesTotal());
			// Restart the cluster to ensure new rename operation 
			// recorded in editlog is processed right
			RestartCluster(false);
			int count = 0;
			bool exception = true;
			src = GetTestPath("testDeletionOfDstFile/dir/src");
			dst = GetTestPath("testDeletionOfDstFile/newdir/dst");
			while (exception && count < 5)
			{
				try
				{
					FileContextTestHelper.Exists(fc, src);
					exception = false;
				}
				catch (Exception e)
				{
					Log.Warn("Exception " + " count " + count + " " + e.Message);
					Sharpen.Thread.Sleep(1000);
					count++;
				}
			}
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc, src));
			NUnit.Framework.Assert.IsTrue(FileContextTestHelper.Exists(fc, dst));
		}

		/// <summary>Rename test where both src and dst are directories</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDeletionOfDstDirectory()
		{
			Path src = GetTestPath("testDeletionOfDstDirectory/dir/src");
			Path dst = GetTestPath("testDeletionOfDstDirectory/newdir/dst");
			fc.Mkdir(src, FileContext.DefaultPerm, true);
			fc.Mkdir(dst, FileContext.DefaultPerm, true);
			FSNamesystem namesystem = cluster.GetNamesystem();
			long fileCount = namesystem.GetFilesTotal();
			Rename(src, dst, false, false, true, Options.Rename.Overwrite);
			// After successful rename dst directory is deleted
			NUnit.Framework.Assert.AreEqual(fileCount - 1, namesystem.GetFilesTotal());
			// Restart the cluster to ensure new rename operation 
			// recorded in editlog is processed right
			RestartCluster(false);
			src = GetTestPath("testDeletionOfDstDirectory/dir/src");
			dst = GetTestPath("testDeletionOfDstDirectory/newdir/dst");
			int count = 0;
			bool exception = true;
			while (exception && count < 5)
			{
				try
				{
					FileContextTestHelper.Exists(fc, src);
					exception = false;
				}
				catch (Exception e)
				{
					Log.Warn("Exception " + " count " + count + " " + e.Message);
					Sharpen.Thread.Sleep(1000);
					count++;
				}
			}
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc, src));
			NUnit.Framework.Assert.IsTrue(FileContextTestHelper.Exists(fc, dst));
		}

		/// <exception cref="System.IO.IOException"/>
		private void Rename(Path src, Path dst, bool exception, bool srcExists, bool dstExists
			, params Options.Rename[] options)
		{
			try
			{
				fc.Rename(src, dst, options);
				NUnit.Framework.Assert.IsFalse("Expected exception is not thrown", exception);
			}
			catch (Exception e)
			{
				Log.Warn("Exception ", e);
				NUnit.Framework.Assert.IsTrue(exception);
			}
			NUnit.Framework.Assert.AreEqual(srcExists, FileContextTestHelper.Exists(fc, src));
			NUnit.Framework.Assert.AreEqual(dstExists, FileContextTestHelper.Exists(fc, dst));
		}
	}
}
