/*
* Licensed to the Apache Software Foundation (ASF) under one
*  or more contributor license agreements.  See the NOTICE file
*  distributed with this work for additional information
*  regarding copyright ownership.  The ASF licenses this file
*  to you under the Apache License, Version 2.0 (the
*  "License"); you may not use this file except in compliance
*  with the License.  You may obtain a copy of the License at
*
*       http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software
*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
*/
using Sharpen;

namespace org.apache.hadoop.fs.contract
{
	/// <summary>Test creating files, overwrite options &c</summary>
	public abstract class AbstractContractRenameTest : org.apache.hadoop.fs.contract.AbstractFSContractTestBase
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRenameNewFileSameDir()
		{
			describe("rename a file into a new file in the same directory");
			org.apache.hadoop.fs.Path renameSrc = path("rename_src");
			org.apache.hadoop.fs.Path renameTarget = path("rename_dest");
			byte[] data = org.apache.hadoop.fs.contract.ContractTestUtils.dataset(256, 'a', 'z'
				);
			org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset(getFileSystem(), renameSrc
				, data, data.Length, 1024 * 1024, false);
			bool rename = rename(renameSrc, renameTarget);
			NUnit.Framework.Assert.IsTrue("rename(" + renameSrc + ", " + renameTarget + ") returned false"
				, rename);
			org.apache.hadoop.fs.contract.ContractTestUtils.assertListStatusFinds(getFileSystem
				(), renameTarget.getParent(), renameTarget);
			org.apache.hadoop.fs.contract.ContractTestUtils.verifyFileContents(getFileSystem(
				), renameTarget, data);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRenameNonexistentFile()
		{
			describe("rename a file into a new file in the same directory");
			org.apache.hadoop.fs.Path missing = path("testRenameNonexistentFileSrc");
			org.apache.hadoop.fs.Path target = path("testRenameNonexistentFileDest");
			bool renameReturnsFalseOnFailure = isSupported(org.apache.hadoop.fs.contract.ContractOptions
				.RENAME_RETURNS_FALSE_IF_SOURCE_MISSING);
			mkdirs(missing.getParent());
			try
			{
				bool renamed = rename(missing, target);
				//expected an exception
				if (!renameReturnsFalseOnFailure)
				{
					string destDirLS = generateAndLogErrorListing(missing, target);
					NUnit.Framework.Assert.Fail("expected rename(" + missing + ", " + target + " ) to fail,"
						 + " got a result of " + renamed + " and a destination directory of " + destDirLS
						);
				}
				else
				{
					// at least one FS only returns false here, if that is the case
					// warn but continue
					getLog().warn("Rename returned {} renaming a nonexistent file", renamed);
					NUnit.Framework.Assert.IsFalse("Renaming a missing file returned true", renamed);
				}
			}
			catch (java.io.FileNotFoundException e)
			{
				if (renameReturnsFalseOnFailure)
				{
					org.apache.hadoop.fs.contract.ContractTestUtils.fail("Renaming a missing file unexpectedly threw an exception"
						, e);
				}
				handleExpectedException(e);
			}
			catch (System.IO.IOException e)
			{
				handleRelaxedException("rename nonexistent file", "FileNotFoundException", e);
			}
			assertPathDoesNotExist("rename nonexistent file created a destination file", target
				);
		}

		/// <summary>
		/// Rename test -handles filesystems that will overwrite the destination
		/// as well as those that do not (i.e.
		/// </summary>
		/// <remarks>
		/// Rename test -handles filesystems that will overwrite the destination
		/// as well as those that do not (i.e. HDFS).
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRenameFileOverExistingFile()
		{
			describe("Verify renaming a file onto an existing file matches expectations");
			org.apache.hadoop.fs.Path srcFile = path("source-256.txt");
			byte[] srcData = org.apache.hadoop.fs.contract.ContractTestUtils.dataset(256, 'a'
				, 'z');
			org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset(getFileSystem(), srcFile
				, srcData, srcData.Length, 1024, false);
			org.apache.hadoop.fs.Path destFile = path("dest-512.txt");
			byte[] destData = org.apache.hadoop.fs.contract.ContractTestUtils.dataset(512, 'A'
				, 'Z');
			org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset(getFileSystem(), destFile
				, destData, destData.Length, 1024, false);
			assertIsFile(destFile);
			bool renameOverwritesDest = isSupported(RENAME_OVERWRITES_DEST);
			bool renameReturnsFalseOnRenameDestExists = !isSupported(RENAME_RETURNS_FALSE_IF_DEST_EXISTS
				);
			bool destUnchanged = true;
			try
			{
				bool renamed = rename(srcFile, destFile);
				if (renameOverwritesDest)
				{
					// the filesystem supports rename(file, file2) by overwriting file2
					NUnit.Framework.Assert.IsTrue("Rename returned false", renamed);
					destUnchanged = false;
				}
				else
				{
					// rename is rejected by returning 'false' or throwing an exception
					if (renamed && !renameReturnsFalseOnRenameDestExists)
					{
						//expected an exception
						string destDirLS = generateAndLogErrorListing(srcFile, destFile);
						getLog().error("dest dir {}", destDirLS);
						NUnit.Framework.Assert.Fail("expected rename(" + srcFile + ", " + destFile + " ) to fail,"
							 + " but got success and destination of " + destDirLS);
					}
				}
			}
			catch (org.apache.hadoop.fs.FileAlreadyExistsException e)
			{
				handleExpectedException(e);
			}
			// verify that the destination file is as expected based on the expected
			// outcome
			org.apache.hadoop.fs.contract.ContractTestUtils.verifyFileContents(getFileSystem(
				), destFile, destUnchanged ? destData : srcData);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRenameDirIntoExistingDir()
		{
			describe("Verify renaming a dir into an existing dir puts it underneath" + " and leaves existing files alone"
				);
			org.apache.hadoop.fs.FileSystem fs = getFileSystem();
			string sourceSubdir = "source";
			org.apache.hadoop.fs.Path srcDir = path(sourceSubdir);
			org.apache.hadoop.fs.Path srcFilePath = new org.apache.hadoop.fs.Path(srcDir, "source-256.txt"
				);
			byte[] srcDataset = org.apache.hadoop.fs.contract.ContractTestUtils.dataset(256, 
				'a', 'z');
			org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset(fs, srcFilePath, srcDataset
				, srcDataset.Length, 1024, false);
			org.apache.hadoop.fs.Path destDir = path("dest");
			org.apache.hadoop.fs.Path destFilePath = new org.apache.hadoop.fs.Path(destDir, "dest-512.txt"
				);
			byte[] destDateset = org.apache.hadoop.fs.contract.ContractTestUtils.dataset(512, 
				'A', 'Z');
			org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset(fs, destFilePath, destDateset
				, destDateset.Length, 1024, false);
			assertIsFile(destFilePath);
			bool rename = rename(srcDir, destDir);
			org.apache.hadoop.fs.Path renamedSrc = new org.apache.hadoop.fs.Path(destDir, sourceSubdir
				);
			assertIsFile(destFilePath);
			assertIsDirectory(renamedSrc);
			org.apache.hadoop.fs.contract.ContractTestUtils.verifyFileContents(fs, destFilePath
				, destDateset);
			NUnit.Framework.Assert.IsTrue("rename returned false though the contents were copied"
				, rename);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRenameFileNonexistentDir()
		{
			describe("rename a file into a new file in the same directory");
			org.apache.hadoop.fs.Path renameSrc = path("testRenameSrc");
			org.apache.hadoop.fs.Path renameTarget = path("subdir/testRenameTarget");
			byte[] data = org.apache.hadoop.fs.contract.ContractTestUtils.dataset(256, 'a', 'z'
				);
			org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset(getFileSystem(), renameSrc
				, data, data.Length, 1024 * 1024, false);
			bool renameCreatesDestDirs = isSupported(RENAME_CREATES_DEST_DIRS);
			try
			{
				bool rename = rename(renameSrc, renameTarget);
				if (renameCreatesDestDirs)
				{
					NUnit.Framework.Assert.IsTrue(rename);
					org.apache.hadoop.fs.contract.ContractTestUtils.verifyFileContents(getFileSystem(
						), renameTarget, data);
				}
				else
				{
					NUnit.Framework.Assert.IsFalse(rename);
					org.apache.hadoop.fs.contract.ContractTestUtils.verifyFileContents(getFileSystem(
						), renameSrc, data);
				}
			}
			catch (java.io.FileNotFoundException)
			{
				// allowed unless that rename flag is set
				NUnit.Framework.Assert.IsFalse(renameCreatesDestDirs);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRenameWithNonEmptySubDir()
		{
			org.apache.hadoop.fs.Path renameTestDir = path("testRenameWithNonEmptySubDir");
			org.apache.hadoop.fs.Path srcDir = new org.apache.hadoop.fs.Path(renameTestDir, "src1"
				);
			org.apache.hadoop.fs.Path srcSubDir = new org.apache.hadoop.fs.Path(srcDir, "sub"
				);
			org.apache.hadoop.fs.Path finalDir = new org.apache.hadoop.fs.Path(renameTestDir, 
				"dest");
			org.apache.hadoop.fs.FileSystem fs = getFileSystem();
			bool renameRemoveEmptyDest = isSupported(RENAME_REMOVE_DEST_IF_EMPTY_DIR);
			org.apache.hadoop.fs.contract.ContractTestUtils.rm(fs, renameTestDir, true, false
				);
			fs.mkdirs(srcDir);
			fs.mkdirs(finalDir);
			org.apache.hadoop.fs.contract.ContractTestUtils.writeTextFile(fs, new org.apache.hadoop.fs.Path
				(srcDir, "source.txt"), "this is the file in src dir", false);
			org.apache.hadoop.fs.contract.ContractTestUtils.writeTextFile(fs, new org.apache.hadoop.fs.Path
				(srcSubDir, "subfile.txt"), "this is the file in src/sub dir", false);
			org.apache.hadoop.fs.contract.ContractTestUtils.assertPathExists(fs, "not created in src dir"
				, new org.apache.hadoop.fs.Path(srcDir, "source.txt"));
			org.apache.hadoop.fs.contract.ContractTestUtils.assertPathExists(fs, "not created in src/sub dir"
				, new org.apache.hadoop.fs.Path(srcSubDir, "subfile.txt"));
			fs.rename(srcDir, finalDir);
			// Accept both POSIX rename behavior and CLI rename behavior
			if (renameRemoveEmptyDest)
			{
				// POSIX rename behavior
				org.apache.hadoop.fs.contract.ContractTestUtils.assertPathExists(fs, "not renamed into dest dir"
					, new org.apache.hadoop.fs.Path(finalDir, "source.txt"));
				org.apache.hadoop.fs.contract.ContractTestUtils.assertPathExists(fs, "not renamed into dest/sub dir"
					, new org.apache.hadoop.fs.Path(finalDir, "sub/subfile.txt"));
			}
			else
			{
				// CLI rename behavior
				org.apache.hadoop.fs.contract.ContractTestUtils.assertPathExists(fs, "not renamed into dest dir"
					, new org.apache.hadoop.fs.Path(finalDir, "src1/source.txt"));
				org.apache.hadoop.fs.contract.ContractTestUtils.assertPathExists(fs, "not renamed into dest/sub dir"
					, new org.apache.hadoop.fs.Path(finalDir, "src1/sub/subfile.txt"));
			}
			org.apache.hadoop.fs.contract.ContractTestUtils.assertPathDoesNotExist(fs, "not deleted"
				, new org.apache.hadoop.fs.Path(srcDir, "source.txt"));
		}
	}
}
