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
using System.IO;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Contract
{
	/// <summary>Test creating files, overwrite options &c</summary>
	public abstract class AbstractContractRenameTest : AbstractFSContractTestBase
	{
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRenameNewFileSameDir()
		{
			Describe("rename a file into a new file in the same directory");
			Path renameSrc = Path("rename_src");
			Path renameTarget = Path("rename_dest");
			byte[] data = ContractTestUtils.Dataset(256, 'a', 'z');
			ContractTestUtils.WriteDataset(GetFileSystem(), renameSrc, data, data.Length, 1024
				 * 1024, false);
			bool rename = Rename(renameSrc, renameTarget);
			Assert.True("rename(" + renameSrc + ", " + renameTarget + ") returned false"
				, rename);
			ContractTestUtils.AssertListStatusFinds(GetFileSystem(), renameTarget.GetParent()
				, renameTarget);
			ContractTestUtils.VerifyFileContents(GetFileSystem(), renameTarget, data);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRenameNonexistentFile()
		{
			Describe("rename a file into a new file in the same directory");
			Path missing = Path("testRenameNonexistentFileSrc");
			Path target = Path("testRenameNonexistentFileDest");
			bool renameReturnsFalseOnFailure = IsSupported(ContractOptions.RenameReturnsFalseIfSourceMissing
				);
			Mkdirs(missing.GetParent());
			try
			{
				bool renamed = Rename(missing, target);
				//expected an exception
				if (!renameReturnsFalseOnFailure)
				{
					string destDirLS = GenerateAndLogErrorListing(missing, target);
					NUnit.Framework.Assert.Fail("expected rename(" + missing + ", " + target + " ) to fail,"
						 + " got a result of " + renamed + " and a destination directory of " + destDirLS
						);
				}
				else
				{
					// at least one FS only returns false here, if that is the case
					// warn but continue
					GetLog().Warn("Rename returned {} renaming a nonexistent file", renamed);
					NUnit.Framework.Assert.IsFalse("Renaming a missing file returned true", renamed);
				}
			}
			catch (FileNotFoundException e)
			{
				if (renameReturnsFalseOnFailure)
				{
					ContractTestUtils.Fail("Renaming a missing file unexpectedly threw an exception", 
						e);
				}
				HandleExpectedException(e);
			}
			catch (IOException e)
			{
				HandleRelaxedException("rename nonexistent file", "FileNotFoundException", e);
			}
			AssertPathDoesNotExist("rename nonexistent file created a destination file", target
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
		[Fact]
		public virtual void TestRenameFileOverExistingFile()
		{
			Describe("Verify renaming a file onto an existing file matches expectations");
			Path srcFile = Path("source-256.txt");
			byte[] srcData = ContractTestUtils.Dataset(256, 'a', 'z');
			ContractTestUtils.WriteDataset(GetFileSystem(), srcFile, srcData, srcData.Length, 
				1024, false);
			Path destFile = Path("dest-512.txt");
			byte[] destData = ContractTestUtils.Dataset(512, 'A', 'Z');
			ContractTestUtils.WriteDataset(GetFileSystem(), destFile, destData, destData.Length
				, 1024, false);
			AssertIsFile(destFile);
			bool renameOverwritesDest = IsSupported(RenameOverwritesDest);
			bool renameReturnsFalseOnRenameDestExists = !IsSupported(RenameReturnsFalseIfDestExists
				);
			bool destUnchanged = true;
			try
			{
				bool renamed = Rename(srcFile, destFile);
				if (renameOverwritesDest)
				{
					// the filesystem supports rename(file, file2) by overwriting file2
					Assert.True("Rename returned false", renamed);
					destUnchanged = false;
				}
				else
				{
					// rename is rejected by returning 'false' or throwing an exception
					if (renamed && !renameReturnsFalseOnRenameDestExists)
					{
						//expected an exception
						string destDirLS = GenerateAndLogErrorListing(srcFile, destFile);
						GetLog().Error("dest dir {}", destDirLS);
						NUnit.Framework.Assert.Fail("expected rename(" + srcFile + ", " + destFile + " ) to fail,"
							 + " but got success and destination of " + destDirLS);
					}
				}
			}
			catch (FileAlreadyExistsException e)
			{
				HandleExpectedException(e);
			}
			// verify that the destination file is as expected based on the expected
			// outcome
			ContractTestUtils.VerifyFileContents(GetFileSystem(), destFile, destUnchanged ? destData
				 : srcData);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRenameDirIntoExistingDir()
		{
			Describe("Verify renaming a dir into an existing dir puts it underneath" + " and leaves existing files alone"
				);
			FileSystem fs = GetFileSystem();
			string sourceSubdir = "source";
			Path srcDir = Path(sourceSubdir);
			Path srcFilePath = new Path(srcDir, "source-256.txt");
			byte[] srcDataset = ContractTestUtils.Dataset(256, 'a', 'z');
			ContractTestUtils.WriteDataset(fs, srcFilePath, srcDataset, srcDataset.Length, 1024
				, false);
			Path destDir = Path("dest");
			Path destFilePath = new Path(destDir, "dest-512.txt");
			byte[] destDateset = ContractTestUtils.Dataset(512, 'A', 'Z');
			ContractTestUtils.WriteDataset(fs, destFilePath, destDateset, destDateset.Length, 
				1024, false);
			AssertIsFile(destFilePath);
			bool rename = Rename(srcDir, destDir);
			Path renamedSrc = new Path(destDir, sourceSubdir);
			AssertIsFile(destFilePath);
			AssertIsDirectory(renamedSrc);
			ContractTestUtils.VerifyFileContents(fs, destFilePath, destDateset);
			Assert.True("rename returned false though the contents were copied"
				, rename);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRenameFileNonexistentDir()
		{
			Describe("rename a file into a new file in the same directory");
			Path renameSrc = Path("testRenameSrc");
			Path renameTarget = Path("subdir/testRenameTarget");
			byte[] data = ContractTestUtils.Dataset(256, 'a', 'z');
			ContractTestUtils.WriteDataset(GetFileSystem(), renameSrc, data, data.Length, 1024
				 * 1024, false);
			bool renameCreatesDestDirs = IsSupported(RenameCreatesDestDirs);
			try
			{
				bool rename = Rename(renameSrc, renameTarget);
				if (renameCreatesDestDirs)
				{
					Assert.True(rename);
					ContractTestUtils.VerifyFileContents(GetFileSystem(), renameTarget, data);
				}
				else
				{
					NUnit.Framework.Assert.IsFalse(rename);
					ContractTestUtils.VerifyFileContents(GetFileSystem(), renameSrc, data);
				}
			}
			catch (FileNotFoundException)
			{
				// allowed unless that rename flag is set
				NUnit.Framework.Assert.IsFalse(renameCreatesDestDirs);
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRenameWithNonEmptySubDir()
		{
			Path renameTestDir = Path("testRenameWithNonEmptySubDir");
			Path srcDir = new Path(renameTestDir, "src1");
			Path srcSubDir = new Path(srcDir, "sub");
			Path finalDir = new Path(renameTestDir, "dest");
			FileSystem fs = GetFileSystem();
			bool renameRemoveEmptyDest = IsSupported(RenameRemoveDestIfEmptyDir);
			ContractTestUtils.Rm(fs, renameTestDir, true, false);
			fs.Mkdirs(srcDir);
			fs.Mkdirs(finalDir);
			ContractTestUtils.WriteTextFile(fs, new Path(srcDir, "source.txt"), "this is the file in src dir"
				, false);
			ContractTestUtils.WriteTextFile(fs, new Path(srcSubDir, "subfile.txt"), "this is the file in src/sub dir"
				, false);
			ContractTestUtils.AssertPathExists(fs, "not created in src dir", new Path(srcDir, 
				"source.txt"));
			ContractTestUtils.AssertPathExists(fs, "not created in src/sub dir", new Path(srcSubDir
				, "subfile.txt"));
			fs.Rename(srcDir, finalDir);
			// Accept both POSIX rename behavior and CLI rename behavior
			if (renameRemoveEmptyDest)
			{
				// POSIX rename behavior
				ContractTestUtils.AssertPathExists(fs, "not renamed into dest dir", new Path(finalDir
					, "source.txt"));
				ContractTestUtils.AssertPathExists(fs, "not renamed into dest/sub dir", new Path(
					finalDir, "sub/subfile.txt"));
			}
			else
			{
				// CLI rename behavior
				ContractTestUtils.AssertPathExists(fs, "not renamed into dest dir", new Path(finalDir
					, "src1/source.txt"));
				ContractTestUtils.AssertPathExists(fs, "not renamed into dest/sub dir", new Path(
					finalDir, "src1/sub/subfile.txt"));
			}
			ContractTestUtils.AssertPathDoesNotExist(fs, "not deleted", new Path(srcDir, "source.txt"
				));
		}
	}
}
