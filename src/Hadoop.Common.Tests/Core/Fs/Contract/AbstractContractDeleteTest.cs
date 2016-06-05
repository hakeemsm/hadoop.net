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


namespace Org.Apache.Hadoop.FS.Contract
{
	/// <summary>Test creating files, overwrite options &c</summary>
	public abstract class AbstractContractDeleteTest : AbstractFSContractTestBase
	{
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestDeleteEmptyDirNonRecursive()
		{
			Path path = Path("testDeleteEmptyDirNonRecursive");
			Mkdirs(path);
			AssertDeleted(path, false);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestDeleteEmptyDirRecursive()
		{
			Path path = Path("testDeleteEmptyDirRecursive");
			Mkdirs(path);
			AssertDeleted(path, true);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestDeleteNonexistentPathRecursive()
		{
			Path path = Path("testDeleteNonexistentPathRecursive");
			ContractTestUtils.AssertPathDoesNotExist(GetFileSystem(), "leftover", path);
			ContractTestUtils.RejectRootOperation(path);
			NUnit.Framework.Assert.IsFalse("Returned true attempting to delete" + " a nonexistent path "
				 + path, GetFileSystem().Delete(path, false));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestDeleteNonexistentPathNonRecursive()
		{
			Path path = Path("testDeleteNonexistentPathNonRecursive");
			ContractTestUtils.AssertPathDoesNotExist(GetFileSystem(), "leftover", path);
			ContractTestUtils.RejectRootOperation(path);
			NUnit.Framework.Assert.IsFalse("Returned true attempting to recursively delete" +
				 " a nonexistent path " + path, GetFileSystem().Delete(path, false));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestDeleteNonEmptyDirNonRecursive()
		{
			Path path = Path("testDeleteNonEmptyDirNonRecursive");
			Mkdirs(path);
			Path file = new Path(path, "childfile");
			ContractTestUtils.WriteTextFile(GetFileSystem(), file, "goodbye, world", true);
			try
			{
				ContractTestUtils.RejectRootOperation(path);
				bool deleted = GetFileSystem().Delete(path, false);
				NUnit.Framework.Assert.Fail("non recursive delete should have raised an exception,"
					 + " but completed with exit code " + deleted);
			}
			catch (IOException expected)
			{
				//expected
				HandleExpectedException(expected);
			}
			ContractTestUtils.AssertIsDirectory(GetFileSystem(), path);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestDeleteNonEmptyDirRecursive()
		{
			Path path = Path("testDeleteNonEmptyDirNonRecursive");
			Mkdirs(path);
			Path file = new Path(path, "childfile");
			ContractTestUtils.WriteTextFile(GetFileSystem(), file, "goodbye, world", true);
			AssertDeleted(path, true);
			ContractTestUtils.AssertPathDoesNotExist(GetFileSystem(), "not deleted", file);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestDeleteDeepEmptyDir()
		{
			Mkdirs(Path("testDeleteDeepEmptyDir/d1/d2/d3/d4"));
			AssertDeleted(Path("testDeleteDeepEmptyDir/d1/d2/d3"), true);
			FileSystem fs = GetFileSystem();
			ContractTestUtils.AssertPathDoesNotExist(fs, "not deleted", Path("testDeleteDeepEmptyDir/d1/d2/d3/d4"
				));
			ContractTestUtils.AssertPathDoesNotExist(fs, "not deleted", Path("testDeleteDeepEmptyDir/d1/d2/d3"
				));
			ContractTestUtils.AssertPathExists(fs, "parent dir is deleted", Path("testDeleteDeepEmptyDir/d1/d2"
				));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestDeleteSingleFile()
		{
			// Test delete of just a file
			Path path = Path("testDeleteSingleFile/d1/d2");
			Mkdirs(path);
			Path file = new Path(path, "childfile");
			ContractTestUtils.WriteTextFile(GetFileSystem(), file, "single file to be deleted."
				, true);
			ContractTestUtils.AssertPathExists(GetFileSystem(), "single file not created", file
				);
			AssertDeleted(file, false);
		}
	}
}
