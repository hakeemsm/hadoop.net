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
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Contract
{
	/// <summary>This class does things to the root directory.</summary>
	/// <remarks>
	/// This class does things to the root directory.
	/// Only subclass this for tests against transient filesystems where
	/// you don't care about the data.
	/// </remarks>
	public abstract class AbstractContractRootDirectoryTest : AbstractFSContractTestBase
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(AbstractContractRootDirectoryTest
			));

		/// <exception cref="System.Exception"/>
		public override void Setup()
		{
			base.Setup();
			SkipIfUnsupported(TestRootTestsEnabled);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMkDirDepth1()
		{
			FileSystem fs = GetFileSystem();
			Path dir = new Path("/testmkdirdepth1");
			AssertPathDoesNotExist("directory already exists", dir);
			fs.Mkdirs(dir);
			ContractTestUtils.AssertIsDirectory(GetFileSystem(), dir);
			AssertPathExists("directory already exists", dir);
			AssertDeleted(dir, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRmEmptyRootDirNonRecursive()
		{
			//extra sanity checks here to avoid support calls about complete loss of data
			SkipIfUnsupported(TestRootTestsEnabled);
			Path root = new Path("/");
			ContractTestUtils.AssertIsDirectory(GetFileSystem(), root);
			bool deleted = GetFileSystem().Delete(root, true);
			Log.Info("rm / of empty dir result is {}", deleted);
			ContractTestUtils.AssertIsDirectory(GetFileSystem(), root);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRmNonEmptyRootDirNonRecursive()
		{
			//extra sanity checks here to avoid support calls about complete loss of data
			SkipIfUnsupported(TestRootTestsEnabled);
			Path root = new Path("/");
			string touchfile = "/testRmNonEmptyRootDirNonRecursive";
			Path file = new Path(touchfile);
			ContractTestUtils.Touch(GetFileSystem(), file);
			ContractTestUtils.AssertIsDirectory(GetFileSystem(), root);
			try
			{
				bool deleted = GetFileSystem().Delete(root, false);
				NUnit.Framework.Assert.Fail("non recursive delete should have raised an exception,"
					 + " but completed with exit code " + deleted);
			}
			catch (IOException e)
			{
				//expected
				HandleExpectedException(e);
			}
			finally
			{
				GetFileSystem().Delete(file, false);
			}
			ContractTestUtils.AssertIsDirectory(GetFileSystem(), root);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRmRootRecursive()
		{
			//extra sanity checks here to avoid support calls about complete loss of data
			SkipIfUnsupported(TestRootTestsEnabled);
			Path root = new Path("/");
			ContractTestUtils.AssertIsDirectory(GetFileSystem(), root);
			Path file = new Path("/testRmRootRecursive");
			ContractTestUtils.Touch(GetFileSystem(), file);
			bool deleted = GetFileSystem().Delete(root, true);
			ContractTestUtils.AssertIsDirectory(GetFileSystem(), root);
			Log.Info("rm -rf / result is {}", deleted);
			if (deleted)
			{
				AssertPathDoesNotExist("expected file to be deleted", file);
			}
			else
			{
				AssertPathExists("expected file to be preserved", file);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateFileOverRoot()
		{
			Path root = new Path("/");
			byte[] dataset = ContractTestUtils.Dataset(1024, ' ', 'z');
			try
			{
				ContractTestUtils.CreateFile(GetFileSystem(), root, false, dataset);
				NUnit.Framework.Assert.Fail("expected an exception, got a file created over root: "
					 + Ls(root));
			}
			catch (IOException e)
			{
				//expected
				HandleExpectedException(e);
			}
			AssertIsDirectory(root);
		}
	}
}
