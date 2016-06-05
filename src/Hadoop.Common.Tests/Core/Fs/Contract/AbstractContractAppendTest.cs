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
using System;
using Org.Apache.Hadoop.FS;
using Org.Slf4j;


namespace Org.Apache.Hadoop.FS.Contract
{
	/// <summary>Test concat -if supported</summary>
	public abstract class AbstractContractAppendTest : AbstractFSContractTestBase
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(AbstractContractAppendTest
			));

		private Path testPath;

		private Path target;

		/// <exception cref="System.Exception"/>
		public override void Setup()
		{
			base.Setup();
			SkipIfUnsupported(SupportsAppend);
			//delete the test directory
			testPath = Path("test");
			target = new Path(testPath, "target");
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestAppendToEmptyFile()
		{
			ContractTestUtils.Touch(GetFileSystem(), target);
			byte[] dataset = ContractTestUtils.Dataset(256, 'a', 'z');
			FSDataOutputStream outputStream = GetFileSystem().Append(target);
			try
			{
				outputStream.Write(dataset);
			}
			finally
			{
				outputStream.Close();
			}
			byte[] bytes = ContractTestUtils.ReadDataset(GetFileSystem(), target, dataset.Length
				);
			ContractTestUtils.CompareByteArrays(dataset, bytes, dataset.Length);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestAppendNonexistentFile()
		{
			try
			{
				FSDataOutputStream @out = GetFileSystem().Append(target);
				//got here: trouble
				@out.Close();
				NUnit.Framework.Assert.Fail("expected a failure");
			}
			catch (Exception e)
			{
				//expected
				HandleExpectedException(e);
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestAppendToExistingFile()
		{
			byte[] original = ContractTestUtils.Dataset(8192, 'A', 'Z');
			byte[] appended = ContractTestUtils.Dataset(8192, '0', '9');
			ContractTestUtils.CreateFile(GetFileSystem(), target, false, original);
			FSDataOutputStream outputStream = GetFileSystem().Append(target);
			outputStream.Write(appended);
			outputStream.Close();
			byte[] bytes = ContractTestUtils.ReadDataset(GetFileSystem(), target, original.Length
				 + appended.Length);
			ContractTestUtils.ValidateFileContent(bytes, new byte[][] { original, appended });
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestAppendMissingTarget()
		{
			try
			{
				FSDataOutputStream @out = GetFileSystem().Append(target);
				//got here: trouble
				@out.Close();
				NUnit.Framework.Assert.Fail("expected a failure");
			}
			catch (Exception e)
			{
				//expected
				HandleExpectedException(e);
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRenameFileBeingAppended()
		{
			ContractTestUtils.Touch(GetFileSystem(), target);
			AssertPathExists("original file does not exist", target);
			byte[] dataset = ContractTestUtils.Dataset(256, 'a', 'z');
			FSDataOutputStream outputStream = GetFileSystem().Append(target);
			outputStream.Write(dataset);
			Path renamed = new Path(testPath, "renamed");
			outputStream.Close();
			string listing = Ls(testPath);
			//expected: the stream goes to the file that was being renamed, not
			//the original path
			AssertPathExists("renamed destination file does not exist", renamed);
			AssertPathDoesNotExist("Source file found after rename during append:\n" + listing
				, target);
			byte[] bytes = ContractTestUtils.ReadDataset(GetFileSystem(), renamed, dataset.Length
				);
			ContractTestUtils.CompareByteArrays(dataset, bytes, dataset.Length);
		}
	}
}
