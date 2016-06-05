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
using Sharpen;

namespace Org.Apache.Hadoop.FS.Contract
{
	/// <summary>Test concat -if supported</summary>
	public abstract class AbstractContractConcatTest : AbstractFSContractTestBase
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(AbstractContractConcatTest
			));

		private Path testPath;

		private Path srcFile;

		private Path zeroByteFile;

		private Path target;

		/// <exception cref="System.Exception"/>
		public override void Setup()
		{
			base.Setup();
			SkipIfUnsupported(SupportsConcat);
			//delete the test directory
			testPath = Path("test");
			srcFile = new Path(testPath, "small.txt");
			zeroByteFile = new Path(testPath, "zero.txt");
			target = new Path(testPath, "target");
			byte[] block = ContractTestUtils.Dataset(TestFileLen, 0, 255);
			ContractTestUtils.CreateFile(GetFileSystem(), srcFile, false, block);
			ContractTestUtils.Touch(GetFileSystem(), zeroByteFile);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestConcatEmptyFiles()
		{
			ContractTestUtils.Touch(GetFileSystem(), target);
			try
			{
				GetFileSystem().Concat(target, new Path[0]);
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
		public virtual void TestConcatMissingTarget()
		{
			try
			{
				GetFileSystem().Concat(target, new Path[] { zeroByteFile });
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
		public virtual void TestConcatFileOnFile()
		{
			byte[] block = ContractTestUtils.Dataset(TestFileLen, 0, 255);
			ContractTestUtils.CreateFile(GetFileSystem(), target, false, block);
			GetFileSystem().Concat(target, new Path[] { srcFile });
			ContractTestUtils.AssertFileHasLength(GetFileSystem(), target, TestFileLen * 2);
			ContractTestUtils.ValidateFileContent(ContractTestUtils.ReadDataset(GetFileSystem
				(), target, TestFileLen * 2), new byte[][] { block, block });
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestConcatOnSelf()
		{
			byte[] block = ContractTestUtils.Dataset(TestFileLen, 0, 255);
			ContractTestUtils.CreateFile(GetFileSystem(), target, false, block);
			try
			{
				GetFileSystem().Concat(target, new Path[] { target });
			}
			catch (Exception e)
			{
				//expected
				HandleExpectedException(e);
			}
		}
	}
}
