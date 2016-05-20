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

namespace org.apache.hadoop.fs.contract.localfs
{
	/// <summary>The contract of the Local filesystem.</summary>
	/// <remarks>
	/// The contract of the Local filesystem.
	/// This changes its feature set from platform for platform -the default
	/// set is updated during initialization.
	/// This contract contains some override points, to permit
	/// the raw local filesystem and other filesystems to subclass it.
	/// </remarks>
	public class LocalFSContract : org.apache.hadoop.fs.contract.AbstractFSContract
	{
		public const string CONTRACT_XML = "contract/localfs.xml";

		public const string SYSPROP_TEST_BUILD_DATA = "test.build.data";

		public const string DEFAULT_TEST_BUILD_DATA_DIR = "test/build/data";

		private org.apache.hadoop.fs.FileSystem fs;

		public LocalFSContract(org.apache.hadoop.conf.Configuration conf)
			: base(conf)
		{
			//insert the base features
			addConfResource(getContractXml());
		}

		/// <summary>Return the contract file for this filesystem</summary>
		/// <returns>the XML</returns>
		protected internal virtual string getContractXml()
		{
			return CONTRACT_XML;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void init()
		{
			base.init();
			fs = getLocalFS();
			adjustContractToLocalEnvironment();
		}

		/// <summary>
		/// tweak some of the contract parameters based on the local system
		/// state
		/// </summary>
		protected internal virtual void adjustContractToLocalEnvironment()
		{
			if (org.apache.hadoop.util.Shell.WINDOWS)
			{
				//NTFS doesn't do case sensitivity, and its permissions are ACL-based
				getConf().setBoolean(getConfKey(org.apache.hadoop.fs.contract.ContractOptions.IS_CASE_SENSITIVE
					), false);
				getConf().setBoolean(getConfKey(org.apache.hadoop.fs.contract.ContractOptions.SUPPORTS_UNIX_PERMISSIONS
					), false);
			}
			else
			{
				if (org.apache.hadoop.fs.contract.ContractTestUtils.isOSX())
				{
					//OSX HFS+ is not case sensitive
					getConf().setBoolean(getConfKey(org.apache.hadoop.fs.contract.ContractOptions.IS_CASE_SENSITIVE
						), false);
				}
			}
		}

		/// <summary>Get the local filesystem.</summary>
		/// <remarks>Get the local filesystem. This may be overridden</remarks>
		/// <returns>the filesystem</returns>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual org.apache.hadoop.fs.FileSystem getLocalFS()
		{
			return org.apache.hadoop.fs.FileSystem.getLocal(getConf());
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileSystem getTestFileSystem()
		{
			return fs;
		}

		public override string getScheme()
		{
			return "file";
		}

		public override org.apache.hadoop.fs.Path getTestPath()
		{
			org.apache.hadoop.fs.Path path = fs.makeQualified(new org.apache.hadoop.fs.Path(getTestDataDir
				()));
			return path;
		}

		/// <summary>Get the test data directory</summary>
		/// <returns>the directory for test data</returns>
		protected internal virtual string getTestDataDir()
		{
			return Sharpen.Runtime.getProperty(SYSPROP_TEST_BUILD_DATA, DEFAULT_TEST_BUILD_DATA_DIR
				);
		}
	}
}
