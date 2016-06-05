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
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Contract;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.FS.Contract.Localfs
{
	/// <summary>The contract of the Local filesystem.</summary>
	/// <remarks>
	/// The contract of the Local filesystem.
	/// This changes its feature set from platform for platform -the default
	/// set is updated during initialization.
	/// This contract contains some override points, to permit
	/// the raw local filesystem and other filesystems to subclass it.
	/// </remarks>
	public class LocalFSContract : AbstractFSContract
	{
		public const string ContractXml = "contract/localfs.xml";

		public const string SyspropTestBuildData = "test.build.data";

		public const string DefaultTestBuildDataDir = "test/build/data";

		private FileSystem fs;

		public LocalFSContract(Configuration conf)
			: base(conf)
		{
			//insert the base features
			AddConfResource(GetContractXml());
		}

		/// <summary>Return the contract file for this filesystem</summary>
		/// <returns>the XML</returns>
		protected internal virtual string GetContractXml()
		{
			return ContractXml;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Init()
		{
			base.Init();
			fs = GetLocalFS();
			AdjustContractToLocalEnvironment();
		}

		/// <summary>
		/// tweak some of the contract parameters based on the local system
		/// state
		/// </summary>
		protected internal virtual void AdjustContractToLocalEnvironment()
		{
			if (Shell.Windows)
			{
				//NTFS doesn't do case sensitivity, and its permissions are ACL-based
				GetConf().SetBoolean(GetConfKey(ContractOptions.IsCaseSensitive), false);
				GetConf().SetBoolean(GetConfKey(ContractOptions.SupportsUnixPermissions), false);
			}
			else
			{
				if (ContractTestUtils.IsOSX())
				{
					//OSX HFS+ is not case sensitive
					GetConf().SetBoolean(GetConfKey(ContractOptions.IsCaseSensitive), false);
				}
			}
		}

		/// <summary>Get the local filesystem.</summary>
		/// <remarks>Get the local filesystem. This may be overridden</remarks>
		/// <returns>the filesystem</returns>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual FileSystem GetLocalFS()
		{
			return FileSystem.GetLocal(GetConf());
		}

		/// <exception cref="System.IO.IOException"/>
		public override FileSystem GetTestFileSystem()
		{
			return fs;
		}

		public override string GetScheme()
		{
			return "file";
		}

		public override Path GetTestPath()
		{
			Path path = fs.MakeQualified(new Path(GetTestDataDir()));
			return path;
		}

		/// <summary>Get the test data directory</summary>
		/// <returns>the directory for test data</returns>
		protected internal virtual string GetTestDataDir()
		{
			return Runtime.GetProperty(SyspropTestBuildData, DefaultTestBuildDataDir);
		}
	}
}
