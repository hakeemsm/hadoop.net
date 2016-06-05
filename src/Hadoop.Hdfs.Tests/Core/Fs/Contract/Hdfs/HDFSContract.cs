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
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Contract;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Contract.Hdfs
{
	/// <summary>
	/// The contract of HDFS
	/// This changes its feature set from platform for platform -the default
	/// set is updated during initialization.
	/// </summary>
	public class HDFSContract : AbstractFSContract
	{
		public const string ContractHdfsXml = "contract/hdfs.xml";

		public const int BlockSize = AbstractFSContractTestBase.TestFileLen;

		private static MiniDFSCluster cluster;

		public HDFSContract(Configuration conf)
			: base(conf)
		{
			//insert the base features
			AddConfResource(ContractHdfsXml);
		}

		/// <exception cref="System.IO.IOException"/>
		public static void CreateCluster()
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			conf.AddResource(ContractHdfsXml);
			//hack in a 256 byte block size
			conf.SetInt(DFSConfigKeys.DfsBlockSizeKey, BlockSize);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			cluster.WaitClusterUp();
		}

		/// <exception cref="System.IO.IOException"/>
		public static void DestroyCluster()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		public static MiniDFSCluster GetCluster()
		{
			return cluster;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Init()
		{
			base.Init();
			NUnit.Framework.Assert.IsTrue("contract options not loaded", IsSupported(ContractOptions
				.IsCaseSensitive, false));
		}

		/// <exception cref="System.IO.IOException"/>
		public override FileSystem GetTestFileSystem()
		{
			//assumes cluster is not null
			NUnit.Framework.Assert.IsNotNull("cluster not created", cluster);
			return cluster.GetFileSystem();
		}

		public override string GetScheme()
		{
			return "hdfs";
		}

		public override Path GetTestPath()
		{
			Path path = new Path("/test");
			return path;
		}
	}
}
