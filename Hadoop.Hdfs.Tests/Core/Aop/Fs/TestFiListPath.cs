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
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>A large directory listing may have to go through multiple RPCs.</summary>
	/// <remarks>
	/// A large directory listing may have to go through multiple RPCs.
	/// The directory to be listed may be removed before all contents are listed.
	/// This test uses AspectJ to simulate the scenario.
	/// </remarks>
	public class TestFiListPath
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestFiListPath));

		private const int ListLimit = 1;

		private static MiniDFSCluster cluster = null;

		private static FileSystem fs;

		private static Path TestPath = new Path("/tmp");

		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void Setup()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsListLimit, ListLimit);
			cluster = new MiniDFSCluster.Builder(conf).Build();
			cluster.WaitClusterUp();
			fs = cluster.GetFileSystem();
		}

		/// <exception cref="System.IO.IOException"/>
		[AfterClass]
		public static void Teardown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Prepare()
		{
			fs.Mkdirs(TestPath);
			for (int i = 0; i < ListLimit + 1; i++)
			{
				fs.Mkdirs(new Path(TestPath, "dir" + i));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void Cleanup()
		{
			fs.Delete(TestPath, true);
		}

		/// <summary>Remove the target directory after the getListing RPC</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTargetDeletionForListStatus()
		{
			Log.Info("Test Target Delete For listStatus");
			try
			{
				fs.ListStatus(TestPath);
				NUnit.Framework.Assert.Fail("Test should fail with FileNotFoundException");
			}
			catch (FileNotFoundException e)
			{
				NUnit.Framework.Assert.AreEqual("File " + TestPath + " does not exist.", e.Message
					);
				Log.Info(StringUtils.StringifyException(e));
			}
		}

		/// <summary>Remove the target directory after the getListing RPC</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTargetDeletionForListLocatedStatus()
		{
			Log.Info("Test Target Delete For listLocatedStatus");
			RemoteIterator<LocatedFileStatus> itor = fs.ListLocatedStatus(TestPath);
			itor.Next();
			NUnit.Framework.Assert.IsFalse(itor.HasNext());
		}
	}
}
