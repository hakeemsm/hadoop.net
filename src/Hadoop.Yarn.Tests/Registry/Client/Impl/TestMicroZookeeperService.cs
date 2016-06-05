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
using NUnit.Framework;
using NUnit.Framework.Rules;
using Org.Apache.Hadoop.Registry.Server.Services;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Client.Impl
{
	/// <summary>Simple tests to look at the micro ZK service itself</summary>
	public class TestMicroZookeeperService : Assert
	{
		private MicroZookeeperService zookeeper;

		[Rule]
		public readonly Timeout testTimeout = new Timeout(10000);

		[Rule]
		public TestName methodName = new TestName();

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void DestroyZKServer()
		{
			ServiceOperations.Stop(zookeeper);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTempDirSupport()
		{
			YarnConfiguration conf = new YarnConfiguration();
			zookeeper = new MicroZookeeperService("t1");
			zookeeper.Init(conf);
			zookeeper.Start();
			zookeeper.Stop();
		}
	}
}
