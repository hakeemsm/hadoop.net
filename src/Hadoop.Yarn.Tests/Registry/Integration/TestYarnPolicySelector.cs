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
using Org.Apache.Hadoop.Registry;
using Org.Apache.Hadoop.Registry.Client.Types;
using Org.Apache.Hadoop.Registry.Client.Types.Yarn;
using Org.Apache.Hadoop.Registry.Server.Integration;
using Org.Apache.Hadoop.Registry.Server.Services;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Integration
{
	public class TestYarnPolicySelector : RegistryTestHelper
	{
		private ServiceRecord record = CreateRecord("1", PersistencePolicies.Application, 
			"one", null);

		private RegistryPathStatus status = new RegistryPathStatus("/", 0, 0, 1);

		public virtual void AssertSelected(bool outcome, RegistryAdminService.NodeSelector
			 selector)
		{
			bool select = selector.ShouldSelect("/", status, record);
			NUnit.Framework.Assert.AreEqual(selector.ToString(), outcome, select);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestByContainer()
		{
			AssertSelected(false, new SelectByYarnPersistence("1", PersistencePolicies.Container
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestByApp()
		{
			AssertSelected(true, new SelectByYarnPersistence("1", PersistencePolicies.Application
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestByAppName()
		{
			AssertSelected(false, new SelectByYarnPersistence("2", PersistencePolicies.Application
				));
		}
	}
}
