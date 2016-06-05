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
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Registry.Client.Api;
using Org.Apache.Hadoop.Registry.Client.Binding;
using Org.Apache.Hadoop.Registry.Client.Types;
using Org.Apache.Hadoop.Registry.Client.Types.Yarn;
using Org.Apache.Hadoop.Registry.Server.Integration;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Registry
{
	/// <summary>Abstract registry tests ..</summary>
	/// <remarks>
	/// Abstract registry tests .. inits the field
	/// <see cref="registry"/>
	/// before the test with an instance of
	/// <see cref="Org.Apache.Hadoop.Registry.Server.Integration.RMRegistryOperationsService
	/// 	"/>
	/// ;
	/// and
	/// <see cref="operations"/>
	/// with the same instance cast purely
	/// to the type
	/// <see cref="Org.Apache.Hadoop.Registry.Client.Api.RegistryOperations"/>
	/// .
	/// </remarks>
	public class AbstractRegistryTest : AbstractZKRegistryTest
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(AbstractRegistryTest
			));

		protected internal RMRegistryOperationsService registry;

		protected internal RegistryOperations operations;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void SetupRegistry()
		{
			registry = new RMRegistryOperationsService("yarnRegistry");
			operations = registry;
			registry.Init(CreateRegistryConfiguration());
			registry.Start();
			operations.Delete("/", true);
			registry.CreateRootRegistryPaths();
			AddToTeardown(registry);
		}

		/// <summary>
		/// Create a service entry with the sample endpoints, and put it
		/// at the destination
		/// </summary>
		/// <param name="path">path</param>
		/// <param name="createFlags">flags</param>
		/// <returns>the record</returns>
		/// <exception cref="System.IO.IOException">on a failure</exception>
		/// <exception cref="Sharpen.URISyntaxException"/>
		protected internal virtual ServiceRecord PutExampleServiceEntry(string path, int 
			createFlags)
		{
			return PutExampleServiceEntry(path, createFlags, PersistencePolicies.Permanent);
		}

		/// <summary>
		/// Create a service entry with the sample endpoints, and put it
		/// at the destination
		/// </summary>
		/// <param name="path">path</param>
		/// <param name="createFlags">flags</param>
		/// <returns>the record</returns>
		/// <exception cref="System.IO.IOException">on a failure</exception>
		/// <exception cref="Sharpen.URISyntaxException"/>
		protected internal virtual ServiceRecord PutExampleServiceEntry(string path, int 
			createFlags, string persistence)
		{
			ServiceRecord record = BuildExampleServiceEntry(persistence);
			registry.Mknode(RegistryPathUtils.ParentOf(path), true);
			operations.Bind(path, record, createFlags);
			return record;
		}

		/// <summary>Assert a path exists</summary>
		/// <param name="path">path in the registry</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void AssertPathExists(string path)
		{
			operations.Stat(path);
		}

		/// <summary>assert that a path does not exist</summary>
		/// <param name="path">path in the registry</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void AssertPathNotFound(string path)
		{
			try
			{
				operations.Stat(path);
				NUnit.Framework.Assert.Fail("Path unexpectedly found: " + path);
			}
			catch (PathNotFoundException)
			{
			}
		}

		/// <summary>Assert that a path resolves to a service record</summary>
		/// <param name="path">path in the registry</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void AssertResolves(string path)
		{
			operations.Resolve(path);
		}
	}
}
