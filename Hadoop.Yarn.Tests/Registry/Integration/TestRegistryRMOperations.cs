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
using System.Collections.Generic;
using System.IO;
using Org.Apache.Curator.Framework.Api;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Registry;
using Org.Apache.Hadoop.Registry.Client.Api;
using Org.Apache.Hadoop.Registry.Client.Binding;
using Org.Apache.Hadoop.Registry.Client.Impl;
using Org.Apache.Hadoop.Registry.Client.Impl.ZK;
using Org.Apache.Hadoop.Registry.Client.Types;
using Org.Apache.Hadoop.Registry.Client.Types.Yarn;
using Org.Apache.Hadoop.Registry.Server.Services;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Integration
{
	public class TestRegistryRMOperations : AbstractRegistryTest
	{
		protected internal static readonly Logger Log = LoggerFactory.GetLogger(typeof(TestRegistryRMOperations
			));

		/// <summary>trigger a purge operation</summary>
		/// <param name="path">path</param>
		/// <param name="id">yarn ID</param>
		/// <param name="policyMatch">policy to match ID on</param>
		/// <param name="purgePolicy">policy when there are children under a match</param>
		/// <returns>the number purged</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.ExecutionException"/>
		/// <exception cref="System.Exception"/>
		public virtual int Purge(string path, string id, string policyMatch, RegistryAdminService.PurgePolicy
			 purgePolicy)
		{
			return Purge(path, id, policyMatch, purgePolicy, null);
		}

		/// <summary>trigger a purge operation</summary>
		/// <param name="path">pathn</param>
		/// <param name="id">yarn ID</param>
		/// <param name="policyMatch">policy to match ID on</param>
		/// <param name="purgePolicy">policy when there are children under a match</param>
		/// <param name="callback">optional callback</param>
		/// <returns>the number purged</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.ExecutionException"/>
		/// <exception cref="System.Exception"/>
		public virtual int Purge(string path, string id, string policyMatch, RegistryAdminService.PurgePolicy
			 purgePolicy, BackgroundCallback callback)
		{
			Future<int> future = registry.PurgeRecordsAsync(path, id, policyMatch, purgePolicy
				, callback);
			try
			{
				return future.Get();
			}
			catch (ExecutionException e)
			{
				if (e.InnerException is IOException)
				{
					throw (IOException)e.InnerException;
				}
				else
				{
					throw;
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPurgeEntryCuratorCallback()
		{
			string path = "/users/example/hbase/hbase1/";
			ServiceRecord written = BuildExampleServiceEntry(PersistencePolicies.ApplicationAttempt
				);
			written.Set(YarnRegistryAttributes.YarnId, "testAsyncPurgeEntry_attempt_001");
			operations.Mknode(RegistryPathUtils.ParentOf(path), true);
			operations.Bind(path, written, 0);
			ZKPathDumper dump = registry.DumpPath(false);
			CuratorEventCatcher events = new CuratorEventCatcher();
			Log.Info("Initial state {}", dump);
			// container query
			string id = written.Get(YarnRegistryAttributes.YarnId, string.Empty);
			int opcount = Purge("/", id, PersistencePolicies.Container, RegistryAdminService.PurgePolicy
				.PurgeAll, events);
			AssertPathExists(path);
			NUnit.Framework.Assert.AreEqual(0, opcount);
			NUnit.Framework.Assert.AreEqual("Event counter", 0, events.GetCount());
			// now the application attempt
			opcount = Purge("/", id, PersistencePolicies.ApplicationAttempt, RegistryAdminService.PurgePolicy
				.PurgeAll, events);
			Log.Info("Final state {}", dump);
			AssertPathNotFound(path);
			NUnit.Framework.Assert.AreEqual("wrong no of delete operations in " + dump, 1, opcount
				);
			// and validate the callback event
			NUnit.Framework.Assert.AreEqual("Event counter", 1, events.GetCount());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAsyncPurgeEntry()
		{
			string path = "/users/example/hbase/hbase1/";
			ServiceRecord written = BuildExampleServiceEntry(PersistencePolicies.ApplicationAttempt
				);
			written.Set(YarnRegistryAttributes.YarnId, "testAsyncPurgeEntry_attempt_001");
			operations.Mknode(RegistryPathUtils.ParentOf(path), true);
			operations.Bind(path, written, 0);
			ZKPathDumper dump = registry.DumpPath(false);
			Log.Info("Initial state {}", dump);
			DeleteCompletionCallback deletions = new DeleteCompletionCallback();
			int opcount = Purge("/", written.Get(YarnRegistryAttributes.YarnId, string.Empty)
				, PersistencePolicies.Container, RegistryAdminService.PurgePolicy.PurgeAll, deletions
				);
			AssertPathExists(path);
			dump = registry.DumpPath(false);
			NUnit.Framework.Assert.AreEqual("wrong no of delete operations in " + dump, 0, deletions
				.GetEventCount());
			NUnit.Framework.Assert.AreEqual("wrong no of delete operations in " + dump, 0, opcount
				);
			// now app attempt
			deletions = new DeleteCompletionCallback();
			opcount = Purge("/", written.Get(YarnRegistryAttributes.YarnId, string.Empty), PersistencePolicies
				.ApplicationAttempt, RegistryAdminService.PurgePolicy.PurgeAll, deletions);
			dump = registry.DumpPath(false);
			Log.Info("Final state {}", dump);
			AssertPathNotFound(path);
			NUnit.Framework.Assert.AreEqual("wrong no of delete operations in " + dump, 1, deletions
				.GetEventCount());
			NUnit.Framework.Assert.AreEqual("wrong no of delete operations in " + dump, 1, opcount
				);
		}

		// and validate the callback event
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPutGetContainerPersistenceServiceEntry()
		{
			string path = EntryPath;
			ServiceRecord written = BuildExampleServiceEntry(PersistencePolicies.Container);
			operations.Mknode(RegistryPathUtils.ParentOf(path), true);
			operations.Bind(path, written, BindFlags.Create);
			ServiceRecord resolved = operations.Resolve(path);
			ValidateEntry(resolved);
			AssertMatches(written, resolved);
		}

		/// <summary>Create a complex example app</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateComplexApplication()
		{
			string appId = "application_1408631738011_0001";
			string cid = "container_1408631738011_0001_01_";
			string cid1 = cid + "000001";
			string cid2 = cid + "000002";
			string appPath = Userpath + "tomcat";
			ServiceRecord webapp = CreateRecord(appId, PersistencePolicies.Application, "tomcat-based web application"
				, null);
			webapp.AddExternalEndpoint(RegistryTypeUtils.RestEndpoint("www", new URI("http", 
				"//loadbalancer/", null)));
			ServiceRecord comp1 = CreateRecord(cid1, PersistencePolicies.Container, null, null
				);
			comp1.AddExternalEndpoint(RegistryTypeUtils.RestEndpoint("www", new URI("http", "//rack4server3:43572"
				, null)));
			comp1.AddInternalEndpoint(RegistryTypeUtils.InetAddrEndpoint("jmx", "JMX", "rack4server3"
				, 43573));
			// Component 2 has a container lifespan
			ServiceRecord comp2 = CreateRecord(cid2, PersistencePolicies.Container, null, null
				);
			comp2.AddExternalEndpoint(RegistryTypeUtils.RestEndpoint("www", new URI("http", "//rack1server28:35881"
				, null)));
			comp2.AddInternalEndpoint(RegistryTypeUtils.InetAddrEndpoint("jmx", "JMX", "rack1server28"
				, 35882));
			operations.Mknode(Userpath, false);
			operations.Bind(appPath, webapp, BindFlags.Overwrite);
			string componentsPath = appPath + RegistryConstants.SubpathComponents;
			operations.Mknode(componentsPath, false);
			string dns1 = RegistryPathUtils.EncodeYarnID(cid1);
			string dns1path = componentsPath + dns1;
			operations.Bind(dns1path, comp1, BindFlags.Create);
			string dns2 = RegistryPathUtils.EncodeYarnID(cid2);
			string dns2path = componentsPath + dns2;
			operations.Bind(dns2path, comp2, BindFlags.Create);
			ZKPathDumper pathDumper = registry.DumpPath(false);
			Log.Info(pathDumper.ToString());
			LogRecord("tomcat", webapp);
			LogRecord(dns1, comp1);
			LogRecord(dns2, comp2);
			ServiceRecord dns1resolved = operations.Resolve(dns1path);
			NUnit.Framework.Assert.AreEqual("Persistence policies on resolved entry", PersistencePolicies
				.Container, dns1resolved.Get(YarnRegistryAttributes.YarnPersistence, string.Empty
				));
			IDictionary<string, RegistryPathStatus> children = RegistryUtils.StatChildren(operations
				, componentsPath);
			NUnit.Framework.Assert.AreEqual(2, children.Count);
			ICollection<RegistryPathStatus> componentStats = children.Values;
			IDictionary<string, ServiceRecord> records = RegistryUtils.ExtractServiceRecords(
				operations, componentsPath, componentStats);
			NUnit.Framework.Assert.AreEqual(2, records.Count);
			ServiceRecord retrieved1 = records[dns1path];
			LogRecord(retrieved1.Get(YarnRegistryAttributes.YarnId, string.Empty), retrieved1
				);
			AssertMatches(dns1resolved, retrieved1);
			NUnit.Framework.Assert.AreEqual(PersistencePolicies.Container, retrieved1.Get(YarnRegistryAttributes
				.YarnPersistence, string.Empty));
			// create a listing under components/
			operations.Mknode(componentsPath + "subdir", false);
			// this shows up in the listing of child entries
			IDictionary<string, RegistryPathStatus> childrenUpdated = RegistryUtils.StatChildren
				(operations, componentsPath);
			NUnit.Framework.Assert.AreEqual(3, childrenUpdated.Count);
			// the non-record child this is not picked up in the record listing
			IDictionary<string, ServiceRecord> recordsUpdated = RegistryUtils.ExtractServiceRecords
				(operations, componentsPath, childrenUpdated);
			NUnit.Framework.Assert.AreEqual(2, recordsUpdated.Count);
			// now do some deletions.
			// synchronous delete container ID 2
			// fail if the app policy is chosen
			NUnit.Framework.Assert.AreEqual(0, Purge("/", cid2, PersistencePolicies.Application
				, RegistryAdminService.PurgePolicy.FailOnChildren));
			// succeed for container
			NUnit.Framework.Assert.AreEqual(1, Purge("/", cid2, PersistencePolicies.Container
				, RegistryAdminService.PurgePolicy.FailOnChildren));
			AssertPathNotFound(dns2path);
			AssertPathExists(dns1path);
			// expect a skip on children to skip
			NUnit.Framework.Assert.AreEqual(0, Purge("/", appId, PersistencePolicies.Application
				, RegistryAdminService.PurgePolicy.SkipOnChildren));
			AssertPathExists(appPath);
			AssertPathExists(dns1path);
			// attempt to delete app with policy of fail on children
			try
			{
				int p = Purge("/", appId, PersistencePolicies.Application, RegistryAdminService.PurgePolicy
					.FailOnChildren);
				NUnit.Framework.Assert.Fail("expected a failure, got a purge count of " + p);
			}
			catch (PathIsNotEmptyDirectoryException)
			{
			}
			// expected
			AssertPathExists(appPath);
			AssertPathExists(dns1path);
			// now trigger recursive delete
			NUnit.Framework.Assert.AreEqual(1, Purge("/", appId, PersistencePolicies.Application
				, RegistryAdminService.PurgePolicy.PurgeAll));
			AssertPathNotFound(appPath);
			AssertPathNotFound(dns1path);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestChildDeletion()
		{
			ServiceRecord app = CreateRecord("app1", PersistencePolicies.Application, "app", 
				null);
			ServiceRecord container = CreateRecord("container1", PersistencePolicies.Container
				, "container", null);
			operations.Bind("/app", app, BindFlags.Overwrite);
			operations.Bind("/app/container", container, BindFlags.Overwrite);
			try
			{
				int p = Purge("/", "app1", PersistencePolicies.Application, RegistryAdminService.PurgePolicy
					.FailOnChildren);
				NUnit.Framework.Assert.Fail("expected a failure, got a purge count of " + p);
			}
			catch (PathIsNotEmptyDirectoryException)
			{
			}
		}
		// expected
	}
}
