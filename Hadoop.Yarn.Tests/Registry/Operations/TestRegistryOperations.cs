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
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Registry;
using Org.Apache.Hadoop.Registry.Client.Api;
using Org.Apache.Hadoop.Registry.Client.Binding;
using Org.Apache.Hadoop.Registry.Client.Exceptions;
using Org.Apache.Hadoop.Registry.Client.Types;
using Org.Apache.Hadoop.Registry.Client.Types.Yarn;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Operations
{
	public class TestRegistryOperations : AbstractRegistryTest
	{
		protected internal static readonly Logger Log = LoggerFactory.GetLogger(typeof(TestRegistryOperations
			));

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPutGetServiceEntry()
		{
			ServiceRecord written = PutExampleServiceEntry(EntryPath, 0, PersistencePolicies.
				Application);
			ServiceRecord resolved = operations.Resolve(EntryPath);
			ValidateEntry(resolved);
			AssertMatches(written, resolved);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDeleteServiceEntry()
		{
			PutExampleServiceEntry(EntryPath, 0);
			operations.Delete(EntryPath, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDeleteNonexistentEntry()
		{
			operations.Delete(EntryPath, false);
			operations.Delete(EntryPath, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStat()
		{
			PutExampleServiceEntry(EntryPath, 0);
			RegistryPathStatus stat = operations.Stat(EntryPath);
			NUnit.Framework.Assert.IsTrue(stat.size > 0);
			NUnit.Framework.Assert.IsTrue(stat.time > 0);
			NUnit.Framework.Assert.AreEqual(Name, stat.path);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLsParent()
		{
			ServiceRecord written = PutExampleServiceEntry(EntryPath, 0);
			RegistryPathStatus stat = operations.Stat(EntryPath);
			IList<string> children = operations.List(ParentPath);
			NUnit.Framework.Assert.AreEqual(1, children.Count);
			NUnit.Framework.Assert.AreEqual(Name, children[0]);
			IDictionary<string, RegistryPathStatus> childStats = RegistryUtils.StatChildren(operations
				, ParentPath);
			NUnit.Framework.Assert.AreEqual(1, childStats.Count);
			NUnit.Framework.Assert.AreEqual(stat, childStats[Name]);
			IDictionary<string, ServiceRecord> records = RegistryUtils.ExtractServiceRecords(
				operations, ParentPath, childStats.Values);
			NUnit.Framework.Assert.AreEqual(1, records.Count);
			ServiceRecord record = records[EntryPath];
			RegistryTypeUtils.ValidateServiceRecord(EntryPath, record);
			AssertMatches(written, record);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDeleteNonEmpty()
		{
			PutExampleServiceEntry(EntryPath, 0);
			try
			{
				operations.Delete(ParentPath, false);
				NUnit.Framework.Assert.Fail("Expected a failure");
			}
			catch (PathIsNotEmptyDirectoryException)
			{
			}
			// expected; ignore
			operations.Delete(ParentPath, true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestStatEmptyPath()
		{
			operations.Stat(EntryPath);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestLsEmptyPath()
		{
			operations.List(ParentPath);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestResolveEmptyPath()
		{
			operations.Resolve(EntryPath);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMkdirNoParent()
		{
			string path = EntryPath + "/missing";
			try
			{
				operations.Mknode(path, false);
				RegistryPathStatus stat = operations.Stat(path);
				NUnit.Framework.Assert.Fail("Got a status " + stat);
			}
			catch (PathNotFoundException)
			{
			}
		}

		// expected
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDoubleMkdir()
		{
			operations.Mknode(Userpath, false);
			string path = Userpath + "newentry";
			NUnit.Framework.Assert.IsTrue(operations.Mknode(path, false));
			operations.Stat(path);
			NUnit.Framework.Assert.IsFalse(operations.Mknode(path, false));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPutNoParent()
		{
			ServiceRecord record = new ServiceRecord();
			record.Set(YarnRegistryAttributes.YarnId, "testPutNoParent");
			string path = "/path/without/parent";
			try
			{
				operations.Bind(path, record, 0);
				// didn't get a failure
				// trouble
				RegistryPathStatus stat = operations.Stat(path);
				NUnit.Framework.Assert.Fail("Got a status " + stat);
			}
			catch (PathNotFoundException)
			{
			}
		}

		// expected
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPutMinimalRecord()
		{
			string path = "/path/with/minimal";
			operations.Mknode(path, true);
			ServiceRecord record = new ServiceRecord();
			operations.Bind(path, record, BindFlags.Overwrite);
			ServiceRecord resolve = operations.Resolve(path);
			AssertMatches(record, resolve);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestPutNoParent2()
		{
			ServiceRecord record = new ServiceRecord();
			record.Set(YarnRegistryAttributes.YarnId, "testPutNoParent");
			string path = "/path/without/parent";
			operations.Bind(path, record, 0);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStatDirectory()
		{
			string empty = "/empty";
			operations.Mknode(empty, false);
			operations.Stat(empty);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStatRootPath()
		{
			operations.Mknode("/", false);
			operations.Stat("/");
			operations.List("/");
			operations.List("/");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStatOneLevelDown()
		{
			operations.Mknode("/subdir", true);
			operations.Stat("/subdir");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLsRootPath()
		{
			string empty = "/";
			operations.Mknode(empty, false);
			operations.Stat(empty);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestResolvePathThatHasNoEntry()
		{
			string empty = "/empty2";
			operations.Mknode(empty, false);
			try
			{
				ServiceRecord record = operations.Resolve(empty);
				NUnit.Framework.Assert.Fail("expected an exception, got " + record);
			}
			catch (NoRecordException)
			{
			}
		}

		// expected
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOverwrite()
		{
			ServiceRecord written = PutExampleServiceEntry(EntryPath, 0);
			ServiceRecord resolved1 = operations.Resolve(EntryPath);
			resolved1.description = "resolved1";
			try
			{
				operations.Bind(EntryPath, resolved1, 0);
				NUnit.Framework.Assert.Fail("overwrite succeeded when it should have failed");
			}
			catch (FileAlreadyExistsException)
			{
			}
			// expected
			// verify there's no changed
			ServiceRecord resolved2 = operations.Resolve(EntryPath);
			AssertMatches(written, resolved2);
			operations.Bind(EntryPath, resolved1, BindFlags.Overwrite);
			ServiceRecord resolved3 = operations.Resolve(EntryPath);
			AssertMatches(resolved1, resolved3);
		}

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

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAddingWriteAccessIsNoOpEntry()
		{
			NUnit.Framework.Assert.IsFalse(operations.AddWriteAccessor("id", "pass"));
			operations.ClearWriteAccessors();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestListListFully()
		{
			ServiceRecord r1 = new ServiceRecord();
			ServiceRecord r2 = CreateRecord("i", PersistencePolicies.Permanent, "r2");
			string path = Userpath + ScHadoop + "/listing";
			operations.Mknode(path, true);
			string r1path = path + "/r1";
			operations.Bind(r1path, r1, 0);
			string r2path = path + "/r2";
			operations.Bind(r2path, r2, 0);
			RegistryPathStatus r1stat = operations.Stat(r1path);
			NUnit.Framework.Assert.AreEqual("r1", r1stat.path);
			RegistryPathStatus r2stat = operations.Stat(r2path);
			NUnit.Framework.Assert.AreEqual("r2", r2stat.path);
			AssertNotEquals(r1stat, r2stat);
			// listings now
			IList<string> list = operations.List(path);
			NUnit.Framework.Assert.AreEqual("Wrong no. of children", 2, list.Count);
			// there's no order here, so create one
			IDictionary<string, string> names = new Dictionary<string, string>();
			string entries = string.Empty;
			foreach (string child in list)
			{
				names[child] = child;
				entries += child + " ";
			}
			NUnit.Framework.Assert.IsTrue("No 'r1' in " + entries, names.Contains("r1"));
			NUnit.Framework.Assert.IsTrue("No 'r2' in " + entries, names.Contains("r2"));
			IDictionary<string, RegistryPathStatus> stats = RegistryUtils.StatChildren(operations
				, path);
			NUnit.Framework.Assert.AreEqual("Wrong no. of children", 2, stats.Count);
			NUnit.Framework.Assert.AreEqual(r1stat, stats["r1"]);
			NUnit.Framework.Assert.AreEqual(r2stat, stats["r2"]);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestComplexUsernames()
		{
			operations.Mknode("/users/user with spaces", true);
			operations.Mknode("/users/user-with_underscores", true);
			operations.Mknode("/users/000000", true);
			operations.Mknode("/users/-storm", true);
			operations.Mknode("/users/windows\\ user", true);
			string home = RegistryUtils.HomePathForUser("\u0413PA\u0414_3");
			operations.Mknode(home, true);
			operations.Mknode(RegistryUtils.ServicePath(home, "service.class", "service 4_5")
				, true);
			operations.Mknode(RegistryUtils.HomePathForUser("hbase@HADOOP.APACHE.ORG"), true);
			operations.Mknode(RegistryUtils.HomePathForUser("hbase/localhost@HADOOP.APACHE.ORG"
				), true);
			home = RegistryUtils.HomePathForUser("ADMINISTRATOR/127.0.0.1");
			NUnit.Framework.Assert.IsTrue("No 'administrator' in " + home, home.Contains("administrator"
				));
			operations.Mknode(home, true);
		}
	}
}
