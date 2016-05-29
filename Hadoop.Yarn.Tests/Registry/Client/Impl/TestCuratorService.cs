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
using NUnit.Framework;
using Org.Apache.Curator.Framework.Api;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Registry;
using Org.Apache.Hadoop.Registry.Client.Impl.ZK;
using Org.Apache.Hadoop.Service;
using Org.Apache.Zookeeper;
using Org.Apache.Zookeeper.Data;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Client.Impl
{
	/// <summary>Test the curator service</summary>
	public class TestCuratorService : AbstractZKRegistryTest
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(TestCuratorService
			));

		protected internal CuratorService curatorService;

		public const string Missing = "/missing";

		private IList<ACL> rootACL;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void StartCurator()
		{
			CreateCuratorService();
		}

		[TearDown]
		public virtual void StopCurator()
		{
			ServiceOperations.Stop(curatorService);
		}

		/// <summary>Create an instance</summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void CreateCuratorService()
		{
			curatorService = new CuratorService("curatorService");
			curatorService.Init(CreateRegistryConfiguration());
			curatorService.Start();
			rootACL = RegistrySecurity.WorldReadWriteACL;
			curatorService.MaybeCreate(string.Empty, CreateMode.Persistent, rootACL, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLs()
		{
			curatorService.ZkList("/");
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestLsNotFound()
		{
			IList<string> ls = curatorService.ZkList(Missing);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestExists()
		{
			NUnit.Framework.Assert.IsTrue(curatorService.ZkPathExists("/"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestExistsMissing()
		{
			NUnit.Framework.Assert.IsFalse(curatorService.ZkPathExists(Missing));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestVerifyExists()
		{
			PathMustExist("/");
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestVerifyExistsMissing()
		{
			PathMustExist("/file-not-found");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMkdirs()
		{
			MkPath("/p1", CreateMode.Persistent);
			PathMustExist("/p1");
			MkPath("/p1/p2", CreateMode.Ephemeral);
			PathMustExist("/p1/p2");
		}

		/// <exception cref="System.IO.IOException"/>
		private void MkPath(string path, CreateMode mode)
		{
			curatorService.ZkMkPath(path, mode, false, RegistrySecurity.WorldReadWriteACL);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void PathMustExist(string path)
		{
			curatorService.ZkPathMustExist(path);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMkdirChild()
		{
			MkPath("/testMkdirChild/child", CreateMode.Persistent);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMaybeCreate()
		{
			NUnit.Framework.Assert.IsTrue(curatorService.MaybeCreate("/p3", CreateMode.Persistent
				, RegistrySecurity.WorldReadWriteACL, false));
			NUnit.Framework.Assert.IsFalse(curatorService.MaybeCreate("/p3", CreateMode.Persistent
				, RegistrySecurity.WorldReadWriteACL, false));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRM()
		{
			MkPath("/rm", CreateMode.Persistent);
			curatorService.ZkDelete("/rm", false, null);
			VerifyNotExists("/rm");
			curatorService.ZkDelete("/rm", false, null);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRMNonRf()
		{
			MkPath("/rm", CreateMode.Persistent);
			MkPath("/rm/child", CreateMode.Persistent);
			try
			{
				curatorService.ZkDelete("/rm", false, null);
				NUnit.Framework.Assert.Fail("expected a failure");
			}
			catch (PathIsNotEmptyDirectoryException)
			{
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRMRf()
		{
			MkPath("/rm", CreateMode.Persistent);
			MkPath("/rm/child", CreateMode.Persistent);
			curatorService.ZkDelete("/rm", true, null);
			VerifyNotExists("/rm");
			curatorService.ZkDelete("/rm", true, null);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBackgroundDelete()
		{
			MkPath("/rm", CreateMode.Persistent);
			MkPath("/rm/child", CreateMode.Persistent);
			CuratorEventCatcher events = new CuratorEventCatcher();
			curatorService.ZkDelete("/rm", true, events);
			CuratorEvent taken = events.Take();
			Log.Info("took {}", taken);
			NUnit.Framework.Assert.AreEqual(1, events.GetCount());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCreate()
		{
			curatorService.ZkCreate("/testcreate", CreateMode.Persistent, GetTestBuffer(), rootACL
				);
			PathMustExist("/testcreate");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateTwice()
		{
			byte[] buffer = GetTestBuffer();
			curatorService.ZkCreate("/testcreatetwice", CreateMode.Persistent, buffer, rootACL
				);
			try
			{
				curatorService.ZkCreate("/testcreatetwice", CreateMode.Persistent, buffer, rootACL
					);
				NUnit.Framework.Assert.Fail();
			}
			catch (FileAlreadyExistsException)
			{
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateUpdate()
		{
			byte[] buffer = GetTestBuffer();
			curatorService.ZkCreate("/testcreateupdate", CreateMode.Persistent, buffer, rootACL
				);
			curatorService.ZkUpdate("/testcreateupdate", buffer);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestUpdateMissing()
		{
			curatorService.ZkUpdate("/testupdatemissing", GetTestBuffer());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUpdateDirectory()
		{
			MkPath("/testupdatedirectory", CreateMode.Persistent);
			curatorService.ZkUpdate("/testupdatedirectory", GetTestBuffer());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUpdateDirectorywithChild()
		{
			MkPath("/testupdatedirectorywithchild", CreateMode.Persistent);
			MkPath("/testupdatedirectorywithchild/child", CreateMode.Persistent);
			curatorService.ZkUpdate("/testupdatedirectorywithchild", GetTestBuffer());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUseZKServiceForBinding()
		{
			CuratorService cs2 = new CuratorService("curator", zookeeper);
			cs2.Init(new Configuration());
			cs2.Start();
		}

		protected internal virtual byte[] GetTestBuffer()
		{
			byte[] buffer = new byte[1];
			buffer[0] = (byte)('0');
			return buffer;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void VerifyNotExists(string path)
		{
			if (curatorService.ZkPathExists(path))
			{
				NUnit.Framework.Assert.Fail("Path should not exist: " + path);
			}
		}
	}
}
