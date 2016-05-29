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
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Registry.Client.Api;
using Org.Apache.Hadoop.Registry.Client.Exceptions;
using Org.Apache.Hadoop.Registry.Client.Impl;
using Org.Apache.Hadoop.Registry.Client.Impl.ZK;
using Org.Apache.Hadoop.Registry.Server.Integration;
using Org.Apache.Hadoop.Registry.Server.Services;
using Org.Apache.Hadoop.Security;
using Org.Apache.Zookeeper.Data;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Secure
{
	/// <summary>
	/// Verify that the
	/// <see cref="Org.Apache.Hadoop.Registry.Server.Integration.RMRegistryOperationsService
	/// 	"/>
	/// works securely
	/// </summary>
	public class TestSecureRMRegistryOperations : AbstractSecureRegistryTest
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(TestSecureRMRegistryOperations
			));

		private Configuration secureConf;

		private Configuration zkClientConf;

		private UserGroupInformation zookeeperUGI;

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void SetupTestSecureRMRegistryOperations()
		{
			StartSecureZK();
			secureConf = new Configuration();
			secureConf.SetBoolean(KeyRegistrySecure, true);
			// create client conf containing the ZK quorum
			zkClientConf = new Configuration(secureZK.GetConfig());
			zkClientConf.SetBoolean(KeyRegistrySecure, true);
			AssertNotEmpty(zkClientConf.Get(RegistryConstants.KeyRegistryZkQuorum));
			// ZK is in charge
			secureConf.Set(KeyRegistrySystemAccounts, "sasl:zookeeper@");
			zookeeperUGI = LoginUGI(Zookeeper, keytab_zk);
		}

		[TearDown]
		public virtual void TeardownTestSecureRMRegistryOperations()
		{
		}

		/// <summary>Create the RM registry operations as the current user</summary>
		/// <returns>the service</returns>
		/// <exception cref="Javax.Security.Auth.Login.LoginException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual RMRegistryOperationsService StartRMRegistryOperations()
		{
			// kerberos
			secureConf.Set(KeyRegistryClientAuth, RegistryClientAuthKerberos);
			secureConf.Set(KeyRegistryClientJaasContext, ZookeeperClientContext);
			RMRegistryOperationsService registryOperations = zookeeperUGI.DoAs(new _PrivilegedExceptionAction_97
				(this));
			return registryOperations;
		}

		private sealed class _PrivilegedExceptionAction_97 : PrivilegedExceptionAction<RMRegistryOperationsService
			>
		{
			public _PrivilegedExceptionAction_97(TestSecureRMRegistryOperations _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public RMRegistryOperationsService Run()
			{
				RMRegistryOperationsService operations = new RMRegistryOperationsService("rmregistry"
					, this._enclosing.secureZK);
				this._enclosing.AddToTeardown(operations);
				operations.Init(this._enclosing.secureConf);
				TestSecureRMRegistryOperations.Log.Info(operations.BindingDiagnosticDetails());
				operations.Start();
				return operations;
			}

			private readonly TestSecureRMRegistryOperations _enclosing;
		}

		/// <summary>test that ZK can write as itself</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestZookeeperCanWriteUnderSystem()
		{
			RMRegistryOperationsService rmRegistryOperations = StartRMRegistryOperations();
			RegistryOperations operations = rmRegistryOperations;
			operations.Mknode(PathSystemServices + "hdfs", false);
			ZKPathDumper pathDumper = rmRegistryOperations.DumpPath(true);
			Log.Info(pathDumper.ToString());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAnonReadAccess()
		{
			RMRegistryOperationsService rmRegistryOperations = StartRMRegistryOperations();
			Describe(Log, "testAnonReadAccess");
			RegistryOperations operations = RegistryOperationsFactory.CreateAnonymousInstance
				(zkClientConf);
			AddToTeardown(operations);
			operations.Start();
			NUnit.Framework.Assert.IsFalse("RegistrySecurity.isClientSASLEnabled()==true", RegistrySecurity
				.IsClientSASLEnabled());
			operations.List(PathSystemServices);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAnonNoWriteAccess()
		{
			RMRegistryOperationsService rmRegistryOperations = StartRMRegistryOperations();
			Describe(Log, "testAnonNoWriteAccess");
			RegistryOperations operations = RegistryOperationsFactory.CreateAnonymousInstance
				(zkClientConf);
			AddToTeardown(operations);
			operations.Start();
			string servicePath = PathSystemServices + "hdfs";
			ExpectMkNodeFailure(operations, servicePath);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAnonNoWriteAccessOffRoot()
		{
			RMRegistryOperationsService rmRegistryOperations = StartRMRegistryOperations();
			Describe(Log, "testAnonNoWriteAccessOffRoot");
			RegistryOperations operations = RegistryOperationsFactory.CreateAnonymousInstance
				(zkClientConf);
			AddToTeardown(operations);
			operations.Start();
			NUnit.Framework.Assert.IsFalse("mknode(/)", operations.Mknode("/", false));
			ExpectMkNodeFailure(operations, "/sub");
			ExpectDeleteFailure(operations, PathSystemServices, true);
		}

		/// <summary>Expect a mknode operation to fail</summary>
		/// <param name="operations">operations instance</param>
		/// <param name="path">path</param>
		/// <exception cref="System.IO.IOException">An IO failure other than those permitted</exception>
		public virtual void ExpectMkNodeFailure(RegistryOperations operations, string path
			)
		{
			try
			{
				operations.Mknode(path, false);
				NUnit.Framework.Assert.Fail("should have failed to create a node under " + path);
			}
			catch (PathPermissionException)
			{
			}
			catch (NoPathPermissionsException)
			{
			}
		}

		// expected
		// expected
		/// <summary>Expect a delete operation to fail</summary>
		/// <param name="operations">operations instance</param>
		/// <param name="path">path</param>
		/// <param name="recursive"/>
		/// <exception cref="System.IO.IOException">An IO failure other than those permitted</exception>
		public virtual void ExpectDeleteFailure(RegistryOperations operations, string path
			, bool recursive)
		{
			try
			{
				operations.Delete(path, recursive);
				NUnit.Framework.Assert.Fail("should have failed to delete the node " + path);
			}
			catch (PathPermissionException)
			{
			}
			catch (NoPathPermissionsException)
			{
			}
		}

		// expected
		// expected
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAlicePathRestrictedAnonAccess()
		{
			RMRegistryOperationsService rmRegistryOperations = StartRMRegistryOperations();
			string aliceHome = rmRegistryOperations.InitUserRegistry(Alice);
			Describe(Log, "Creating anonymous accessor");
			RegistryOperations anonOperations = RegistryOperationsFactory.CreateAnonymousInstance
				(zkClientConf);
			AddToTeardown(anonOperations);
			anonOperations.Start();
			anonOperations.List(aliceHome);
			ExpectMkNodeFailure(anonOperations, aliceHome + "/anon");
			ExpectDeleteFailure(anonOperations, aliceHome, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUserZookeeperHomePathAccess()
		{
			RMRegistryOperationsService rmRegistryOperations = StartRMRegistryOperations();
			string home = rmRegistryOperations.InitUserRegistry(Zookeeper);
			Describe(Log, "Creating ZK client");
			RegistryOperations operations = zookeeperUGI.DoAs(new _PrivilegedExceptionAction_232
				(this));
			operations.List(home);
			string path = home + "/subpath";
			operations.Mknode(path, false);
			operations.Delete(path, true);
		}

		private sealed class _PrivilegedExceptionAction_232 : PrivilegedExceptionAction<RegistryOperations
			>
		{
			public _PrivilegedExceptionAction_232(TestSecureRMRegistryOperations _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public RegistryOperations Run()
			{
				RegistryOperations operations = RegistryOperationsFactory.CreateKerberosInstance(
					this._enclosing.zkClientConf, AbstractSecureRegistryTest.ZookeeperClientContext);
				this._enclosing.AddToTeardown(operations);
				operations.Start();
				return operations;
			}

			private readonly TestSecureRMRegistryOperations _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUserHomedirsPermissionsRestricted()
		{
			// test that the /users/$user permissions are restricted
			RMRegistryOperationsService rmRegistryOperations = StartRMRegistryOperations();
			// create Alice's dir, so it should have an ACL for Alice
			string home = rmRegistryOperations.InitUserRegistry(Alice);
			IList<ACL> acls = rmRegistryOperations.ZkGetACLS(home);
			ACL aliceACL = null;
			foreach (ACL acl in acls)
			{
				Log.Info(RegistrySecurity.AclToString(acl));
				ID id = acl.GetId();
				if (id.GetScheme().Equals(ZookeeperConfigOptions.SchemeSasl) && id.GetId().StartsWith
					(Alice))
				{
					aliceACL = acl;
					break;
				}
			}
			NUnit.Framework.Assert.IsNotNull(aliceACL);
			NUnit.Framework.Assert.AreEqual(RegistryAdminService.UserHomedirAclPermissions, aliceACL
				.GetPerms());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDigestAccess()
		{
			RMRegistryOperationsService registryAdmin = StartRMRegistryOperations();
			string id = "username";
			string pass = "password";
			registryAdmin.AddWriteAccessor(id, pass);
			IList<ACL> clientAcls = registryAdmin.GetClientAcls();
			Log.Info("Client ACLS=\n{}", RegistrySecurity.AclsToString(clientAcls));
			string @base = "/digested";
			registryAdmin.Mknode(@base, false);
			IList<ACL> baseACLs = registryAdmin.ZkGetACLS(@base);
			string aclset = RegistrySecurity.AclsToString(baseACLs);
			Log.Info("Base ACLs=\n{}", aclset);
			ACL found = null;
			foreach (ACL acl in baseACLs)
			{
				if (ZookeeperConfigOptions.SchemeDigest.Equals(acl.GetId().GetScheme()))
				{
					found = acl;
					break;
				}
			}
			NUnit.Framework.Assert.IsNotNull("Did not find digest entry in ACLs " + aclset, found
				);
			zkClientConf.Set(KeyRegistryUserAccounts, "sasl:somebody@EXAMPLE.COM, sasl:other"
				);
			RegistryOperations operations = RegistryOperationsFactory.CreateAuthenticatedInstance
				(zkClientConf, id, pass);
			AddToTeardown(operations);
			operations.Start();
			RegistryOperationsClient operationsClient = (RegistryOperationsClient)operations;
			IList<ACL> digestClientACLs = operationsClient.GetClientAcls();
			Log.Info("digest client ACLs=\n{}", RegistrySecurity.AclsToString(digestClientACLs
				));
			operations.Stat(@base);
			operations.Mknode(@base + "/subdir", false);
			ZKPathDumper pathDumper = registryAdmin.DumpPath(true);
			Log.Info(pathDumper.ToString());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNoDigestAuthMissingId()
		{
			RegistryOperationsFactory.CreateAuthenticatedInstance(zkClientConf, string.Empty, 
				"pass");
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNoDigestAuthMissingId2()
		{
			zkClientConf.Set(KeyRegistryClientAuth, RegistryClientAuthDigest);
			zkClientConf.Set(KeyRegistryClientAuthenticationId, string.Empty);
			zkClientConf.Set(KeyRegistryClientAuthenticationPassword, "pass");
			RegistryOperationsFactory.CreateInstance("DigestRegistryOperations", zkClientConf
				);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNoDigestAuthMissingPass()
		{
			RegistryOperationsFactory.CreateAuthenticatedInstance(zkClientConf, "id", string.Empty
				);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNoDigestAuthMissingPass2()
		{
			zkClientConf.Set(KeyRegistryClientAuth, RegistryClientAuthDigest);
			zkClientConf.Set(KeyRegistryClientAuthenticationId, "id");
			zkClientConf.Set(KeyRegistryClientAuthenticationPassword, string.Empty);
			RegistryOperationsFactory.CreateInstance("DigestRegistryOperations", zkClientConf
				);
		}
	}
}
