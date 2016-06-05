using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Records;
using Org.Apache.Hadoop.Yarn.Server.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery
{
	public class TestFSRMStateStore : RMStateStoreTestBase
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.TestFSRMStateStore
			));

		private TestFSRMStateStore.TestFSRMStateStoreTester fsTester;

		internal class TestFSRMStateStoreTester : RMStateStoreTestBase.RMStateStoreHelper
		{
			internal Path workingDirPathURI;

			internal TestFSRMStateStore.TestFSRMStateStoreTester.TestFileSystemRMStore store;

			internal MiniDFSCluster cluster;

			internal bool adminCheckEnable;

			internal class TestFileSystemRMStore : FileSystemRMStateStore
			{
				/// <exception cref="System.Exception"/>
				internal TestFileSystemRMStore(TestFSRMStateStoreTester _enclosing, Configuration
					 conf)
				{
					this._enclosing = _enclosing;
					this.Init(conf);
					NUnit.Framework.Assert.IsNull(this.fs);
					NUnit.Framework.Assert.IsTrue(this._enclosing.workingDirPathURI.Equals(this.fsWorkingPath
						));
					this.Start();
					NUnit.Framework.Assert.IsNotNull(this.fs);
				}

				public virtual Path GetVersionNode()
				{
					return new Path(new Path(this._enclosing.workingDirPathURI, FileSystemRMStateStore
						.RootDirName), RMStateStore.VersionNode);
				}

				protected internal override Version GetCurrentVersion()
				{
					return FileSystemRMStateStore.CurrentVersionInfo;
				}

				public virtual Path GetAppDir(string appId)
				{
					Path rootDir = new Path(this._enclosing.workingDirPathURI, FileSystemRMStateStore
						.RootDirName);
					Path appRootDir = new Path(rootDir, RMStateStore.RmAppRoot);
					Path appDir = new Path(appRootDir, appId);
					return appDir;
				}

				private readonly TestFSRMStateStoreTester _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public TestFSRMStateStoreTester(TestFSRMStateStore _enclosing, MiniDFSCluster cluster
				, bool adminCheckEnable)
			{
				this._enclosing = _enclosing;
				Path workingDirPath = new Path("/yarn/Test");
				this.adminCheckEnable = adminCheckEnable;
				this.cluster = cluster;
				FileSystem fs = cluster.GetFileSystem();
				fs.Mkdirs(workingDirPath);
				Path clusterURI = new Path(cluster.GetURI());
				this.workingDirPathURI = new Path(clusterURI, workingDirPath);
				fs.Close();
			}

			/// <exception cref="System.Exception"/>
			public virtual RMStateStore GetRMStateStore()
			{
				YarnConfiguration conf = new YarnConfiguration();
				conf.Set(YarnConfiguration.FsRmStateStoreUri, this.workingDirPathURI.ToString());
				conf.Set(YarnConfiguration.FsRmStateStoreRetryPolicySpec, "100,6000");
				conf.SetInt(YarnConfiguration.FsRmStateStoreNumRetries, 8);
				conf.SetLong(YarnConfiguration.FsRmStateStoreRetryIntervalMs, 900L);
				if (this.adminCheckEnable)
				{
					conf.SetBoolean(YarnConfiguration.YarnIntermediateDataEncryption, true);
				}
				this.store = new TestFSRMStateStore.TestFSRMStateStoreTester.TestFileSystemRMStore
					(this, conf);
				NUnit.Framework.Assert.AreEqual(this.store.GetNumRetries(), 8);
				NUnit.Framework.Assert.AreEqual(this.store.GetRetryInterval(), 900L);
				return this.store;
			}

			/// <exception cref="System.Exception"/>
			public virtual bool IsFinalStateValid()
			{
				FileSystem fs = this.cluster.GetFileSystem();
				FileStatus[] files = fs.ListStatus(this.workingDirPathURI);
				return files.Length == 1;
			}

			/// <exception cref="System.Exception"/>
			public virtual void WriteVersion(Version version)
			{
				this.store.UpdateFile(this.store.GetVersionNode(), ((VersionPBImpl)version).GetProto
					().ToByteArray(), false);
			}

			/// <exception cref="System.Exception"/>
			public virtual Version GetCurrentVersion()
			{
				return this.store.GetCurrentVersion();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual bool AppExists(RMApp app)
			{
				FileSystem fs = this.cluster.GetFileSystem();
				Path nodePath = this.store.GetAppDir(app.GetApplicationId().ToString());
				return fs.Exists(nodePath);
			}

			private readonly TestFSRMStateStore _enclosing;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFSRMStateStore()
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			try
			{
				fsTester = new TestFSRMStateStore.TestFSRMStateStoreTester(this, cluster, false);
				// If the state store is FileSystemRMStateStore then add corrupted entry.
				// It should discard the entry and remove it from file system.
				FSDataOutputStream fsOut = null;
				FileSystemRMStateStore fileSystemRMStateStore = (FileSystemRMStateStore)fsTester.
					GetRMStateStore();
				string appAttemptIdStr3 = "appattempt_1352994193343_0001_000003";
				ApplicationAttemptId attemptId3 = ConverterUtils.ToApplicationAttemptId(appAttemptIdStr3
					);
				Path appDir = fsTester.store.GetAppDir(attemptId3.GetApplicationId().ToString());
				Path tempAppAttemptFile = new Path(appDir, attemptId3.ToString() + ".tmp");
				fsOut = fileSystemRMStateStore.fs.Create(tempAppAttemptFile, false);
				fsOut.Write(Sharpen.Runtime.GetBytesForString("Some random data "));
				fsOut.Close();
				TestRMAppStateStore(fsTester);
				NUnit.Framework.Assert.IsFalse(fsTester.workingDirPathURI.GetFileSystem(conf).Exists
					(tempAppAttemptFile));
				TestRMDTSecretManagerStateStore(fsTester);
				TestCheckVersion(fsTester);
				TestEpoch(fsTester);
				TestAppDeletion(fsTester);
				TestDeleteStore(fsTester);
				TestAMRMTokenSecretManagerStateStore(fsTester);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestHDFSRMStateStore()
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			UserGroupInformation yarnAdmin = UserGroupInformation.CreateUserForTesting("yarn"
				, new string[] { "admin" });
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.GetFileSystem().Mkdir(new Path("/yarn"), FsPermission.ValueOf("-rwxrwxrwx"
				));
			cluster.GetFileSystem().SetOwner(new Path("/yarn"), "yarn", "admin");
			UserGroupInformation hdfsAdmin = UserGroupInformation.GetCurrentUser();
			RMStateStoreTestBase.StoreStateVerifier verifier = new _StoreStateVerifier_200(this
				, hdfsAdmin, cluster);
			// Wait for things to settle
			// Wait for things to settle
			try
			{
				yarnAdmin.DoAs(new _PrivilegedExceptionAction_243(this, cluster, verifier));
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		private sealed class _StoreStateVerifier_200 : RMStateStoreTestBase.StoreStateVerifier
		{
			public _StoreStateVerifier_200(TestFSRMStateStore _enclosing, UserGroupInformation
				 hdfsAdmin, MiniDFSCluster cluster)
			{
				this._enclosing = _enclosing;
				this.hdfsAdmin = hdfsAdmin;
				this.cluster = cluster;
			}

			internal override void AfterStoreApp(RMStateStore store, ApplicationId appId)
			{
				try
				{
					Sharpen.Thread.Sleep(5000);
					hdfsAdmin.DoAs(new _PrivilegedExceptionAction_207(this, cluster, store, appId));
				}
				catch (Exception e)
				{
					throw new RuntimeException(e);
				}
			}

			private sealed class _PrivilegedExceptionAction_207 : PrivilegedExceptionAction<Void
				>
			{
				public _PrivilegedExceptionAction_207(_StoreStateVerifier_200 _enclosing, MiniDFSCluster
					 cluster, RMStateStore store, ApplicationId appId)
				{
					this._enclosing = _enclosing;
					this.cluster = cluster;
					this.store = store;
					this.appId = appId;
				}

				/// <exception cref="System.Exception"/>
				public Void Run()
				{
					this._enclosing._enclosing.VerifyFilesUnreadablebyHDFS(cluster, ((FileSystemRMStateStore
						)store).GetAppDir(appId));
					return null;
				}

				private readonly _StoreStateVerifier_200 _enclosing;

				private readonly MiniDFSCluster cluster;

				private readonly RMStateStore store;

				private readonly ApplicationId appId;
			}

			internal override void AfterStoreAppAttempt(RMStateStore store, ApplicationAttemptId
				 appAttId)
			{
				try
				{
					Sharpen.Thread.Sleep(5000);
					hdfsAdmin.DoAs(new _PrivilegedExceptionAction_228(this, cluster, store, appAttId)
						);
				}
				catch (Exception e)
				{
					throw new RuntimeException(e);
				}
			}

			private sealed class _PrivilegedExceptionAction_228 : PrivilegedExceptionAction<Void
				>
			{
				public _PrivilegedExceptionAction_228(_StoreStateVerifier_200 _enclosing, MiniDFSCluster
					 cluster, RMStateStore store, ApplicationAttemptId appAttId)
				{
					this._enclosing = _enclosing;
					this.cluster = cluster;
					this.store = store;
					this.appAttId = appAttId;
				}

				/// <exception cref="System.Exception"/>
				public Void Run()
				{
					this._enclosing._enclosing.VerifyFilesUnreadablebyHDFS(cluster, ((FileSystemRMStateStore
						)store).GetAppAttemptDir(appAttId));
					return null;
				}

				private readonly _StoreStateVerifier_200 _enclosing;

				private readonly MiniDFSCluster cluster;

				private readonly RMStateStore store;

				private readonly ApplicationAttemptId appAttId;
			}

			private readonly TestFSRMStateStore _enclosing;

			private readonly UserGroupInformation hdfsAdmin;

			private readonly MiniDFSCluster cluster;
		}

		private sealed class _PrivilegedExceptionAction_243 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_243(TestFSRMStateStore _enclosing, MiniDFSCluster
				 cluster, RMStateStoreTestBase.StoreStateVerifier verifier)
			{
				this._enclosing = _enclosing;
				this.cluster = cluster;
				this.verifier = verifier;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				this._enclosing.fsTester = new TestFSRMStateStore.TestFSRMStateStoreTester(this, 
					cluster, true);
				this._enclosing.TestRMAppStateStore(this._enclosing.fsTester, verifier);
				return null;
			}

			private readonly TestFSRMStateStore _enclosing;

			private readonly MiniDFSCluster cluster;

			private readonly RMStateStoreTestBase.StoreStateVerifier verifier;
		}

		/// <exception cref="System.Exception"/>
		private void VerifyFilesUnreadablebyHDFS(MiniDFSCluster cluster, Path root)
		{
			DistributedFileSystem fs = cluster.GetFileSystem();
			Queue<Path> paths = new List<Path>();
			paths.AddItem(root);
			while (!paths.IsEmpty())
			{
				Path p = paths.Poll();
				FileStatus stat = fs.GetFileStatus(p);
				if (!stat.IsDirectory())
				{
					try
					{
						Log.Warn("\n\n ##Testing path [" + p + "]\n\n");
						fs.Open(p);
						NUnit.Framework.Assert.Fail("Super user should not be able to read [" + UserGroupInformation
							.GetCurrentUser() + "] [" + p.GetName() + "]");
					}
					catch (AccessControlException e)
					{
						NUnit.Framework.Assert.IsTrue(e.Message.Contains("superuser is not allowed to perform this operation"
							));
					}
					catch (Exception)
					{
						NUnit.Framework.Assert.Fail("Should get an AccessControlException here");
					}
				}
				if (stat.IsDirectory())
				{
					FileStatus[] ls = fs.ListStatus(p);
					foreach (FileStatus f in ls)
					{
						paths.AddItem(f.GetPath());
					}
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCheckMajorVersionChange()
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			try
			{
				fsTester = new _TestFSRMStateStoreTester_291(this, cluster, false);
				// default version
				RMStateStore store = fsTester.GetRMStateStore();
				Version defaultVersion = fsTester.GetCurrentVersion();
				store.CheckVersion();
				NUnit.Framework.Assert.AreEqual(defaultVersion, store.LoadVersion());
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		private sealed class _TestFSRMStateStoreTester_291 : TestFSRMStateStore.TestFSRMStateStoreTester
		{
			public _TestFSRMStateStoreTester_291(TestFSRMStateStore _enclosing, MiniDFSCluster
				 baseArg1, bool baseArg2)
				: base(_enclosing, baseArg1, baseArg2)
			{
				this._enclosing = _enclosing;
				this.VersionInfo = Version.NewInstance(int.MaxValue, 0);
			}

			internal Version VersionInfo;

			/// <exception cref="System.Exception"/>
			public override Version GetCurrentVersion()
			{
				return this.VersionInfo;
			}

			/// <exception cref="System.Exception"/>
			public override RMStateStore GetRMStateStore()
			{
				YarnConfiguration conf = new YarnConfiguration();
				conf.Set(YarnConfiguration.FsRmStateStoreUri, this.workingDirPathURI.ToString());
				conf.Set(YarnConfiguration.FsRmStateStoreRetryPolicySpec, "100,6000");
				this.store = new _TestFileSystemRMStore_306(this, conf);
				return this.store;
			}

			private sealed class _TestFileSystemRMStore_306 : TestFSRMStateStore.TestFSRMStateStoreTester.TestFileSystemRMStore
			{
				public _TestFileSystemRMStore_306(_TestFSRMStateStoreTester_291 _enclosing, Configuration
					 baseArg1)
					: base(_enclosing, baseArg1)
				{
					this._enclosing = _enclosing;
					this.storedVersion = null;
				}

				internal Version storedVersion;

				protected internal override Version GetCurrentVersion()
				{
					return this._enclosing.VersionInfo;
				}

				/// <exception cref="System.Exception"/>
				protected internal override Version LoadVersion()
				{
					lock (this)
					{
						return this.storedVersion;
					}
				}

				/// <exception cref="System.Exception"/>
				protected internal override void StoreVersion()
				{
					lock (this)
					{
						this.storedVersion = this._enclosing.VersionInfo;
					}
				}

				private readonly _TestFSRMStateStoreTester_291 _enclosing;
			}

			private readonly TestFSRMStateStore _enclosing;
		}

		/// <exception cref="System.Exception"/>
		protected internal override void ModifyAppState()
		{
			// imitate appAttemptFile1 is still .new, but old one is deleted
			string appAttemptIdStr1 = "appattempt_1352994193343_0001_000001";
			ApplicationAttemptId attemptId1 = ConverterUtils.ToApplicationAttemptId(appAttemptIdStr1
				);
			Path appDir = fsTester.store.GetAppDir(attemptId1.GetApplicationId().ToString());
			Path appAttemptFile1 = new Path(appDir, attemptId1.ToString() + ".new");
			FileSystemRMStateStore fileSystemRMStateStore = (FileSystemRMStateStore)fsTester.
				GetRMStateStore();
			fileSystemRMStateStore.RenameFile(appAttemptFile1, new Path(appAttemptFile1.GetParent
				(), appAttemptFile1.GetName() + ".new"));
		}

		/// <exception cref="System.Exception"/>
		protected internal override void ModifyRMDelegationTokenState()
		{
			// imitate dt file is still .new, but old one is deleted
			Path nodeCreatePath = fsTester.store.GetNodePath(fsTester.store.rmDTSecretManagerRoot
				, FileSystemRMStateStore.DelegationTokenPrefix + 0);
			FileSystemRMStateStore fileSystemRMStateStore = (FileSystemRMStateStore)fsTester.
				GetRMStateStore();
			fileSystemRMStateStore.RenameFile(nodeCreatePath, new Path(nodeCreatePath.GetParent
				(), nodeCreatePath.GetName() + ".new"));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFSRMStateStoreClientRetry()
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			cluster.WaitActive();
			try
			{
				TestFSRMStateStore.TestFSRMStateStoreTester fsTester = new TestFSRMStateStore.TestFSRMStateStoreTester
					(this, cluster, false);
				RMStateStore store = fsTester.GetRMStateStore();
				store.SetRMDispatcher(new RMStateStoreTestBase.TestDispatcher());
				AtomicBoolean assertionFailedInThread = new AtomicBoolean(false);
				cluster.ShutdownNameNodes();
				Sharpen.Thread clientThread = new _Thread_381(store, assertionFailedInThread);
				Sharpen.Thread.Sleep(2000);
				clientThread.Start();
				cluster.RestartNameNode();
				clientThread.Join();
				NUnit.Framework.Assert.IsFalse(assertionFailedInThread.Get());
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		private sealed class _Thread_381 : Sharpen.Thread
		{
			public _Thread_381(RMStateStore store, AtomicBoolean assertionFailedInThread)
			{
				this.store = store;
				this.assertionFailedInThread = assertionFailedInThread;
			}

			public override void Run()
			{
				try
				{
					store.StoreApplicationStateInternal(ApplicationId.NewInstance(100L, 1), ApplicationStateData
						.NewInstance(111, 111, "user", null, RMAppState.Accepted, "diagnostics", 333));
				}
				catch (Exception e)
				{
					assertionFailedInThread.Set(true);
					Sharpen.Runtime.PrintStackTrace(e);
				}
			}

			private readonly RMStateStore store;

			private readonly AtomicBoolean assertionFailedInThread;
		}
	}
}
