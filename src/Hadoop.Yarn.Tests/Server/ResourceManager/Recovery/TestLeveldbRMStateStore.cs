using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery
{
	public class TestLeveldbRMStateStore : RMStateStoreTestBase
	{
		private static readonly FilePath TestDir = new FilePath(Runtime.GetProperty("test.build.data"
			, Runtime.GetProperty("java.io.tmpdir")), typeof(TestLeveldbRMStateStore).FullName
			);

		private YarnConfiguration conf;

		private LeveldbRMStateStore stateStore = null;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			FileUtil.FullyDelete(TestDir);
			conf = new YarnConfiguration();
			conf.Set(YarnConfiguration.RmLeveldbStorePath, TestDir.ToString());
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void Cleanup()
		{
			if (stateStore != null)
			{
				stateStore.Close();
			}
			FileUtil.FullyDelete(TestDir);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestApps()
		{
			TestLeveldbRMStateStore.LeveldbStateStoreTester tester = new TestLeveldbRMStateStore.LeveldbStateStoreTester
				(this);
			TestRMAppStateStore(tester);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestClientTokens()
		{
			TestLeveldbRMStateStore.LeveldbStateStoreTester tester = new TestLeveldbRMStateStore.LeveldbStateStoreTester
				(this);
			TestRMDTSecretManagerStateStore(tester);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestVersion()
		{
			TestLeveldbRMStateStore.LeveldbStateStoreTester tester = new TestLeveldbRMStateStore.LeveldbStateStoreTester
				(this);
			TestCheckVersion(tester);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestEpoch()
		{
			TestLeveldbRMStateStore.LeveldbStateStoreTester tester = new TestLeveldbRMStateStore.LeveldbStateStoreTester
				(this);
			TestEpoch(tester);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAppDeletion()
		{
			TestLeveldbRMStateStore.LeveldbStateStoreTester tester = new TestLeveldbRMStateStore.LeveldbStateStoreTester
				(this);
			TestAppDeletion(tester);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDeleteStore()
		{
			TestLeveldbRMStateStore.LeveldbStateStoreTester tester = new TestLeveldbRMStateStore.LeveldbStateStoreTester
				(this);
			TestDeleteStore(tester);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAMTokens()
		{
			TestLeveldbRMStateStore.LeveldbStateStoreTester tester = new TestLeveldbRMStateStore.LeveldbStateStoreTester
				(this);
			TestAMRMTokenSecretManagerStateStore(tester);
		}

		internal class LeveldbStateStoreTester : RMStateStoreTestBase.RMStateStoreHelper
		{
			/// <exception cref="System.Exception"/>
			public virtual RMStateStore GetRMStateStore()
			{
				if (this._enclosing.stateStore != null)
				{
					this._enclosing.stateStore.Close();
				}
				this._enclosing.stateStore = new LeveldbRMStateStore();
				this._enclosing.stateStore.Init(this._enclosing.conf);
				this._enclosing.stateStore.Start();
				return this._enclosing.stateStore;
			}

			/// <exception cref="System.Exception"/>
			public virtual bool IsFinalStateValid()
			{
				// There should be 6 total entries:
				//   1 entry for version
				//   2 entries for app 0010 with one attempt
				//   3 entries for app 0001 with two attempts
				return this._enclosing.stateStore.GetNumEntriesInDatabase() == 6;
			}

			/// <exception cref="System.Exception"/>
			public virtual void WriteVersion(Version version)
			{
				this._enclosing.stateStore.DbStoreVersion(version);
			}

			/// <exception cref="System.Exception"/>
			public virtual Version GetCurrentVersion()
			{
				return this._enclosing.stateStore.GetCurrentVersion();
			}

			/// <exception cref="System.Exception"/>
			public virtual bool AppExists(RMApp app)
			{
				if (this._enclosing.stateStore.IsClosed())
				{
					this.GetRMStateStore();
				}
				return this._enclosing.stateStore.LoadRMAppState(app.GetApplicationId()) != null;
			}

			internal LeveldbStateStoreTester(TestLeveldbRMStateStore _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestLeveldbRMStateStore _enclosing;
		}
	}
}
