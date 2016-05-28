using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// Test
	/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.BlockStoragePolicy"/>
	/// 
	/// </summary>
	public class TestBlockStoragePolicy
	{
		public static readonly BlockStoragePolicySuite PolicySuite;

		public static readonly BlockStoragePolicy DefaultStoragePolicy;

		public static readonly Configuration conf;

		static TestBlockStoragePolicy()
		{
			conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			conf.SetInt(DFSConfigKeys.DfsNamenodeReplicationIntervalKey, 1);
			PolicySuite = BlockStoragePolicySuite.CreateDefaultSuite();
			DefaultStoragePolicy = PolicySuite.GetDefaultPolicy();
		}

		internal static readonly EnumSet<StorageType> none = EnumSet.NoneOf<StorageType>(
			);

		internal static readonly EnumSet<StorageType> archive = EnumSet.Of(StorageType.Archive
			);

		internal static readonly EnumSet<StorageType> disk = EnumSet.Of(StorageType.Disk);

		internal static readonly EnumSet<StorageType> both = EnumSet.Of(StorageType.Disk, 
			StorageType.Archive);

		internal const long FileLen = 1024;

		internal const short Replication = 3;

		internal const byte Cold = HdfsConstants.ColdStoragePolicyId;

		internal const byte Warm = HdfsConstants.WarmStoragePolicyId;

		internal const byte Hot = HdfsConstants.HotStoragePolicyId;

		internal const byte Onessd = HdfsConstants.OnessdStoragePolicyId;

		internal const byte Allssd = HdfsConstants.AllssdStoragePolicyId;

		internal const byte LazyPersist = HdfsConstants.MemoryStoragePolicyId;

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestConfigKeyEnabled()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetBoolean(DFSConfigKeys.DfsStoragePolicyEnabledKey, true);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			try
			{
				cluster.WaitActive();
				cluster.GetFileSystem().SetStoragePolicy(new Path("/"), HdfsConstants.ColdStoragePolicyName
					);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Ensure that setStoragePolicy throws IOException when
		/// dfs.storage.policy.enabled is set to false.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestConfigKeyDisabled()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetBoolean(DFSConfigKeys.DfsStoragePolicyEnabledKey, false);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			try
			{
				cluster.WaitActive();
				cluster.GetFileSystem().SetStoragePolicy(new Path("/"), HdfsConstants.ColdStoragePolicyName
					);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestDefaultPolicies()
		{
			IDictionary<byte, string> expectedPolicyStrings = new Dictionary<byte, string>();
			expectedPolicyStrings[Cold] = "BlockStoragePolicy{COLD:" + Cold + ", storageTypes=[ARCHIVE], "
				 + "creationFallbacks=[], replicationFallbacks=[]}";
			expectedPolicyStrings[Warm] = "BlockStoragePolicy{WARM:" + Warm + ", storageTypes=[DISK, ARCHIVE], "
				 + "creationFallbacks=[DISK, ARCHIVE], " + "replicationFallbacks=[DISK, ARCHIVE]}";
			expectedPolicyStrings[Hot] = "BlockStoragePolicy{HOT:" + Hot + ", storageTypes=[DISK], "
				 + "creationFallbacks=[], replicationFallbacks=[ARCHIVE]}";
			expectedPolicyStrings[Onessd] = "BlockStoragePolicy{ONE_SSD:" + Onessd + ", storageTypes=[SSD, DISK], creationFallbacks=[SSD, DISK], "
				 + "replicationFallbacks=[SSD, DISK]}";
			expectedPolicyStrings[Allssd] = "BlockStoragePolicy{ALL_SSD:" + Allssd + ", storageTypes=[SSD], creationFallbacks=[DISK], "
				 + "replicationFallbacks=[DISK]}";
			expectedPolicyStrings[LazyPersist] = "BlockStoragePolicy{LAZY_PERSIST:" + LazyPersist
				 + ", storageTypes=[RAM_DISK, DISK], " + "creationFallbacks=[DISK], replicationFallbacks=[DISK]}";
			for (byte i = 1; ((sbyte)i) < 16; i++)
			{
				BlockStoragePolicy policy = PolicySuite.GetPolicy(i);
				if (policy != null)
				{
					string s = policy.ToString();
					NUnit.Framework.Assert.AreEqual(expectedPolicyStrings[i], s);
				}
			}
			NUnit.Framework.Assert.AreEqual(PolicySuite.GetPolicy(Hot), PolicySuite.GetDefaultPolicy
				());
			{
				// check Cold policy
				BlockStoragePolicy cold = PolicySuite.GetPolicy(Cold);
				for (short replication = 1; replication < 6; replication++)
				{
					IList<StorageType> computed = cold.ChooseStorageTypes(replication);
					AssertStorageType(computed, replication, StorageType.Archive);
				}
				AssertCreationFallback(cold, null, null, null);
				AssertReplicationFallback(cold, null, null, null);
			}
			{
				// check Warm policy
				BlockStoragePolicy warm = PolicySuite.GetPolicy(Warm);
				for (short replication = 1; replication < 6; replication++)
				{
					IList<StorageType> computed = warm.ChooseStorageTypes(replication);
					AssertStorageType(computed, replication, StorageType.Disk, StorageType.Archive);
				}
				AssertCreationFallback(warm, StorageType.Disk, StorageType.Disk, StorageType.Archive
					);
				AssertReplicationFallback(warm, StorageType.Disk, StorageType.Disk, StorageType.Archive
					);
			}
			{
				// check Hot policy
				BlockStoragePolicy hot = PolicySuite.GetPolicy(Hot);
				for (short replication = 1; replication < 6; replication++)
				{
					IList<StorageType> computed = hot.ChooseStorageTypes(replication);
					AssertStorageType(computed, replication, StorageType.Disk);
				}
				AssertCreationFallback(hot, null, null, null);
				AssertReplicationFallback(hot, StorageType.Archive, null, StorageType.Archive);
			}
		}

		internal static StorageType[] NewStorageTypes(int nDisk, int nArchive)
		{
			StorageType[] t = new StorageType[nDisk + nArchive];
			Arrays.Fill(t, 0, nDisk, StorageType.Disk);
			Arrays.Fill(t, nDisk, t.Length, StorageType.Archive);
			return t;
		}

		internal static IList<StorageType> AsList(int nDisk, int nArchive)
		{
			return Arrays.AsList(NewStorageTypes(nDisk, nArchive));
		}

		internal static void AssertStorageType(IList<StorageType> computed, short replication
			, params StorageType[] answers)
		{
			NUnit.Framework.Assert.AreEqual(replication, computed.Count);
			StorageType last = answers[answers.Length - 1];
			for (int i = 0; i < computed.Count; i++)
			{
				StorageType expected = i < answers.Length ? answers[i] : last;
				NUnit.Framework.Assert.AreEqual(expected, computed[i]);
			}
		}

		internal static void AssertCreationFallback(BlockStoragePolicy policy, StorageType
			 noneExpected, StorageType archiveExpected, StorageType diskExpected)
		{
			NUnit.Framework.Assert.AreEqual(noneExpected, policy.GetCreationFallback(none));
			NUnit.Framework.Assert.AreEqual(archiveExpected, policy.GetCreationFallback(archive
				));
			NUnit.Framework.Assert.AreEqual(diskExpected, policy.GetCreationFallback(disk));
			NUnit.Framework.Assert.AreEqual(null, policy.GetCreationFallback(both));
		}

		internal static void AssertReplicationFallback(BlockStoragePolicy policy, StorageType
			 noneExpected, StorageType archiveExpected, StorageType diskExpected)
		{
			NUnit.Framework.Assert.AreEqual(noneExpected, policy.GetReplicationFallback(none)
				);
			NUnit.Framework.Assert.AreEqual(archiveExpected, policy.GetReplicationFallback(archive
				));
			NUnit.Framework.Assert.AreEqual(diskExpected, policy.GetReplicationFallback(disk)
				);
			NUnit.Framework.Assert.AreEqual(null, policy.GetReplicationFallback(both));
		}

		private abstract class CheckChooseStorageTypes
		{
			public abstract void CheckChooseStorageTypes(BlockStoragePolicy p, short replication
				, IList<StorageType> chosen, params StorageType[] expected);

			private sealed class _CheckChooseStorageTypes_219 : TestBlockStoragePolicy.CheckChooseStorageTypes
			{
				public _CheckChooseStorageTypes_219()
				{
				}

				public override void CheckChooseStorageTypes(BlockStoragePolicy p, short replication
					, IList<StorageType> chosen, params StorageType[] expected)
				{
					IList<StorageType> types = p.ChooseStorageTypes(replication, chosen);
					TestBlockStoragePolicy.AssertStorageTypes(types, expected);
				}
			}

			/// <summary>Basic case: pass only replication and chosen</summary>
			public const TestBlockStoragePolicy.CheckChooseStorageTypes Basic = new _CheckChooseStorageTypes_219
				();

			private sealed class _CheckChooseStorageTypes_230 : TestBlockStoragePolicy.CheckChooseStorageTypes
			{
				public _CheckChooseStorageTypes_230()
				{
				}

				public override void CheckChooseStorageTypes(BlockStoragePolicy p, short replication
					, IList<StorageType> chosen, params StorageType[] expected)
				{
					IList<StorageType> types = p.ChooseStorageTypes(replication, chosen, TestBlockStoragePolicy
						.none, true);
					TestBlockStoragePolicy.AssertStorageTypes(types, expected);
				}
			}

			/// <summary>With empty unavailables and isNewBlock=true</summary>
			public const TestBlockStoragePolicy.CheckChooseStorageTypes EmptyUnavailablesAndNewBlock
				 = new _CheckChooseStorageTypes_230();

			private sealed class _CheckChooseStorageTypes_242 : TestBlockStoragePolicy.CheckChooseStorageTypes
			{
				public _CheckChooseStorageTypes_242()
				{
				}

				public override void CheckChooseStorageTypes(BlockStoragePolicy p, short replication
					, IList<StorageType> chosen, params StorageType[] expected)
				{
					IList<StorageType> types = p.ChooseStorageTypes(replication, chosen, TestBlockStoragePolicy
						.none, false);
					TestBlockStoragePolicy.AssertStorageTypes(types, expected);
				}
			}

			/// <summary>With empty unavailables and isNewBlock=false</summary>
			public const TestBlockStoragePolicy.CheckChooseStorageTypes EmptyUnavailablesAndNonNewBlock
				 = new _CheckChooseStorageTypes_242();

			private sealed class _CheckChooseStorageTypes_254 : TestBlockStoragePolicy.CheckChooseStorageTypes
			{
				public _CheckChooseStorageTypes_254()
				{
				}

				public override void CheckChooseStorageTypes(BlockStoragePolicy p, short replication
					, IList<StorageType> chosen, params StorageType[] expected)
				{
					IList<StorageType> types = p.ChooseStorageTypes(replication, chosen, TestBlockStoragePolicy
						.both, true);
					TestBlockStoragePolicy.AssertStorageTypes(types, expected);
				}
			}

			/// <summary>With both DISK and ARCHIVE unavailables and isNewBlock=true</summary>
			public const TestBlockStoragePolicy.CheckChooseStorageTypes BothUnavailableAndNewBlock
				 = new _CheckChooseStorageTypes_254();

			private sealed class _CheckChooseStorageTypes_266 : TestBlockStoragePolicy.CheckChooseStorageTypes
			{
				public _CheckChooseStorageTypes_266()
				{
				}

				public override void CheckChooseStorageTypes(BlockStoragePolicy p, short replication
					, IList<StorageType> chosen, params StorageType[] expected)
				{
					IList<StorageType> types = p.ChooseStorageTypes(replication, chosen, TestBlockStoragePolicy
						.both, false);
					TestBlockStoragePolicy.AssertStorageTypes(types, expected);
				}
			}

			/// <summary>With both DISK and ARCHIVE unavailable and isNewBlock=false</summary>
			public const TestBlockStoragePolicy.CheckChooseStorageTypes BothUnavailableAndNonNewBlock
				 = new _CheckChooseStorageTypes_266();

			private sealed class _CheckChooseStorageTypes_278 : TestBlockStoragePolicy.CheckChooseStorageTypes
			{
				public _CheckChooseStorageTypes_278()
				{
				}

				public override void CheckChooseStorageTypes(BlockStoragePolicy p, short replication
					, IList<StorageType> chosen, params StorageType[] expected)
				{
					IList<StorageType> types = p.ChooseStorageTypes(replication, chosen, TestBlockStoragePolicy
						.archive, true);
					TestBlockStoragePolicy.AssertStorageTypes(types, expected);
				}
			}

			/// <summary>With ARCHIVE unavailable and isNewBlock=true</summary>
			public const TestBlockStoragePolicy.CheckChooseStorageTypes ArchivalUnavailableAndNewBlock
				 = new _CheckChooseStorageTypes_278();

			private sealed class _CheckChooseStorageTypes_290 : TestBlockStoragePolicy.CheckChooseStorageTypes
			{
				public _CheckChooseStorageTypes_290()
				{
				}

				public override void CheckChooseStorageTypes(BlockStoragePolicy p, short replication
					, IList<StorageType> chosen, params StorageType[] expected)
				{
					IList<StorageType> types = p.ChooseStorageTypes(replication, chosen, TestBlockStoragePolicy
						.archive, false);
					TestBlockStoragePolicy.AssertStorageTypes(types, expected);
				}
			}

			/// <summary>With ARCHIVE unavailable and isNewBlock=true</summary>
			public const TestBlockStoragePolicy.CheckChooseStorageTypes ArchivalUnavailableAndNonNewBlock
				 = new _CheckChooseStorageTypes_290();
		}

		private static class CheckChooseStorageTypesConstants
		{
		}

		[NUnit.Framework.Test]
		public virtual void TestChooseStorageTypes()
		{
			Run(TestBlockStoragePolicy.CheckChooseStorageTypes.Basic);
			Run(TestBlockStoragePolicy.CheckChooseStorageTypes.EmptyUnavailablesAndNewBlock);
			Run(TestBlockStoragePolicy.CheckChooseStorageTypes.EmptyUnavailablesAndNonNewBlock
				);
		}

		private static void Run(TestBlockStoragePolicy.CheckChooseStorageTypes method)
		{
			BlockStoragePolicy hot = PolicySuite.GetPolicy(Hot);
			BlockStoragePolicy warm = PolicySuite.GetPolicy(Warm);
			BlockStoragePolicy cold = PolicySuite.GetPolicy(Cold);
			short replication = 3;
			{
				IList<StorageType> chosen = Lists.NewArrayList();
				method.CheckChooseStorageTypes(hot, replication, chosen, StorageType.Disk, StorageType
					.Disk, StorageType.Disk);
				method.CheckChooseStorageTypes(warm, replication, chosen, StorageType.Disk, StorageType
					.Archive, StorageType.Archive);
				method.CheckChooseStorageTypes(cold, replication, chosen, StorageType.Archive, StorageType
					.Archive, StorageType.Archive);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Disk);
				method.CheckChooseStorageTypes(hot, replication, chosen, StorageType.Disk, StorageType
					.Disk);
				method.CheckChooseStorageTypes(warm, replication, chosen, StorageType.Archive, StorageType
					.Archive);
				method.CheckChooseStorageTypes(cold, replication, chosen, StorageType.Archive, StorageType
					.Archive, StorageType.Archive);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Archive);
				method.CheckChooseStorageTypes(hot, replication, chosen, StorageType.Disk, StorageType
					.Disk, StorageType.Disk);
				method.CheckChooseStorageTypes(warm, replication, chosen, StorageType.Disk, StorageType
					.Archive);
				method.CheckChooseStorageTypes(cold, replication, chosen, StorageType.Archive, StorageType
					.Archive);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Disk, StorageType.Disk);
				method.CheckChooseStorageTypes(hot, replication, chosen, StorageType.Disk);
				method.CheckChooseStorageTypes(warm, replication, chosen, StorageType.Archive, StorageType
					.Archive);
				method.CheckChooseStorageTypes(cold, replication, chosen, StorageType.Archive, StorageType
					.Archive, StorageType.Archive);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Disk, StorageType.Archive);
				method.CheckChooseStorageTypes(hot, replication, chosen, StorageType.Disk, StorageType
					.Disk);
				method.CheckChooseStorageTypes(warm, replication, chosen, StorageType.Archive);
				method.CheckChooseStorageTypes(cold, replication, chosen, StorageType.Archive, StorageType
					.Archive);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Archive, StorageType.Archive
					);
				method.CheckChooseStorageTypes(hot, replication, chosen, StorageType.Disk, StorageType
					.Disk, StorageType.Disk);
				method.CheckChooseStorageTypes(warm, replication, chosen, StorageType.Disk);
				method.CheckChooseStorageTypes(cold, replication, chosen, StorageType.Archive);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Disk, StorageType.Disk, StorageType
					.Disk);
				method.CheckChooseStorageTypes(hot, replication, chosen);
				method.CheckChooseStorageTypes(warm, replication, chosen, StorageType.Archive, StorageType
					.Archive);
				method.CheckChooseStorageTypes(cold, replication, chosen, StorageType.Archive, StorageType
					.Archive, StorageType.Archive);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Disk, StorageType.Disk, StorageType
					.Archive);
				method.CheckChooseStorageTypes(hot, replication, chosen, StorageType.Disk);
				method.CheckChooseStorageTypes(warm, replication, chosen, StorageType.Archive);
				method.CheckChooseStorageTypes(cold, replication, chosen, StorageType.Archive, StorageType
					.Archive);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Disk, StorageType.Archive, 
					StorageType.Archive);
				method.CheckChooseStorageTypes(hot, replication, chosen, StorageType.Disk, StorageType
					.Disk);
				method.CheckChooseStorageTypes(warm, replication, chosen);
				method.CheckChooseStorageTypes(cold, replication, chosen, StorageType.Archive);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Archive, StorageType.Archive
					, StorageType.Archive);
				method.CheckChooseStorageTypes(hot, replication, chosen, StorageType.Disk, StorageType
					.Disk, StorageType.Disk);
				method.CheckChooseStorageTypes(warm, replication, chosen, StorageType.Disk);
				method.CheckChooseStorageTypes(cold, replication, chosen);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestChooseStorageTypesWithBothUnavailable()
		{
			RunWithBothUnavailable(TestBlockStoragePolicy.CheckChooseStorageTypes.BothUnavailableAndNewBlock
				);
			RunWithBothUnavailable(TestBlockStoragePolicy.CheckChooseStorageTypes.BothUnavailableAndNonNewBlock
				);
		}

		private static void RunWithBothUnavailable(TestBlockStoragePolicy.CheckChooseStorageTypes
			 method)
		{
			BlockStoragePolicy hot = PolicySuite.GetPolicy(Hot);
			BlockStoragePolicy warm = PolicySuite.GetPolicy(Warm);
			BlockStoragePolicy cold = PolicySuite.GetPolicy(Cold);
			short replication = 3;
			for (int n = 0; n <= 3; n++)
			{
				for (int d = 0; d <= n; d++)
				{
					int a = n - d;
					IList<StorageType> chosen = AsList(d, a);
					method.CheckChooseStorageTypes(hot, replication, chosen);
					method.CheckChooseStorageTypes(warm, replication, chosen);
					method.CheckChooseStorageTypes(cold, replication, chosen);
				}
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestChooseStorageTypesWithDiskUnavailableAndNewBlock()
		{
			BlockStoragePolicy hot = PolicySuite.GetPolicy(Hot);
			BlockStoragePolicy warm = PolicySuite.GetPolicy(Warm);
			BlockStoragePolicy cold = PolicySuite.GetPolicy(Cold);
			short replication = 3;
			EnumSet<StorageType> unavailables = disk;
			bool isNewBlock = true;
			{
				IList<StorageType> chosen = Lists.NewArrayList();
				CheckChooseStorageTypes(hot, replication, chosen, unavailables, isNewBlock);
				CheckChooseStorageTypes(warm, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive, StorageType.Archive, StorageType.Archive);
				CheckChooseStorageTypes(cold, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive, StorageType.Archive, StorageType.Archive);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Disk);
				CheckChooseStorageTypes(hot, replication, chosen, unavailables, isNewBlock);
				CheckChooseStorageTypes(warm, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive, StorageType.Archive);
				CheckChooseStorageTypes(cold, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive, StorageType.Archive, StorageType.Archive);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Archive);
				CheckChooseStorageTypes(hot, replication, chosen, unavailables, isNewBlock);
				CheckChooseStorageTypes(warm, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive, StorageType.Archive);
				CheckChooseStorageTypes(cold, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive, StorageType.Archive);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Disk, StorageType.Disk);
				CheckChooseStorageTypes(hot, replication, chosen, unavailables, isNewBlock);
				CheckChooseStorageTypes(warm, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive, StorageType.Archive);
				CheckChooseStorageTypes(cold, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive, StorageType.Archive, StorageType.Archive);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Disk, StorageType.Archive);
				CheckChooseStorageTypes(hot, replication, chosen, unavailables, isNewBlock);
				CheckChooseStorageTypes(warm, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive);
				CheckChooseStorageTypes(cold, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive, StorageType.Archive);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Archive, StorageType.Archive
					);
				CheckChooseStorageTypes(hot, replication, chosen, unavailables, isNewBlock);
				CheckChooseStorageTypes(warm, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive);
				CheckChooseStorageTypes(cold, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Disk, StorageType.Disk, StorageType
					.Disk);
				CheckChooseStorageTypes(hot, replication, chosen, unavailables, isNewBlock);
				CheckChooseStorageTypes(warm, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive, StorageType.Archive);
				CheckChooseStorageTypes(cold, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive, StorageType.Archive, StorageType.Archive);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Disk, StorageType.Disk, StorageType
					.Archive);
				CheckChooseStorageTypes(hot, replication, chosen, unavailables, isNewBlock);
				CheckChooseStorageTypes(warm, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive);
				CheckChooseStorageTypes(cold, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive, StorageType.Archive);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Disk, StorageType.Archive, 
					StorageType.Archive);
				CheckChooseStorageTypes(hot, replication, chosen, unavailables, isNewBlock);
				CheckChooseStorageTypes(warm, replication, chosen, unavailables, isNewBlock);
				CheckChooseStorageTypes(cold, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Archive, StorageType.Archive
					, StorageType.Archive);
				CheckChooseStorageTypes(hot, replication, chosen, unavailables, isNewBlock);
				CheckChooseStorageTypes(warm, replication, chosen, unavailables, isNewBlock);
				CheckChooseStorageTypes(cold, replication, chosen, unavailables, isNewBlock);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestChooseStorageTypesWithArchiveUnavailable()
		{
			RunWithArchiveUnavailable(TestBlockStoragePolicy.CheckChooseStorageTypes.ArchivalUnavailableAndNewBlock
				);
			RunWithArchiveUnavailable(TestBlockStoragePolicy.CheckChooseStorageTypes.ArchivalUnavailableAndNonNewBlock
				);
		}

		private static void RunWithArchiveUnavailable(TestBlockStoragePolicy.CheckChooseStorageTypes
			 method)
		{
			BlockStoragePolicy hot = PolicySuite.GetPolicy(Hot);
			BlockStoragePolicy warm = PolicySuite.GetPolicy(Warm);
			BlockStoragePolicy cold = PolicySuite.GetPolicy(Cold);
			short replication = 3;
			{
				IList<StorageType> chosen = Lists.NewArrayList();
				method.CheckChooseStorageTypes(hot, replication, chosen, StorageType.Disk, StorageType
					.Disk, StorageType.Disk);
				method.CheckChooseStorageTypes(warm, replication, chosen, StorageType.Disk, StorageType
					.Disk, StorageType.Disk);
				method.CheckChooseStorageTypes(cold, replication, chosen);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Disk);
				method.CheckChooseStorageTypes(hot, replication, chosen, StorageType.Disk, StorageType
					.Disk);
				method.CheckChooseStorageTypes(warm, replication, chosen, StorageType.Disk, StorageType
					.Disk);
				method.CheckChooseStorageTypes(cold, replication, chosen);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Archive);
				method.CheckChooseStorageTypes(hot, replication, chosen, StorageType.Disk, StorageType
					.Disk, StorageType.Disk);
				method.CheckChooseStorageTypes(warm, replication, chosen, StorageType.Disk, StorageType
					.Disk);
				method.CheckChooseStorageTypes(cold, replication, chosen);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Disk, StorageType.Disk);
				method.CheckChooseStorageTypes(hot, replication, chosen, StorageType.Disk);
				method.CheckChooseStorageTypes(warm, replication, chosen, StorageType.Disk);
				method.CheckChooseStorageTypes(cold, replication, chosen);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Disk, StorageType.Archive);
				method.CheckChooseStorageTypes(hot, replication, chosen, StorageType.Disk, StorageType
					.Disk);
				method.CheckChooseStorageTypes(warm, replication, chosen, StorageType.Disk);
				method.CheckChooseStorageTypes(cold, replication, chosen);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Archive, StorageType.Archive
					);
				method.CheckChooseStorageTypes(hot, replication, chosen, StorageType.Disk, StorageType
					.Disk, StorageType.Disk);
				method.CheckChooseStorageTypes(warm, replication, chosen, StorageType.Disk);
				method.CheckChooseStorageTypes(cold, replication, chosen);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Disk, StorageType.Disk, StorageType
					.Disk);
				method.CheckChooseStorageTypes(hot, replication, chosen);
				method.CheckChooseStorageTypes(warm, replication, chosen);
				method.CheckChooseStorageTypes(cold, replication, chosen);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Disk, StorageType.Disk, StorageType
					.Archive);
				method.CheckChooseStorageTypes(hot, replication, chosen, StorageType.Disk);
				method.CheckChooseStorageTypes(warm, replication, chosen);
				method.CheckChooseStorageTypes(cold, replication, chosen);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Disk, StorageType.Archive, 
					StorageType.Archive);
				method.CheckChooseStorageTypes(hot, replication, chosen, StorageType.Disk, StorageType
					.Disk);
				method.CheckChooseStorageTypes(warm, replication, chosen);
				method.CheckChooseStorageTypes(cold, replication, chosen);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Archive, StorageType.Archive
					, StorageType.Archive);
				method.CheckChooseStorageTypes(hot, replication, chosen, StorageType.Disk, StorageType
					.Disk, StorageType.Disk);
				method.CheckChooseStorageTypes(warm, replication, chosen, StorageType.Disk);
				method.CheckChooseStorageTypes(cold, replication, chosen);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestChooseStorageTypesWithDiskUnavailableAndNonNewBlock()
		{
			BlockStoragePolicy hot = PolicySuite.GetPolicy(Hot);
			BlockStoragePolicy warm = PolicySuite.GetPolicy(Warm);
			BlockStoragePolicy cold = PolicySuite.GetPolicy(Cold);
			short replication = 3;
			EnumSet<StorageType> unavailables = disk;
			bool isNewBlock = false;
			{
				IList<StorageType> chosen = Lists.NewArrayList();
				CheckChooseStorageTypes(hot, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive, StorageType.Archive, StorageType.Archive);
				CheckChooseStorageTypes(warm, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive, StorageType.Archive, StorageType.Archive);
				CheckChooseStorageTypes(cold, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive, StorageType.Archive, StorageType.Archive);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Disk);
				CheckChooseStorageTypes(hot, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive, StorageType.Archive);
				CheckChooseStorageTypes(warm, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive, StorageType.Archive);
				CheckChooseStorageTypes(cold, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive, StorageType.Archive, StorageType.Archive);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Archive);
				CheckChooseStorageTypes(hot, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive, StorageType.Archive);
				CheckChooseStorageTypes(warm, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive, StorageType.Archive);
				CheckChooseStorageTypes(cold, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive, StorageType.Archive);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Disk, StorageType.Disk);
				CheckChooseStorageTypes(hot, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive);
				CheckChooseStorageTypes(warm, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive, StorageType.Archive);
				CheckChooseStorageTypes(cold, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive, StorageType.Archive, StorageType.Archive);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Disk, StorageType.Archive);
				CheckChooseStorageTypes(hot, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive);
				CheckChooseStorageTypes(warm, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive);
				CheckChooseStorageTypes(cold, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive, StorageType.Archive);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Archive, StorageType.Archive
					);
				CheckChooseStorageTypes(hot, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive);
				CheckChooseStorageTypes(warm, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive);
				CheckChooseStorageTypes(cold, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Disk, StorageType.Disk, StorageType
					.Disk);
				CheckChooseStorageTypes(hot, replication, chosen, unavailables, isNewBlock);
				CheckChooseStorageTypes(warm, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive, StorageType.Archive);
				CheckChooseStorageTypes(cold, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive, StorageType.Archive, StorageType.Archive);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Disk, StorageType.Disk, StorageType
					.Archive);
				CheckChooseStorageTypes(hot, replication, chosen, unavailables, isNewBlock);
				CheckChooseStorageTypes(warm, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive);
				CheckChooseStorageTypes(cold, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive, StorageType.Archive);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Disk, StorageType.Archive, 
					StorageType.Archive);
				CheckChooseStorageTypes(hot, replication, chosen, unavailables, isNewBlock);
				CheckChooseStorageTypes(warm, replication, chosen, unavailables, isNewBlock);
				CheckChooseStorageTypes(cold, replication, chosen, unavailables, isNewBlock, StorageType
					.Archive);
			}
			{
				IList<StorageType> chosen = Arrays.AsList(StorageType.Archive, StorageType.Archive
					, StorageType.Archive);
				CheckChooseStorageTypes(hot, replication, chosen, unavailables, isNewBlock);
				CheckChooseStorageTypes(warm, replication, chosen, unavailables, isNewBlock);
				CheckChooseStorageTypes(cold, replication, chosen, unavailables, isNewBlock);
			}
		}

		internal static void CheckChooseStorageTypes(BlockStoragePolicy p, short replication
			, IList<StorageType> chosen, EnumSet<StorageType> unavailables, bool isNewBlock, 
			params StorageType[] expected)
		{
			IList<StorageType> types = p.ChooseStorageTypes(replication, chosen, unavailables
				, isNewBlock);
			AssertStorageTypes(types, expected);
		}

		internal static void AssertStorageTypes(IList<StorageType> computed, params StorageType
			[] expected)
		{
			AssertStorageTypes(Sharpen.Collections.ToArray(computed, StorageType.EmptyArray), 
				expected);
		}

		internal static void AssertStorageTypes(StorageType[] computed, params StorageType
			[] expected)
		{
			Arrays.Sort(expected);
			Arrays.Sort(computed);
			Assert.AssertArrayEquals(expected, computed);
		}

		[NUnit.Framework.Test]
		public virtual void TestChooseExcess()
		{
			BlockStoragePolicy hot = PolicySuite.GetPolicy(Hot);
			BlockStoragePolicy warm = PolicySuite.GetPolicy(Warm);
			BlockStoragePolicy cold = PolicySuite.GetPolicy(Cold);
			short replication = 3;
			for (int n = 0; n <= 6; n++)
			{
				for (int d = 0; d <= n; d++)
				{
					int a = n - d;
					IList<StorageType> chosen = AsList(d, a);
					{
						int nDisk = Math.Max(0, d - replication);
						int nArchive = a;
						StorageType[] expected = NewStorageTypes(nDisk, nArchive);
						CheckChooseExcess(hot, replication, chosen, expected);
					}
					{
						int nDisk = Math.Max(0, d - 1);
						int nArchive = Math.Max(0, a - replication + 1);
						StorageType[] expected = NewStorageTypes(nDisk, nArchive);
						CheckChooseExcess(warm, replication, chosen, expected);
					}
					{
						int nDisk = d;
						int nArchive = Math.Max(0, a - replication);
						StorageType[] expected = NewStorageTypes(nDisk, nArchive);
						CheckChooseExcess(cold, replication, chosen, expected);
					}
				}
			}
		}

		internal static void CheckChooseExcess(BlockStoragePolicy p, short replication, IList
			<StorageType> chosen, params StorageType[] expected)
		{
			IList<StorageType> types = p.ChooseExcess(replication, chosen);
			AssertStorageTypes(types, expected);
		}

		private void CheckDirectoryListing(HdfsFileStatus[] stats, params byte[] policies
			)
		{
			NUnit.Framework.Assert.AreEqual(stats.Length, policies.Length);
			for (int i = 0; i < stats.Length; i++)
			{
				NUnit.Framework.Assert.AreEqual(stats[i].GetStoragePolicy(), policies[i]);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSetStoragePolicy()
		{
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(Replication
				).Build();
			cluster.WaitActive();
			DistributedFileSystem fs = cluster.GetFileSystem();
			try
			{
				Path dir = new Path("/testSetStoragePolicy");
				Path fooFile = new Path(dir, "foo");
				Path barDir = new Path(dir, "bar");
				Path barFile1 = new Path(barDir, "f1");
				Path barFile2 = new Path(barDir, "f2");
				DFSTestUtil.CreateFile(fs, fooFile, FileLen, Replication, 0L);
				DFSTestUtil.CreateFile(fs, barFile1, FileLen, Replication, 0L);
				DFSTestUtil.CreateFile(fs, barFile2, FileLen, Replication, 0L);
				string invalidPolicyName = "INVALID-POLICY";
				try
				{
					fs.SetStoragePolicy(fooFile, invalidPolicyName);
					NUnit.Framework.Assert.Fail("Should throw a HadoopIllegalArgumentException");
				}
				catch (RemoteException e)
				{
					GenericTestUtils.AssertExceptionContains(invalidPolicyName, e);
				}
				// check storage policy
				HdfsFileStatus[] dirList = fs.GetClient().ListPaths(dir.ToString(), HdfsFileStatus
					.EmptyName, true).GetPartialListing();
				HdfsFileStatus[] barList = fs.GetClient().ListPaths(barDir.ToString(), HdfsFileStatus
					.EmptyName, true).GetPartialListing();
				CheckDirectoryListing(dirList, BlockStoragePolicySuite.IdUnspecified, BlockStoragePolicySuite
					.IdUnspecified);
				CheckDirectoryListing(barList, BlockStoragePolicySuite.IdUnspecified, BlockStoragePolicySuite
					.IdUnspecified);
				Path invalidPath = new Path("/invalidPath");
				try
				{
					fs.SetStoragePolicy(invalidPath, HdfsConstants.WarmStoragePolicyName);
					NUnit.Framework.Assert.Fail("Should throw a FileNotFoundException");
				}
				catch (FileNotFoundException e)
				{
					GenericTestUtils.AssertExceptionContains(invalidPath.ToString(), e);
				}
				fs.SetStoragePolicy(fooFile, HdfsConstants.ColdStoragePolicyName);
				fs.SetStoragePolicy(barDir, HdfsConstants.WarmStoragePolicyName);
				fs.SetStoragePolicy(barFile2, HdfsConstants.HotStoragePolicyName);
				dirList = fs.GetClient().ListPaths(dir.ToString(), HdfsFileStatus.EmptyName).GetPartialListing
					();
				barList = fs.GetClient().ListPaths(barDir.ToString(), HdfsFileStatus.EmptyName).GetPartialListing
					();
				CheckDirectoryListing(dirList, Warm, Cold);
				// bar is warm, foo is cold
				CheckDirectoryListing(barList, Warm, Hot);
				// restart namenode to make sure the editlog is correct
				cluster.RestartNameNode(true);
				dirList = fs.GetClient().ListPaths(dir.ToString(), HdfsFileStatus.EmptyName, true
					).GetPartialListing();
				barList = fs.GetClient().ListPaths(barDir.ToString(), HdfsFileStatus.EmptyName, true
					).GetPartialListing();
				CheckDirectoryListing(dirList, Warm, Cold);
				// bar is warm, foo is cold
				CheckDirectoryListing(barList, Warm, Hot);
				// restart namenode with checkpoint to make sure the fsimage is correct
				fs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
				fs.SaveNamespace();
				fs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave);
				cluster.RestartNameNode(true);
				dirList = fs.GetClient().ListPaths(dir.ToString(), HdfsFileStatus.EmptyName).GetPartialListing
					();
				barList = fs.GetClient().ListPaths(barDir.ToString(), HdfsFileStatus.EmptyName).GetPartialListing
					();
				CheckDirectoryListing(dirList, Warm, Cold);
				// bar is warm, foo is cold
				CheckDirectoryListing(barList, Warm, Hot);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSetStoragePolicyWithSnapshot()
		{
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(Replication
				).Build();
			cluster.WaitActive();
			DistributedFileSystem fs = cluster.GetFileSystem();
			try
			{
				Path dir = new Path("/testSetStoragePolicyWithSnapshot");
				Path fooDir = new Path(dir, "foo");
				Path fooFile1 = new Path(fooDir, "f1");
				Path fooFile2 = new Path(fooDir, "f2");
				DFSTestUtil.CreateFile(fs, fooFile1, FileLen, Replication, 0L);
				DFSTestUtil.CreateFile(fs, fooFile2, FileLen, Replication, 0L);
				fs.SetStoragePolicy(fooDir, HdfsConstants.WarmStoragePolicyName);
				HdfsFileStatus[] dirList = fs.GetClient().ListPaths(dir.ToString(), HdfsFileStatus
					.EmptyName, true).GetPartialListing();
				CheckDirectoryListing(dirList, Warm);
				HdfsFileStatus[] fooList = fs.GetClient().ListPaths(fooDir.ToString(), HdfsFileStatus
					.EmptyName, true).GetPartialListing();
				CheckDirectoryListing(fooList, Warm, Warm);
				// take snapshot
				SnapshotTestHelper.CreateSnapshot(fs, dir, "s1");
				// change the storage policy of fooFile1
				fs.SetStoragePolicy(fooFile1, HdfsConstants.ColdStoragePolicyName);
				fooList = fs.GetClient().ListPaths(fooDir.ToString(), HdfsFileStatus.EmptyName).GetPartialListing
					();
				CheckDirectoryListing(fooList, Cold, Warm);
				// check the policy for /dir/.snapshot/s1/foo/f1. Note we always return
				// the latest storage policy for a file/directory.
				Path s1f1 = SnapshotTestHelper.GetSnapshotPath(dir, "s1", "foo/f1");
				DirectoryListing f1Listing = fs.GetClient().ListPaths(s1f1.ToString(), HdfsFileStatus
					.EmptyName);
				CheckDirectoryListing(f1Listing.GetPartialListing(), Cold);
				// delete f1
				fs.Delete(fooFile1, true);
				fooList = fs.GetClient().ListPaths(fooDir.ToString(), HdfsFileStatus.EmptyName).GetPartialListing
					();
				CheckDirectoryListing(fooList, Warm);
				// check the policy for /dir/.snapshot/s1/foo/f1 again after the deletion
				CheckDirectoryListing(fs.GetClient().ListPaths(s1f1.ToString(), HdfsFileStatus.EmptyName
					).GetPartialListing(), Cold);
				// change the storage policy of foo dir
				fs.SetStoragePolicy(fooDir, HdfsConstants.HotStoragePolicyName);
				// /dir/foo is now hot
				dirList = fs.GetClient().ListPaths(dir.ToString(), HdfsFileStatus.EmptyName, true
					).GetPartialListing();
				CheckDirectoryListing(dirList, Hot);
				// /dir/foo/f2 is hot
				fooList = fs.GetClient().ListPaths(fooDir.ToString(), HdfsFileStatus.EmptyName).GetPartialListing
					();
				CheckDirectoryListing(fooList, Hot);
				// check storage policy of snapshot path
				Path s1 = SnapshotTestHelper.GetSnapshotRoot(dir, "s1");
				Path s1foo = SnapshotTestHelper.GetSnapshotPath(dir, "s1", "foo");
				CheckDirectoryListing(fs.GetClient().ListPaths(s1.ToString(), HdfsFileStatus.EmptyName
					).GetPartialListing(), Hot);
				// /dir/.snapshot/.s1/foo/f1 and /dir/.snapshot/.s1/foo/f2 should still
				// follow the latest
				CheckDirectoryListing(fs.GetClient().ListPaths(s1foo.ToString(), HdfsFileStatus.EmptyName
					).GetPartialListing(), Cold, Hot);
				// delete foo
				fs.Delete(fooDir, true);
				CheckDirectoryListing(fs.GetClient().ListPaths(s1.ToString(), HdfsFileStatus.EmptyName
					).GetPartialListing(), Hot);
				CheckDirectoryListing(fs.GetClient().ListPaths(s1foo.ToString(), HdfsFileStatus.EmptyName
					).GetPartialListing(), Cold, Hot);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		private static StorageType[][] GenStorageTypes(int numDataNodes)
		{
			StorageType[][] types = new StorageType[numDataNodes][];
			for (int i = 0; i < types.Length; i++)
			{
				types[i] = new StorageType[] { StorageType.Disk, StorageType.Archive };
			}
			return types;
		}

		private void CheckLocatedBlocks(HdfsLocatedFileStatus status, int blockNum, int replicaNum
			, params StorageType[] types)
		{
			IList<StorageType> typeList = Lists.NewArrayList();
			Sharpen.Collections.AddAll(typeList, types);
			LocatedBlocks lbs = status.GetBlockLocations();
			NUnit.Framework.Assert.AreEqual(blockNum, lbs.GetLocatedBlocks().Count);
			foreach (LocatedBlock lb in lbs.GetLocatedBlocks())
			{
				NUnit.Framework.Assert.AreEqual(replicaNum, lb.GetStorageTypes().Length);
				foreach (StorageType type in lb.GetStorageTypes())
				{
					NUnit.Framework.Assert.IsTrue(typeList.Remove(type));
				}
			}
			NUnit.Framework.Assert.IsTrue(typeList.IsEmpty());
		}

		/// <exception cref="System.Exception"/>
		private void TestChangeFileRep(string policyName, byte policyId, StorageType[] before
			, StorageType[] after)
		{
			int numDataNodes = 5;
			StorageType[][] types = GenStorageTypes(numDataNodes);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDataNodes
				).StorageTypes(types).Build();
			cluster.WaitActive();
			DistributedFileSystem fs = cluster.GetFileSystem();
			try
			{
				Path dir = new Path("/test");
				fs.Mkdirs(dir);
				fs.SetStoragePolicy(dir, policyName);
				Path foo = new Path(dir, "foo");
				DFSTestUtil.CreateFile(fs, foo, FileLen, Replication, 0L);
				HdfsFileStatus[] status = fs.GetClient().ListPaths(foo.ToString(), HdfsFileStatus
					.EmptyName, true).GetPartialListing();
				CheckDirectoryListing(status, policyId);
				HdfsLocatedFileStatus fooStatus = (HdfsLocatedFileStatus)status[0];
				CheckLocatedBlocks(fooStatus, 1, 3, before);
				// change the replication factor to 5
				fs.SetReplication(foo, (short)numDataNodes);
				Sharpen.Thread.Sleep(1000);
				foreach (DataNode dn in cluster.GetDataNodes())
				{
					DataNodeTestUtils.TriggerHeartbeat(dn);
				}
				Sharpen.Thread.Sleep(1000);
				status = fs.GetClient().ListPaths(foo.ToString(), HdfsFileStatus.EmptyName, true)
					.GetPartialListing();
				CheckDirectoryListing(status, policyId);
				fooStatus = (HdfsLocatedFileStatus)status[0];
				CheckLocatedBlocks(fooStatus, 1, numDataNodes, after);
				// change the replication factor back to 3
				fs.SetReplication(foo, Replication);
				Sharpen.Thread.Sleep(1000);
				foreach (DataNode dn_1 in cluster.GetDataNodes())
				{
					DataNodeTestUtils.TriggerHeartbeat(dn_1);
				}
				Sharpen.Thread.Sleep(1000);
				foreach (DataNode dn_2 in cluster.GetDataNodes())
				{
					DataNodeTestUtils.TriggerBlockReport(dn_2);
				}
				Sharpen.Thread.Sleep(1000);
				status = fs.GetClient().ListPaths(foo.ToString(), HdfsFileStatus.EmptyName, true)
					.GetPartialListing();
				CheckDirectoryListing(status, policyId);
				fooStatus = (HdfsLocatedFileStatus)status[0];
				CheckLocatedBlocks(fooStatus, 1, Replication, before);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Consider a File with Hot storage policy.</summary>
		/// <remarks>
		/// Consider a File with Hot storage policy. Increase replication factor of
		/// that file from 3 to 5. Make sure all replications are created in DISKS.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestChangeHotFileRep()
		{
			TestChangeFileRep(HdfsConstants.HotStoragePolicyName, Hot, new StorageType[] { StorageType
				.Disk, StorageType.Disk, StorageType.Disk }, new StorageType[] { StorageType.Disk
				, StorageType.Disk, StorageType.Disk, StorageType.Disk, StorageType.Disk });
		}

		/// <summary>Consider a File with Warm temperature.</summary>
		/// <remarks>
		/// Consider a File with Warm temperature. Increase replication factor of
		/// that file from 3 to 5. Make sure all replicas are created in DISKS
		/// and ARCHIVE.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestChangeWarmRep()
		{
			TestChangeFileRep(HdfsConstants.WarmStoragePolicyName, Warm, new StorageType[] { 
				StorageType.Disk, StorageType.Archive, StorageType.Archive }, new StorageType[] 
				{ StorageType.Disk, StorageType.Archive, StorageType.Archive, StorageType.Archive
				, StorageType.Archive });
		}

		/// <summary>Consider a File with Cold temperature.</summary>
		/// <remarks>
		/// Consider a File with Cold temperature. Increase replication factor of
		/// that file from 3 to 5. Make sure all replicas are created in ARCHIVE.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestChangeColdRep()
		{
			TestChangeFileRep(HdfsConstants.ColdStoragePolicyName, Cold, new StorageType[] { 
				StorageType.Archive, StorageType.Archive, StorageType.Archive }, new StorageType
				[] { StorageType.Archive, StorageType.Archive, StorageType.Archive, StorageType.
				Archive, StorageType.Archive });
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestChooseTargetWithTopology()
		{
			BlockStoragePolicy policy1 = new BlockStoragePolicy(unchecked((byte)9), "TEST1", 
				new StorageType[] { StorageType.Ssd, StorageType.Disk, StorageType.Archive }, new 
				StorageType[] {  }, new StorageType[] {  });
			BlockStoragePolicy policy2 = new BlockStoragePolicy(unchecked((byte)11), "TEST2", 
				new StorageType[] { StorageType.Disk, StorageType.Ssd, StorageType.Archive }, new 
				StorageType[] {  }, new StorageType[] {  });
			string[] racks = new string[] { "/d1/r1", "/d1/r2", "/d1/r2" };
			string[] hosts = new string[] { "host1", "host2", "host3" };
			StorageType[] types = new StorageType[] { StorageType.Disk, StorageType.Ssd, StorageType
				.Archive };
			DatanodeStorageInfo[] storages = DFSTestUtil.CreateDatanodeStorageInfos(3, racks, 
				hosts, types);
			DatanodeDescriptor[] dataNodes = DFSTestUtil.ToDatanodeDescriptor(storages);
			FileSystem.SetDefaultUri(conf, "hdfs://localhost:0");
			conf.Set(DFSConfigKeys.DfsNamenodeHttpAddressKey, "0.0.0.0:0");
			FilePath baseDir = PathUtils.GetTestDir(typeof(TestReplicationPolicy));
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, new FilePath(baseDir, "name").GetPath
				());
			DFSTestUtil.FormatNameNode(conf);
			NameNode namenode = new NameNode(conf);
			BlockManager bm = namenode.GetNamesystem().GetBlockManager();
			BlockPlacementPolicy replicator = bm.GetBlockPlacementPolicy();
			NetworkTopology cluster = bm.GetDatanodeManager().GetNetworkTopology();
			foreach (DatanodeDescriptor datanode in dataNodes)
			{
				cluster.Add(datanode);
			}
			DatanodeStorageInfo[] targets = replicator.ChooseTarget("/foo", 3, dataNodes[0], 
				Sharpen.Collections.EmptyList<DatanodeStorageInfo>(), false, new HashSet<Node>()
				, 0, policy1);
			System.Console.Out.WriteLine(Arrays.AsList(targets));
			NUnit.Framework.Assert.AreEqual(3, targets.Length);
			targets = replicator.ChooseTarget("/foo", 3, dataNodes[0], Sharpen.Collections.EmptyList
				<DatanodeStorageInfo>(), false, new HashSet<Node>(), 0, policy2);
			System.Console.Out.WriteLine(Arrays.AsList(targets));
			NUnit.Framework.Assert.AreEqual(3, targets.Length);
		}

		/// <summary>Test getting all the storage policies from the namenode</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetAllStoragePolicies()
		{
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
			cluster.WaitActive();
			DistributedFileSystem fs = cluster.GetFileSystem();
			try
			{
				BlockStoragePolicy[] policies = fs.GetStoragePolicies();
				NUnit.Framework.Assert.AreEqual(6, policies.Length);
				NUnit.Framework.Assert.AreEqual(PolicySuite.GetPolicy(Cold).ToString(), policies[
					0].ToString());
				NUnit.Framework.Assert.AreEqual(PolicySuite.GetPolicy(Warm).ToString(), policies[
					1].ToString());
				NUnit.Framework.Assert.AreEqual(PolicySuite.GetPolicy(Hot).ToString(), policies[2
					].ToString());
				NUnit.Framework.Assert.AreEqual(PolicySuite.GetPolicy(Onessd).ToString(), policies
					[3].ToString());
				NUnit.Framework.Assert.AreEqual(PolicySuite.GetPolicy(Allssd).ToString(), policies
					[4].ToString());
				NUnit.Framework.Assert.AreEqual(PolicySuite.GetPolicy(LazyPersist).ToString(), policies
					[5].ToString());
			}
			finally
			{
				IOUtils.Cleanup(null, fs);
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestChooseSsdOverDisk()
		{
			BlockStoragePolicy policy = new BlockStoragePolicy(unchecked((byte)9), "TEST1", new 
				StorageType[] { StorageType.Ssd, StorageType.Disk, StorageType.Archive }, new StorageType
				[] {  }, new StorageType[] {  });
			string[] racks = new string[] { "/d1/r1", "/d1/r1", "/d1/r1" };
			string[] hosts = new string[] { "host1", "host2", "host3" };
			StorageType[] disks = new StorageType[] { StorageType.Disk, StorageType.Disk, StorageType
				.Disk };
			DatanodeStorageInfo[] diskStorages = DFSTestUtil.CreateDatanodeStorageInfos(3, racks
				, hosts, disks);
			DatanodeDescriptor[] dataNodes = DFSTestUtil.ToDatanodeDescriptor(diskStorages);
			for (int i = 0; i < dataNodes.Length; i++)
			{
				BlockManagerTestUtil.UpdateStorage(dataNodes[i], new DatanodeStorage("ssd" + i, DatanodeStorage.State
					.Normal, StorageType.Ssd));
			}
			FileSystem.SetDefaultUri(conf, "hdfs://localhost:0");
			conf.Set(DFSConfigKeys.DfsNamenodeHttpAddressKey, "0.0.0.0:0");
			FilePath baseDir = PathUtils.GetTestDir(typeof(TestReplicationPolicy));
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, new FilePath(baseDir, "name").GetPath
				());
			DFSTestUtil.FormatNameNode(conf);
			NameNode namenode = new NameNode(conf);
			BlockManager bm = namenode.GetNamesystem().GetBlockManager();
			BlockPlacementPolicy replicator = bm.GetBlockPlacementPolicy();
			NetworkTopology cluster = bm.GetDatanodeManager().GetNetworkTopology();
			foreach (DatanodeDescriptor datanode in dataNodes)
			{
				cluster.Add(datanode);
			}
			DatanodeStorageInfo[] targets = replicator.ChooseTarget("/foo", 3, dataNodes[0], 
				Sharpen.Collections.EmptyList<DatanodeStorageInfo>(), false, new HashSet<Node>()
				, 0, policy);
			System.Console.Out.WriteLine(policy.GetName() + ": " + Arrays.AsList(targets));
			NUnit.Framework.Assert.AreEqual(2, targets.Length);
			NUnit.Framework.Assert.AreEqual(StorageType.Ssd, targets[0].GetStorageType());
			NUnit.Framework.Assert.AreEqual(StorageType.Disk, targets[1].GetStorageType());
		}

		[NUnit.Framework.Test]
		public virtual void TestStorageType()
		{
			EnumMap<StorageType, int> map = new EnumMap<StorageType, int>(typeof(StorageType)
				);
			//put storage type is reversed order
			map[StorageType.Archive] = 1;
			map[StorageType.Disk] = 1;
			map[StorageType.Ssd] = 1;
			map[StorageType.RamDisk] = 1;
			{
				IEnumerator<StorageType> i = map.Keys.GetEnumerator();
				NUnit.Framework.Assert.AreEqual(StorageType.RamDisk, i.Next());
				NUnit.Framework.Assert.AreEqual(StorageType.Ssd, i.Next());
				NUnit.Framework.Assert.AreEqual(StorageType.Disk, i.Next());
				NUnit.Framework.Assert.AreEqual(StorageType.Archive, i.Next());
			}
			{
				IEnumerator<KeyValuePair<StorageType, int>> i = map.GetEnumerator();
				NUnit.Framework.Assert.AreEqual(StorageType.RamDisk, i.Next().Key);
				NUnit.Framework.Assert.AreEqual(StorageType.Ssd, i.Next().Key);
				NUnit.Framework.Assert.AreEqual(StorageType.Disk, i.Next().Key);
				NUnit.Framework.Assert.AreEqual(StorageType.Archive, i.Next().Key);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetFileStoragePolicyAfterRestartNN()
		{
			//HDFS8219
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(Replication
				).StorageTypes(new StorageType[] { StorageType.Disk, StorageType.Archive }).Build
				();
			cluster.WaitActive();
			DistributedFileSystem fs = cluster.GetFileSystem();
			try
			{
				string file = "/testScheduleWithinSameNode/file";
				Path dir = new Path("/testScheduleWithinSameNode");
				fs.Mkdirs(dir);
				// 2. Set Dir policy
				fs.SetStoragePolicy(dir, "COLD");
				// 3. Create file
				FSDataOutputStream @out = fs.Create(new Path(file));
				@out.WriteChars("testScheduleWithinSameNode");
				@out.Close();
				// 4. Set Dir policy
				fs.SetStoragePolicy(dir, "HOT");
				HdfsFileStatus status = fs.GetClient().GetFileInfo(file);
				// 5. get file policy, it should be parent policy.
				NUnit.Framework.Assert.IsTrue("File storage policy should be HOT", status.GetStoragePolicy
					() == HdfsConstants.HotStoragePolicyId);
				// 6. restart NameNode for reloading edits logs.
				cluster.RestartNameNode(true);
				// 7. get file policy, it should be parent policy.
				status = fs.GetClient().GetFileInfo(file);
				NUnit.Framework.Assert.IsTrue("File storage policy should be HOT", status.GetStoragePolicy
					() == HdfsConstants.HotStoragePolicyId);
			}
			finally
			{
				cluster.Shutdown();
			}
		}
	}
}
