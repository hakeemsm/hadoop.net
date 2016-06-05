using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Util;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>A collection of block storage policies.</summary>
	public class BlockStoragePolicySuite
	{
		internal static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockStoragePolicySuite
			));

		public const string StoragePolicyXattrName = "hsm.block.storage.policy.id";

		public static readonly XAttr.NameSpace XAttrNS = XAttr.NameSpace.System;

		public const int IdBitLength = 4;

		public const byte IdUnspecified = 0;

		[VisibleForTesting]
		public static Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockStoragePolicySuite
			 CreateDefaultSuite()
		{
			BlockStoragePolicy[] policies = new BlockStoragePolicy[1 << IdBitLength];
			byte lazyPersistId = HdfsConstants.MemoryStoragePolicyId;
			policies[lazyPersistId] = new BlockStoragePolicy(lazyPersistId, HdfsConstants.MemoryStoragePolicyName
				, new StorageType[] { StorageType.RamDisk, StorageType.Disk }, new StorageType[]
				 { StorageType.Disk }, new StorageType[] { StorageType.Disk }, true);
			// Cannot be changed on regular files, but inherited.
			byte allssdId = HdfsConstants.AllssdStoragePolicyId;
			policies[allssdId] = new BlockStoragePolicy(allssdId, HdfsConstants.AllssdStoragePolicyName
				, new StorageType[] { StorageType.Ssd }, new StorageType[] { StorageType.Disk }, 
				new StorageType[] { StorageType.Disk });
			byte onessdId = HdfsConstants.OnessdStoragePolicyId;
			policies[onessdId] = new BlockStoragePolicy(onessdId, HdfsConstants.OnessdStoragePolicyName
				, new StorageType[] { StorageType.Ssd, StorageType.Disk }, new StorageType[] { StorageType
				.Ssd, StorageType.Disk }, new StorageType[] { StorageType.Ssd, StorageType.Disk }
				);
			byte hotId = HdfsConstants.HotStoragePolicyId;
			policies[hotId] = new BlockStoragePolicy(hotId, HdfsConstants.HotStoragePolicyName
				, new StorageType[] { StorageType.Disk }, StorageType.EmptyArray, new StorageType
				[] { StorageType.Archive });
			byte warmId = HdfsConstants.WarmStoragePolicyId;
			policies[warmId] = new BlockStoragePolicy(warmId, HdfsConstants.WarmStoragePolicyName
				, new StorageType[] { StorageType.Disk, StorageType.Archive }, new StorageType[]
				 { StorageType.Disk, StorageType.Archive }, new StorageType[] { StorageType.Disk
				, StorageType.Archive });
			byte coldId = HdfsConstants.ColdStoragePolicyId;
			policies[coldId] = new BlockStoragePolicy(coldId, HdfsConstants.ColdStoragePolicyName
				, new StorageType[] { StorageType.Archive }, StorageType.EmptyArray, StorageType
				.EmptyArray);
			return new Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockStoragePolicySuite(
				hotId, policies);
		}

		private readonly byte defaultPolicyID;

		private readonly BlockStoragePolicy[] policies;

		public BlockStoragePolicySuite(byte defaultPolicyID, BlockStoragePolicy[] policies
			)
		{
			this.defaultPolicyID = defaultPolicyID;
			this.policies = policies;
		}

		/// <returns>the corresponding policy.</returns>
		public virtual BlockStoragePolicy GetPolicy(byte id)
		{
			// id == 0 means policy not specified.
			return id == 0 ? GetDefaultPolicy() : policies[id];
		}

		/// <returns>the default policy.</returns>
		public virtual BlockStoragePolicy GetDefaultPolicy()
		{
			return GetPolicy(defaultPolicyID);
		}

		public virtual BlockStoragePolicy GetPolicy(string policyName)
		{
			Preconditions.CheckNotNull(policyName);
			if (policies != null)
			{
				foreach (BlockStoragePolicy policy in policies)
				{
					if (policy != null && Sharpen.Runtime.EqualsIgnoreCase(policy.GetName(), policyName
						))
					{
						return policy;
					}
				}
			}
			return null;
		}

		public virtual BlockStoragePolicy[] GetAllPolicies()
		{
			IList<BlockStoragePolicy> list = Lists.NewArrayList();
			if (policies != null)
			{
				foreach (BlockStoragePolicy policy in policies)
				{
					if (policy != null)
					{
						list.AddItem(policy);
					}
				}
			}
			return Sharpen.Collections.ToArray(list, new BlockStoragePolicy[list.Count]);
		}

		public static string BuildXAttrName()
		{
			return StringUtils.ToLowerCase(XAttrNS.ToString()) + "." + StoragePolicyXattrName;
		}

		public static XAttr BuildXAttr(byte policyId)
		{
			string name = BuildXAttrName();
			return XAttrHelper.BuildXAttr(name, new byte[] { policyId });
		}

		public static bool IsStoragePolicyXAttr(XAttr xattr)
		{
			return xattr != null && xattr.GetNameSpace() == XAttrNS && xattr.GetName().Equals
				(StoragePolicyXattrName);
		}
	}
}
