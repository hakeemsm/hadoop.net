using System.Text;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// Quota feature for
	/// <see cref="INodeDirectory"/>
	/// .
	/// </summary>
	public sealed class DirectoryWithQuotaFeature : INode.Feature
	{
		public const long DefaultNamespaceQuota = long.MaxValue;

		public const long DefaultStorageSpaceQuota = HdfsConstants.QuotaReset;

		private QuotaCounts quota;

		private QuotaCounts usage;

		public class Builder
		{
			private QuotaCounts quota;

			private QuotaCounts usage;

			public Builder()
			{
				this.quota = new QuotaCounts.Builder().NameSpace(DefaultNamespaceQuota).StorageSpace
					(DefaultStorageSpaceQuota).TypeSpaces(DefaultStorageSpaceQuota).Build();
				this.usage = new QuotaCounts.Builder().NameSpace(1).Build();
			}

			public virtual DirectoryWithQuotaFeature.Builder NameSpaceQuota(long nameSpaceQuota
				)
			{
				this.quota.SetNameSpace(nameSpaceQuota);
				return this;
			}

			public virtual DirectoryWithQuotaFeature.Builder StorageSpaceQuota(long spaceQuota
				)
			{
				this.quota.SetStorageSpace(spaceQuota);
				return this;
			}

			public virtual DirectoryWithQuotaFeature.Builder TypeQuotas(EnumCounters<StorageType
				> typeQuotas)
			{
				this.quota.SetTypeSpaces(typeQuotas);
				return this;
			}

			public virtual DirectoryWithQuotaFeature.Builder TypeQuota(StorageType type, long
				 quota)
			{
				this.quota.SetTypeSpace(type, quota);
				return this;
			}

			public virtual DirectoryWithQuotaFeature Build()
			{
				return new DirectoryWithQuotaFeature(this);
			}
		}

		private DirectoryWithQuotaFeature(DirectoryWithQuotaFeature.Builder builder)
		{
			this.quota = builder.quota;
			this.usage = builder.usage;
		}

		/// <returns>the quota set or -1 if it is not set.</returns>
		internal QuotaCounts GetQuota()
		{
			return new QuotaCounts.Builder().QuotaCount(this.quota).Build();
		}

		/// <summary>Set this directory's quota</summary>
		/// <param name="nsQuota">Namespace quota to be set</param>
		/// <param name="ssQuota">Storagespace quota to be set</param>
		/// <param name="type">
		/// Storage type of the storage space quota to be set.
		/// To set storagespace/namespace quota, type must be null.
		/// </param>
		internal void SetQuota(long nsQuota, long ssQuota, StorageType type)
		{
			if (type != null)
			{
				this.quota.SetTypeSpace(type, ssQuota);
			}
			else
			{
				SetQuota(nsQuota, ssQuota);
			}
		}

		internal void SetQuota(long nsQuota, long ssQuota)
		{
			this.quota.SetNameSpace(nsQuota);
			this.quota.SetStorageSpace(ssQuota);
		}

		internal void SetQuota(long quota, StorageType type)
		{
			this.quota.SetTypeSpace(type, quota);
		}

		/// <summary>Set storage type quota in a batch.</summary>
		/// <remarks>Set storage type quota in a batch. (Only used by FSImage load)</remarks>
		/// <param name="tsQuotas">type space counts for all storage types supporting quota</param>
		internal void SetQuota(EnumCounters<StorageType> tsQuotas)
		{
			this.quota.SetTypeSpaces(tsQuotas);
		}

		/// <summary>Add current quota usage to counts and return the updated counts</summary>
		/// <param name="counts">counts to be added with current quota usage</param>
		/// <returns>counts that have been added with the current qutoa usage</returns>
		internal QuotaCounts AddCurrentSpaceUsage(QuotaCounts counts)
		{
			counts.Add(this.usage);
			return counts;
		}

		internal ContentSummaryComputationContext ComputeContentSummary(INodeDirectory dir
			, ContentSummaryComputationContext summary)
		{
			long original = summary.GetCounts().GetStoragespace();
			long oldYieldCount = summary.GetYieldCount();
			dir.ComputeDirectoryContentSummary(summary, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId);
			// Check only when the content has not changed in the middle.
			if (oldYieldCount == summary.GetYieldCount())
			{
				CheckStoragespace(dir, summary.GetCounts().GetStoragespace() - original);
			}
			return summary;
		}

		private void CheckStoragespace(INodeDirectory dir, long computed)
		{
			if (-1 != quota.GetStorageSpace() && usage.GetStorageSpace() != computed)
			{
				NameNode.Log.Error("BUG: Inconsistent storagespace for directory " + dir.GetFullPathName
					() + ". Cached = " + usage.GetStorageSpace() + " != Computed = " + computed);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
		internal void AddSpaceConsumed(INodeDirectory dir, QuotaCounts counts, bool verify
			)
		{
			if (dir.IsQuotaSet())
			{
				// The following steps are important:
				// check quotas in this inode and all ancestors before changing counts
				// so that no change is made if there is any quota violation.
				// (1) verify quota in this inode
				if (verify)
				{
					VerifyQuota(counts);
				}
				// (2) verify quota and then add count in ancestors
				dir.AddSpaceConsumed2Parent(counts, verify);
				// (3) add count in this inode
				AddSpaceConsumed2Cache(counts);
			}
			else
			{
				dir.AddSpaceConsumed2Parent(counts, verify);
			}
		}

		/// <summary>Update the space/namespace/type usage of the tree</summary>
		/// <param name="delta">the change of the namespace/space/type usage</param>
		public void AddSpaceConsumed2Cache(QuotaCounts delta)
		{
			usage.Add(delta);
		}

		/// <summary>
		/// Sets namespace and storagespace take by the directory rooted
		/// at this INode.
		/// </summary>
		/// <remarks>
		/// Sets namespace and storagespace take by the directory rooted
		/// at this INode. This should be used carefully. It does not check
		/// for quota violations.
		/// </remarks>
		/// <param name="namespace">size of the directory to be set</param>
		/// <param name="storagespace">storage space take by all the nodes under this directory
		/// 	</param>
		/// <param name="typespaces">counters of storage type usage</param>
		internal void SetSpaceConsumed(long @namespace, long storagespace, EnumCounters<StorageType
			> typespaces)
		{
			usage.SetNameSpace(@namespace);
			usage.SetStorageSpace(storagespace);
			usage.SetTypeSpaces(typespaces);
		}

		internal void SetSpaceConsumed(QuotaCounts c)
		{
			usage.SetNameSpace(c.GetNameSpace());
			usage.SetStorageSpace(c.GetStorageSpace());
			usage.SetTypeSpaces(c.GetTypeSpaces());
		}

		/// <returns>the namespace and storagespace and typespace consumed.</returns>
		public QuotaCounts GetSpaceConsumed()
		{
			return new QuotaCounts.Builder().QuotaCount(usage).Build();
		}

		/// <summary>Verify if the namespace quota is violated after applying delta.</summary>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.NSQuotaExceededException"/>
		private void VerifyNamespaceQuota(long delta)
		{
			if (Quota.IsViolated(quota.GetNameSpace(), usage.GetNameSpace(), delta))
			{
				throw new NSQuotaExceededException(quota.GetNameSpace(), usage.GetNameSpace() + delta
					);
			}
		}

		/// <summary>Verify if the storagespace quota is violated after applying delta.</summary>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.DSQuotaExceededException"/>
		private void VerifyStoragespaceQuota(long delta)
		{
			if (Quota.IsViolated(quota.GetStorageSpace(), usage.GetStorageSpace(), delta))
			{
				throw new DSQuotaExceededException(quota.GetStorageSpace(), usage.GetStorageSpace
					() + delta);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaByStorageTypeExceededException
		/// 	"/>
		private void VerifyQuotaByStorageType(EnumCounters<StorageType> typeDelta)
		{
			if (!IsQuotaByStorageTypeSet())
			{
				return;
			}
			foreach (StorageType t in StorageType.GetTypesSupportingQuota())
			{
				if (!IsQuotaByStorageTypeSet(t))
				{
					continue;
				}
				if (Quota.IsViolated(quota.GetTypeSpace(t), usage.GetTypeSpace(t), typeDelta.Get(
					t)))
				{
					throw new QuotaByStorageTypeExceededException(quota.GetTypeSpace(t), usage.GetTypeSpace
						(t) + typeDelta.Get(t), t);
				}
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException">
		/// if namespace, storagespace or storage type
		/// space quota is violated after applying the deltas.
		/// </exception>
		internal void VerifyQuota(QuotaCounts counts)
		{
			VerifyNamespaceQuota(counts.GetNameSpace());
			VerifyStoragespaceQuota(counts.GetStorageSpace());
			VerifyQuotaByStorageType(counts.GetTypeSpaces());
		}

		internal bool IsQuotaSet()
		{
			return quota.AnyNsSsCountGreaterOrEqual(0) || quota.AnyTypeSpaceCountGreaterOrEqual
				(0);
		}

		internal bool IsQuotaByStorageTypeSet()
		{
			return quota.AnyTypeSpaceCountGreaterOrEqual(0);
		}

		internal bool IsQuotaByStorageTypeSet(StorageType t)
		{
			return quota.GetTypeSpace(t) >= 0;
		}

		private string NamespaceString()
		{
			return "namespace: " + (quota.GetNameSpace() < 0 ? "-" : usage.GetNameSpace() + "/"
				 + quota.GetNameSpace());
		}

		private string StoragespaceString()
		{
			return "storagespace: " + (quota.GetStorageSpace() < 0 ? "-" : usage.GetStorageSpace
				() + "/" + quota.GetStorageSpace());
		}

		private string TypeSpaceString()
		{
			StringBuilder sb = new StringBuilder();
			foreach (StorageType t in StorageType.GetTypesSupportingQuota())
			{
				sb.Append("StorageType: " + t + (quota.GetTypeSpace(t) < 0 ? "-" : usage.GetTypeSpace
					(t) + "/" + usage.GetTypeSpace(t)));
			}
			return sb.ToString();
		}

		public override string ToString()
		{
			return "Quota[" + NamespaceString() + ", " + StoragespaceString() + ", " + TypeSpaceString
				() + "]";
		}
	}
}
