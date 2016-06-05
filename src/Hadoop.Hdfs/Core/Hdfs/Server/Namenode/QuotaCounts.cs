using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Counters for namespace, storage space and storage type space quota and usage.
	/// 	</summary>
	public class QuotaCounts
	{
		private EnumCounters<Quota> nsSsCounts;

		private EnumCounters<StorageType> tsCounts;

		public class Builder
		{
			private EnumCounters<Quota> nsSsCounts;

			private EnumCounters<StorageType> tsCounts;

			public Builder()
			{
				// Name space and storage space counts (HDFS-7775 refactors the original disk
				// space count to storage space counts)
				// Storage type space counts
				this.nsSsCounts = new EnumCounters<Quota>(typeof(Quota));
				this.tsCounts = new EnumCounters<StorageType>(typeof(StorageType));
			}

			public virtual QuotaCounts.Builder NameSpace(long val)
			{
				this.nsSsCounts.Set(Quota.Namespace, val);
				return this;
			}

			public virtual QuotaCounts.Builder StorageSpace(long val)
			{
				this.nsSsCounts.Set(Quota.Storagespace, val);
				return this;
			}

			public virtual QuotaCounts.Builder TypeSpaces(EnumCounters<StorageType> val)
			{
				if (val != null)
				{
					this.tsCounts.Set(val);
				}
				return this;
			}

			public virtual QuotaCounts.Builder TypeSpaces(long val)
			{
				this.tsCounts.Reset(val);
				return this;
			}

			public virtual QuotaCounts.Builder QuotaCount(QuotaCounts that)
			{
				this.nsSsCounts.Set(that.nsSsCounts);
				this.tsCounts.Set(that.tsCounts);
				return this;
			}

			public virtual QuotaCounts Build()
			{
				return new QuotaCounts(this);
			}
		}

		private QuotaCounts(QuotaCounts.Builder builder)
		{
			this.nsSsCounts = builder.nsSsCounts;
			this.tsCounts = builder.tsCounts;
		}

		public virtual void Add(QuotaCounts that)
		{
			this.nsSsCounts.Add(that.nsSsCounts);
			this.tsCounts.Add(that.tsCounts);
		}

		public virtual void Subtract(QuotaCounts that)
		{
			this.nsSsCounts.Subtract(that.nsSsCounts);
			this.tsCounts.Subtract(that.tsCounts);
		}

		/// <summary>
		/// Returns a QuotaCounts whose value is
		/// <c>(-this)</c>
		/// .
		/// </summary>
		/// <returns>
		/// 
		/// <c>-this</c>
		/// </returns>
		public virtual QuotaCounts Negation()
		{
			QuotaCounts ret = new QuotaCounts.Builder().QuotaCount(this).Build();
			ret.nsSsCounts.Negation();
			ret.tsCounts.Negation();
			return ret;
		}

		public virtual long GetNameSpace()
		{
			return nsSsCounts.Get(Quota.Namespace);
		}

		public virtual void SetNameSpace(long nameSpaceCount)
		{
			this.nsSsCounts.Set(Quota.Namespace, nameSpaceCount);
		}

		public virtual void AddNameSpace(long nsDelta)
		{
			this.nsSsCounts.Add(Quota.Namespace, nsDelta);
		}

		public virtual long GetStorageSpace()
		{
			return nsSsCounts.Get(Quota.Storagespace);
		}

		public virtual void SetStorageSpace(long spaceCount)
		{
			this.nsSsCounts.Set(Quota.Storagespace, spaceCount);
		}

		public virtual void AddStorageSpace(long dsDelta)
		{
			this.nsSsCounts.Add(Quota.Storagespace, dsDelta);
		}

		public virtual EnumCounters<StorageType> GetTypeSpaces()
		{
			EnumCounters<StorageType> ret = new EnumCounters<StorageType>(typeof(StorageType)
				);
			ret.Set(tsCounts);
			return ret;
		}

		internal virtual void SetTypeSpaces(EnumCounters<StorageType> that)
		{
			if (that != null)
			{
				this.tsCounts.Set(that);
			}
		}

		internal virtual long GetTypeSpace(StorageType type)
		{
			return this.tsCounts.Get(type);
		}

		internal virtual void SetTypeSpace(StorageType type, long spaceCount)
		{
			this.tsCounts.Set(type, spaceCount);
		}

		public virtual void AddTypeSpace(StorageType type, long delta)
		{
			this.tsCounts.Add(type, delta);
		}

		public virtual bool AnyNsSsCountGreaterOrEqual(long val)
		{
			return nsSsCounts.AnyGreaterOrEqual(val);
		}

		public virtual bool AnyTypeSpaceCountGreaterOrEqual(long val)
		{
			return tsCounts.AnyGreaterOrEqual(val);
		}

		public override bool Equals(object obj)
		{
			if (obj == this)
			{
				return true;
			}
			else
			{
				if (obj == null || !(obj is QuotaCounts))
				{
					return false;
				}
			}
			QuotaCounts that = (QuotaCounts)obj;
			return this.nsSsCounts.Equals(that.nsSsCounts) && this.tsCounts.Equals(that.tsCounts
				);
		}

		public override int GetHashCode()
		{
			System.Diagnostics.Debug.Assert(false, "hashCode not designed");
			return 42;
		}
		// any arbitrary constant will do
	}
}
