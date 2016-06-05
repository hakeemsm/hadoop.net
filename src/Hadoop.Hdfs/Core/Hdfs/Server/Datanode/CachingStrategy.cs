using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>The caching strategy we should use for an HDFS read or write operation.</summary>
	public class CachingStrategy
	{
		private readonly bool dropBehind;

		private readonly long readahead;

		// null = use server defaults
		// null = use server defaults
		public static Org.Apache.Hadoop.Hdfs.Server.Datanode.CachingStrategy NewDefaultStrategy
			()
		{
			return new Org.Apache.Hadoop.Hdfs.Server.Datanode.CachingStrategy(null, null);
		}

		public static Org.Apache.Hadoop.Hdfs.Server.Datanode.CachingStrategy NewDropBehind
			()
		{
			return new Org.Apache.Hadoop.Hdfs.Server.Datanode.CachingStrategy(true, null);
		}

		public class Builder
		{
			private bool dropBehind;

			private long readahead;

			public Builder(CachingStrategy prev)
			{
				this.dropBehind = prev.dropBehind;
				this.readahead = prev.readahead;
			}

			public virtual CachingStrategy.Builder SetDropBehind(bool dropBehind)
			{
				this.dropBehind = dropBehind;
				return this;
			}

			public virtual CachingStrategy.Builder SetReadahead(long readahead)
			{
				this.readahead = readahead;
				return this;
			}

			public virtual CachingStrategy Build()
			{
				return new CachingStrategy(dropBehind, readahead);
			}
		}

		public CachingStrategy(bool dropBehind, long readahead)
		{
			this.dropBehind = dropBehind;
			this.readahead = readahead;
		}

		public virtual bool GetDropBehind()
		{
			return dropBehind;
		}

		public virtual long GetReadahead()
		{
			return readahead;
		}

		public override string ToString()
		{
			return "CachingStrategy(dropBehind=" + dropBehind + ", readahead=" + readahead + 
				")";
		}
	}
}
