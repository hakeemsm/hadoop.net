using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Client
{
	/// <summary>Options that can be specified when manually triggering a block report.</summary>
	public sealed class BlockReportOptions
	{
		private readonly bool incremental;

		private BlockReportOptions(bool incremental)
		{
			this.incremental = incremental;
		}

		public bool IsIncremental()
		{
			return incremental;
		}

		public class Factory
		{
			private bool incremental = false;

			public Factory()
			{
			}

			public virtual BlockReportOptions.Factory SetIncremental(bool incremental)
			{
				this.incremental = incremental;
				return this;
			}

			public virtual BlockReportOptions Build()
			{
				return new BlockReportOptions(incremental);
			}
		}

		public override string ToString()
		{
			return "BlockReportOptions{incremental=" + incremental + "}";
		}
	}
}
