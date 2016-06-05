using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	/// <summary>Information that describes a journal</summary>
	public class JournalInfo
	{
		private readonly int layoutVersion;

		private readonly string clusterId;

		private readonly int namespaceId;

		public JournalInfo(int lv, string clusterId, int nsId)
		{
			this.layoutVersion = lv;
			this.clusterId = clusterId;
			this.namespaceId = nsId;
		}

		public virtual int GetLayoutVersion()
		{
			return layoutVersion;
		}

		public virtual string GetClusterId()
		{
			return clusterId;
		}

		public virtual int GetNamespaceId()
		{
			return namespaceId;
		}
	}
}
