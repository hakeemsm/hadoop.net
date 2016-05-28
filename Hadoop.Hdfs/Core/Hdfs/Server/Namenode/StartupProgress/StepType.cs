using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress
{
	/// <summary>
	/// Indicates a particular type of
	/// <see cref="Step"/>
	/// .
	/// </summary>
	[System.Serializable]
	public sealed class StepType
	{
		/// <summary>
		/// The namenode has entered safemode and is awaiting block reports from
		/// datanodes.
		/// </summary>
		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress.StepType
			 AwaitingReportedBlocks = new Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress.StepType
			("AwaitingReportedBlocks", "awaiting reported blocks");

		/// <summary>The namenode is performing an operation related to delegation keys.</summary>
		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress.StepType
			 DelegationKeys = new Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress.StepType
			("DelegationKeys", "delegation keys");

		/// <summary>The namenode is performing an operation related to delegation tokens.</summary>
		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress.StepType
			 DelegationTokens = new Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress.StepType
			("DelegationTokens", "delegation tokens");

		/// <summary>The namenode is performing an operation related to inodes.</summary>
		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress.StepType
			 Inodes = new Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress.StepType("Inodes"
			, "inodes");

		/// <summary>The namenode is performing an operation related to cache pools.</summary>
		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress.StepType
			 CachePools = new Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress.StepType
			("CachePools", "cache pools");

		/// <summary>The namenode is performing an operation related to cache entries.</summary>
		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress.StepType
			 CacheEntries = new Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress.StepType
			("CacheEntries", "cache entries");

		private readonly string name;

		private readonly string description;

		/// <summary>Private constructor of enum.</summary>
		/// <param name="name">String step type name</param>
		/// <param name="description">String step type description</param>
		private StepType(string name, string description)
		{
			this.name = name;
			this.description = description;
		}

		/// <summary>Returns step type description.</summary>
		/// <returns>String step type description</returns>
		public string GetDescription()
		{
			return Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress.StepType.description;
		}

		/// <summary>Returns step type name.</summary>
		/// <returns>String step type name</returns>
		public string GetName()
		{
			return Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress.StepType.name;
		}
	}
}
