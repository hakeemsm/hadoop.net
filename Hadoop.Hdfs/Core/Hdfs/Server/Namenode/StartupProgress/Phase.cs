using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress
{
	/// <summary>Indicates a particular phase of the namenode startup sequence.</summary>
	/// <remarks>
	/// Indicates a particular phase of the namenode startup sequence.  The phases
	/// are listed here in their execution order.
	/// </remarks>
	[System.Serializable]
	public sealed class Phase
	{
		/// <summary>The namenode is loading the fsimage file into memory.</summary>
		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress.Phase
			 LoadingFsimage = new Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress.Phase
			("LoadingFsImage", "Loading fsimage");

		/// <summary>
		/// The namenode is loading the edits file and applying its operations to the
		/// in-memory metadata.
		/// </summary>
		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress.Phase
			 LoadingEdits = new Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress.Phase
			("LoadingEdits", "Loading edits");

		/// <summary>The namenode is saving a new checkpoint.</summary>
		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress.Phase
			 SavingCheckpoint = new Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress.Phase
			("SavingCheckpoint", "Saving checkpoint");

		/// <summary>The namenode has entered safemode, awaiting block reports from data nodes.
		/// 	</summary>
		public static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress.Phase
			 Safemode = new Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress.Phase("SafeMode"
			, "Safe mode");

		private readonly string name;

		private readonly string description;

		/// <summary>Returns phase description.</summary>
		/// <returns>String description</returns>
		public string GetDescription()
		{
			return Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress.Phase.description;
		}

		/// <summary>Returns phase name.</summary>
		/// <returns>String phase name</returns>
		public string GetName()
		{
			return Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress.Phase.name;
		}

		/// <summary>Private constructor of enum.</summary>
		/// <param name="name">String phase name</param>
		/// <param name="description">String phase description</param>
		private Phase(string name, string description)
		{
			this.name = name;
			this.description = description;
		}
	}
}
