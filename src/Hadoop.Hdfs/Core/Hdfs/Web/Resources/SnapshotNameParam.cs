using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>The snapshot name parameter for createSnapshot and deleteSnapshot operation.
	/// 	</summary>
	/// <remarks>
	/// The snapshot name parameter for createSnapshot and deleteSnapshot operation.
	/// Also used to indicate the new snapshot name for renameSnapshot operation.
	/// </remarks>
	public class SnapshotNameParam : StringParam
	{
		/// <summary>Parameter name.</summary>
		public const string Name = "snapshotname";

		/// <summary>Default parameter value.</summary>
		public const string Default = string.Empty;

		private static readonly StringParam.Domain Domain = new StringParam.Domain(Name, 
			null);

		public SnapshotNameParam(string str)
			: base(Domain, str != null && !str.Equals(Default) ? str : null)
		{
		}

		public override string GetName()
		{
			return Name;
		}
	}
}
