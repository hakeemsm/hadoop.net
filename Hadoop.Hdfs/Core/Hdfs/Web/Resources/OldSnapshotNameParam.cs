using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>The old snapshot name parameter for renameSnapshot operation.</summary>
	public class OldSnapshotNameParam : StringParam
	{
		/// <summary>Parameter name.</summary>
		public const string Name = "oldsnapshotname";

		/// <summary>Default parameter value.</summary>
		public const string Default = string.Empty;

		private static readonly StringParam.Domain Domain = new StringParam.Domain(Name, 
			null);

		public OldSnapshotNameParam(string str)
			: base(Domain, str != null && !str.Equals(Default) ? str : null)
		{
		}

		public override string GetName()
		{
			return Name;
		}
	}
}
