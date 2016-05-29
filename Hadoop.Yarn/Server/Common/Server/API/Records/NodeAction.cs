using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Records
{
	/// <summary>The NodeManager is instructed to perform the given action.</summary>
	public enum NodeAction
	{
		Normal,
		Resync,
		Shutdown
	}
}
