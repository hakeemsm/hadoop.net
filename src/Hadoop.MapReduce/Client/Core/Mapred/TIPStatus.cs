using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>The states of a Tasks.</summary>
	public enum TIPStatus
	{
		Pending,
		Running,
		Complete,
		Killed,
		Failed
	}
}
