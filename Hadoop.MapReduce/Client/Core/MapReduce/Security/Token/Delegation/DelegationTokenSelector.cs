using Org.Apache.Hadoop.Security.Token.Delegation;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Security.Token.Delegation
{
	/// <summary>A delegation token that is specialized for MapReduce</summary>
	public class DelegationTokenSelector : AbstractDelegationTokenSelector<DelegationTokenIdentifier
		>
	{
		public DelegationTokenSelector()
			: base(DelegationTokenIdentifier.MapreduceDelegationKind)
		{
		}
	}
}
