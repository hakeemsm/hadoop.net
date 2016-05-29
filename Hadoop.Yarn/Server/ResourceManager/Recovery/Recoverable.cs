using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery
{
	public interface Recoverable
	{
		/// <exception cref="System.Exception"/>
		void Recover(RMStateStore.RMState state);
	}
}
