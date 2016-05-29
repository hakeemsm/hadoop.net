using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler
{
	/// <summary>This interface is the one implemented by the schedulers.</summary>
	/// <remarks>
	/// This interface is the one implemented by the schedulers. It mainly extends
	/// <see cref="YarnScheduler"/>
	/// .
	/// </remarks>
	public interface ResourceScheduler : YarnScheduler, Recoverable
	{
		/// <summary>Set RMContext for <code>ResourceScheduler</code>.</summary>
		/// <remarks>
		/// Set RMContext for <code>ResourceScheduler</code>.
		/// This method should be called immediately after instantiating
		/// a scheduler once.
		/// </remarks>
		/// <param name="rmContext">created by ResourceManager</param>
		void SetRMContext(RMContext rmContext);

		/// <summary>Re-initialize the <code>ResourceScheduler</code>.</summary>
		/// <param name="conf">configuration</param>
		/// <exception cref="System.IO.IOException"/>
		void Reinitialize(Configuration conf, RMContext rmContext);
	}
}
