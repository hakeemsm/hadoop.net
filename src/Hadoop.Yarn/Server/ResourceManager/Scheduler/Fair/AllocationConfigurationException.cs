using System;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	/// <summary>
	/// Thrown when the allocation file for
	/// <see cref="QueueManager"/>
	/// is malformed.
	/// </summary>
	[System.Serializable]
	public class AllocationConfigurationException : Exception
	{
		private const long serialVersionUID = 4046517047810854249L;

		public AllocationConfigurationException(string message)
			: base(message)
		{
		}

		public AllocationConfigurationException(string message, Exception t)
			: base(message, t)
		{
		}
	}
}
