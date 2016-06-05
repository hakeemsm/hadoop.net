using System;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.RM
{
	/// <summary>Exception to denote fatal failure in allocating containers from RM.</summary>
	[System.Serializable]
	public class RMContainerAllocationException : Exception
	{
		private const long serialVersionUID = 1L;

		public RMContainerAllocationException(Exception cause)
			: base(cause)
		{
		}

		public RMContainerAllocationException(string message)
			: base(message)
		{
		}

		public RMContainerAllocationException(string message, Exception cause)
			: base(message, cause)
		{
		}
	}
}
