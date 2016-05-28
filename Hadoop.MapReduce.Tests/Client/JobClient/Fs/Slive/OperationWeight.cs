using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>Class which holds an operation and its weight (used in operation selection)
	/// 	</summary>
	internal class OperationWeight
	{
		private double weight;

		private Operation operation;

		internal OperationWeight(Operation op, double weight)
		{
			this.operation = op;
			this.weight = weight;
		}

		/// <summary>Fetches the given operation weight</summary>
		/// <returns>Double</returns>
		internal virtual double GetWeight()
		{
			return weight;
		}

		/// <summary>Gets the operation</summary>
		/// <returns>Operation</returns>
		internal virtual Operation GetOperation()
		{
			return operation;
		}
	}
}
