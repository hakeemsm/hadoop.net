using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>
	/// Factory class which returns instances of operations given there operation
	/// type enumeration (in string or enumeration format).
	/// </summary>
	internal class OperationFactory
	{
		private IDictionary<Constants.OperationType, Operation> typedOperations;

		private ConfigExtractor config;

		private Random rnd;

		internal OperationFactory(ConfigExtractor cfg, Random rnd)
		{
			this.typedOperations = new Dictionary<Constants.OperationType, Operation>();
			this.config = cfg;
			this.rnd = rnd;
		}

		/// <summary>Gets an operation instance (cached) for a given operation type</summary>
		/// <param name="type">the operation type to fetch for</param>
		/// <returns>Operation operation instance or null if it can not be fetched.</returns>
		internal virtual Operation GetOperation(Constants.OperationType type)
		{
			Operation op = typedOperations[type];
			if (op != null)
			{
				return op;
			}
			switch (type)
			{
				case Constants.OperationType.Read:
				{
					op = new ReadOp(this.config, rnd);
					break;
				}

				case Constants.OperationType.Ls:
				{
					op = new ListOp(this.config, rnd);
					break;
				}

				case Constants.OperationType.Mkdir:
				{
					op = new MkdirOp(this.config, rnd);
					break;
				}

				case Constants.OperationType.Append:
				{
					op = new AppendOp(this.config, rnd);
					break;
				}

				case Constants.OperationType.Rename:
				{
					op = new RenameOp(this.config, rnd);
					break;
				}

				case Constants.OperationType.Delete:
				{
					op = new DeleteOp(this.config, rnd);
					break;
				}

				case Constants.OperationType.Create:
				{
					op = new CreateOp(this.config, rnd);
					break;
				}

				case Constants.OperationType.Truncate:
				{
					op = new TruncateOp(this.config, rnd);
					break;
				}
			}
			typedOperations[type] = op;
			return op;
		}
	}
}
