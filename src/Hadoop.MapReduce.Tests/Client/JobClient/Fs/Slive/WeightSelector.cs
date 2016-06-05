using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>
	/// This class is the main handler that selects operations to run using the
	/// currently held selection object.
	/// </summary>
	/// <remarks>
	/// This class is the main handler that selects operations to run using the
	/// currently held selection object. It configures and weights each operation and
	/// then hands the operations + weights off to the selector object to determine
	/// which one should be ran. If no operations are left to be ran then it will
	/// return null.
	/// </remarks>
	internal class WeightSelector
	{
		internal interface Weightable
		{
			// what a weight calculation means
			double Weight(int elapsed, int duration);
		}

		private static readonly Log Log = LogFactory.GetLog(typeof(WeightSelector));

		private class OperationInfo
		{
			internal int amountLeft;

			internal Operation operation;

			internal Constants.Distribution distribution;
		}

		private IDictionary<Constants.OperationType, WeightSelector.OperationInfo> operations;

		private IDictionary<Constants.Distribution, WeightSelector.Weightable> weights;

		private RouletteSelector selector;

		private OperationFactory factory;

		internal WeightSelector(ConfigExtractor cfg, Random rnd)
		{
			selector = new RouletteSelector(rnd);
			factory = new OperationFactory(cfg, rnd);
			ConfigureOperations(cfg);
			ConfigureWeights(cfg);
		}

		protected internal virtual RouletteSelector GetSelector()
		{
			return selector;
		}

		private void ConfigureWeights(ConfigExtractor e)
		{
			weights = new Dictionary<Constants.Distribution, WeightSelector.Weightable>();
			weights[Constants.Distribution.Uniform] = new Weights.UniformWeight();
		}

		// weights.put(Distribution.BEG, new BeginWeight());
		// weights.put(Distribution.END, new EndWeight());
		// weights.put(Distribution.MID, new MidWeight());
		/// <summary>Determines how many initial operations a given operation data should have
		/// 	</summary>
		/// <param name="totalAm">the total amount of operations allowed</param>
		/// <param name="opData">the given operation information (with a valid percentage &gt;= 0)
		/// 	</param>
		/// <returns>the number of items to allow to run</returns>
		/// <exception cref="System.ArgumentException">if negative operations are determined</exception>
		internal static int DetermineHowMany(int totalAm, OperationData opData, Constants.OperationType
			 type)
		{
			if (totalAm <= 0)
			{
				return 0;
			}
			int amLeft = (int)Math.Floor(opData.GetPercent() * totalAm);
			if (amLeft < 0)
			{
				throw new ArgumentException("Invalid amount " + amLeft + " determined for operation type "
					 + type.ToString());
			}
			return amLeft;
		}

		/// <summary>
		/// Sets up the operation using the given configuration by setting up the
		/// number of operations to perform (and how many are left) and setting up the
		/// operation objects to be used throughout selection.
		/// </summary>
		/// <param name="cfg">ConfigExtractor.</param>
		private void ConfigureOperations(ConfigExtractor cfg)
		{
			operations = new SortedDictionary<Constants.OperationType, WeightSelector.OperationInfo
				>();
			IDictionary<Constants.OperationType, OperationData> opinfo = cfg.GetOperations();
			int totalAm = cfg.GetOpCount();
			int opsLeft = totalAm;
			NumberFormat formatter = Formatter.GetPercentFormatter();
			foreach (Constants.OperationType type in opinfo.Keys)
			{
				OperationData opData = opinfo[type];
				WeightSelector.OperationInfo info = new WeightSelector.OperationInfo();
				info.distribution = opData.GetDistribution();
				int amLeft = DetermineHowMany(totalAm, opData, type);
				opsLeft -= amLeft;
				Log.Info(type.ToString() + " has " + amLeft + " initial operations out of " + totalAm
					 + " for its ratio " + formatter.Format(opData.GetPercent()));
				info.amountLeft = amLeft;
				Operation op = factory.GetOperation(type);
				// wrap operation in finalizer so that amount left gets decrements when
				// its done
				if (op != null)
				{
					ObserveableOp.Observer fn = new _Observer_138(this, type);
					info.operation = new ObserveableOp(op, fn);
					operations[type] = info;
				}
			}
			if (opsLeft > 0)
			{
				Log.Info(opsLeft + " left over operations found (due to inability to support partial operations)"
					);
			}
		}

		private sealed class _Observer_138 : ObserveableOp.Observer
		{
			public _Observer_138(WeightSelector _enclosing, Constants.OperationType type)
			{
				this._enclosing = _enclosing;
				this.type = type;
			}

			public void NotifyFinished(Operation op)
			{
				WeightSelector.OperationInfo opInfo = this._enclosing.operations[type];
				if (opInfo != null)
				{
					--opInfo.amountLeft;
				}
			}

			public void NotifyStarting(Operation op)
			{
			}

			private readonly WeightSelector _enclosing;

			private readonly Constants.OperationType type;
		}

		/// <summary>
		/// Selects an operation from the known operation set or returns null if none
		/// are available by applying the weighting algorithms and then handing off the
		/// weight operations to the selection object.
		/// </summary>
		/// <param name="elapsed">the currently elapsed time (milliseconds) of the running program
		/// 	</param>
		/// <param name="duration">the maximum amount of milliseconds of the running program</param>
		/// <returns>operation or null if none left</returns>
		internal virtual Operation Select(int elapsed, int duration)
		{
			IList<OperationWeight> validOps = new AList<OperationWeight>(operations.Count);
			foreach (Constants.OperationType type in operations.Keys)
			{
				WeightSelector.OperationInfo opinfo = operations[type];
				if (opinfo == null || opinfo.amountLeft <= 0)
				{
					continue;
				}
				WeightSelector.Weightable weighter = weights[opinfo.distribution];
				if (weighter != null)
				{
					OperationWeight weightOp = new OperationWeight(opinfo.operation, weighter.Weight(
						elapsed, duration));
					validOps.AddItem(weightOp);
				}
				else
				{
					throw new RuntimeException("Unable to get weight for distribution " + opinfo.distribution
						);
				}
			}
			if (validOps.IsEmpty())
			{
				return null;
			}
			return GetSelector().Select(validOps);
		}
	}
}
