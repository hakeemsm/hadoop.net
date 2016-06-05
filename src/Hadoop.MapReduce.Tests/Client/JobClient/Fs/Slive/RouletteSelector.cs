using System;
using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>
	/// A selection object which simulates a roulette wheel whereby all operation
	/// have a weight and the total value of the wheel is the combined weight and
	/// during selection a random number (0, total weight) is selected and then the
	/// operation that is at that value will be selected.
	/// </summary>
	/// <remarks>
	/// A selection object which simulates a roulette wheel whereby all operation
	/// have a weight and the total value of the wheel is the combined weight and
	/// during selection a random number (0, total weight) is selected and then the
	/// operation that is at that value will be selected. So for a set of operations
	/// with uniform weight they will all have the same probability of being
	/// selected. Operations which choose to have higher weights will have higher
	/// likelihood of being selected (and the same goes for lower weights).
	/// </remarks>
	internal class RouletteSelector
	{
		private Random picker;

		internal RouletteSelector(Random rnd)
		{
			picker = rnd;
		}

		internal virtual Operation Select(IList<OperationWeight> ops)
		{
			if (ops.IsEmpty())
			{
				return null;
			}
			double totalWeight = 0;
			foreach (OperationWeight w in ops)
			{
				if (w.GetWeight() < 0)
				{
					throw new ArgumentException("Negative weights not allowed");
				}
				totalWeight += w.GetWeight();
			}
			// roulette wheel selection
			double sAm = picker.NextDouble() * totalWeight;
			int index = 0;
			for (int i = 0; i < ops.Count; ++i)
			{
				sAm -= ops[i].GetWeight();
				if (sAm <= 0)
				{
					index = i;
					break;
				}
			}
			return ops[index].GetOperation();
		}
	}
}
