using System;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util.Resource
{
	public class Resources
	{
		private sealed class _Resource_31 : Org.Apache.Hadoop.Yarn.Api.Records.Resource
		{
			public _Resource_31()
			{
			}

			// Java doesn't have const :(
			public override int GetMemory()
			{
				return 0;
			}

			public override void SetMemory(int memory)
			{
				throw new RuntimeException("NONE cannot be modified!");
			}

			public override int GetVirtualCores()
			{
				return 0;
			}

			public override void SetVirtualCores(int cores)
			{
				throw new RuntimeException("NONE cannot be modified!");
			}

			public override int CompareTo(Org.Apache.Hadoop.Yarn.Api.Records.Resource o)
			{
				int diff = 0 - o.GetMemory();
				if (diff == 0)
				{
					diff = 0 - o.GetVirtualCores();
				}
				return diff;
			}
		}

		private static readonly Org.Apache.Hadoop.Yarn.Api.Records.Resource None = new _Resource_31
			();

		private sealed class _Resource_64 : Org.Apache.Hadoop.Yarn.Api.Records.Resource
		{
			public _Resource_64()
			{
			}

			public override int GetMemory()
			{
				return int.MaxValue;
			}

			public override void SetMemory(int memory)
			{
				throw new RuntimeException("NONE cannot be modified!");
			}

			public override int GetVirtualCores()
			{
				return int.MaxValue;
			}

			public override void SetVirtualCores(int cores)
			{
				throw new RuntimeException("NONE cannot be modified!");
			}

			public override int CompareTo(Org.Apache.Hadoop.Yarn.Api.Records.Resource o)
			{
				int diff = 0 - o.GetMemory();
				if (diff == 0)
				{
					diff = 0 - o.GetVirtualCores();
				}
				return diff;
			}
		}

		private static readonly Org.Apache.Hadoop.Yarn.Api.Records.Resource Unbounded = new 
			_Resource_64();

		public static Org.Apache.Hadoop.Yarn.Api.Records.Resource CreateResource(int memory
			)
		{
			return CreateResource(memory, (memory > 0) ? 1 : 0);
		}

		public static Org.Apache.Hadoop.Yarn.Api.Records.Resource CreateResource(int memory
			, int cores)
		{
			Org.Apache.Hadoop.Yarn.Api.Records.Resource resource = Org.Apache.Hadoop.Yarn.Util.Records
				.NewRecord<Org.Apache.Hadoop.Yarn.Api.Records.Resource>();
			resource.SetMemory(memory);
			resource.SetVirtualCores(cores);
			return resource;
		}

		public static Org.Apache.Hadoop.Yarn.Api.Records.Resource None()
		{
			return None;
		}

		public static Org.Apache.Hadoop.Yarn.Api.Records.Resource Unbounded()
		{
			return Unbounded;
		}

		public static Org.Apache.Hadoop.Yarn.Api.Records.Resource Clone(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 res)
		{
			return CreateResource(res.GetMemory(), res.GetVirtualCores());
		}

		public static Org.Apache.Hadoop.Yarn.Api.Records.Resource AddTo(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 lhs, Org.Apache.Hadoop.Yarn.Api.Records.Resource rhs)
		{
			lhs.SetMemory(lhs.GetMemory() + rhs.GetMemory());
			lhs.SetVirtualCores(lhs.GetVirtualCores() + rhs.GetVirtualCores());
			return lhs;
		}

		public static Org.Apache.Hadoop.Yarn.Api.Records.Resource Add(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 lhs, Org.Apache.Hadoop.Yarn.Api.Records.Resource rhs)
		{
			return AddTo(Clone(lhs), rhs);
		}

		public static Org.Apache.Hadoop.Yarn.Api.Records.Resource SubtractFrom(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 lhs, Org.Apache.Hadoop.Yarn.Api.Records.Resource rhs)
		{
			lhs.SetMemory(lhs.GetMemory() - rhs.GetMemory());
			lhs.SetVirtualCores(lhs.GetVirtualCores() - rhs.GetVirtualCores());
			return lhs;
		}

		public static Org.Apache.Hadoop.Yarn.Api.Records.Resource Subtract(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 lhs, Org.Apache.Hadoop.Yarn.Api.Records.Resource rhs)
		{
			return SubtractFrom(Clone(lhs), rhs);
		}

		public static Org.Apache.Hadoop.Yarn.Api.Records.Resource Negate(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 resource)
		{
			return Subtract(None, resource);
		}

		public static Org.Apache.Hadoop.Yarn.Api.Records.Resource MultiplyTo(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 lhs, double by)
		{
			lhs.SetMemory((int)(lhs.GetMemory() * by));
			lhs.SetVirtualCores((int)(lhs.GetVirtualCores() * by));
			return lhs;
		}

		public static Org.Apache.Hadoop.Yarn.Api.Records.Resource Multiply(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 lhs, double by)
		{
			return MultiplyTo(Clone(lhs), by);
		}

		public static Org.Apache.Hadoop.Yarn.Api.Records.Resource MultiplyAndNormalizeUp(
			ResourceCalculator calculator, Org.Apache.Hadoop.Yarn.Api.Records.Resource lhs, 
			double by, Org.Apache.Hadoop.Yarn.Api.Records.Resource factor)
		{
			return calculator.MultiplyAndNormalizeUp(lhs, by, factor);
		}

		public static Org.Apache.Hadoop.Yarn.Api.Records.Resource MultiplyAndNormalizeDown
			(ResourceCalculator calculator, Org.Apache.Hadoop.Yarn.Api.Records.Resource lhs, 
			double by, Org.Apache.Hadoop.Yarn.Api.Records.Resource factor)
		{
			return calculator.MultiplyAndNormalizeDown(lhs, by, factor);
		}

		public static Org.Apache.Hadoop.Yarn.Api.Records.Resource MultiplyAndRoundDown(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 lhs, double by)
		{
			Org.Apache.Hadoop.Yarn.Api.Records.Resource @out = Clone(lhs);
			@out.SetMemory((int)(lhs.GetMemory() * by));
			@out.SetVirtualCores((int)(lhs.GetVirtualCores() * by));
			return @out;
		}

		public static Org.Apache.Hadoop.Yarn.Api.Records.Resource Normalize(ResourceCalculator
			 calculator, Org.Apache.Hadoop.Yarn.Api.Records.Resource lhs, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 min, Org.Apache.Hadoop.Yarn.Api.Records.Resource max, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 increment)
		{
			return calculator.Normalize(lhs, min, max, increment);
		}

		public static Org.Apache.Hadoop.Yarn.Api.Records.Resource RoundUp(ResourceCalculator
			 calculator, Org.Apache.Hadoop.Yarn.Api.Records.Resource lhs, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 factor)
		{
			return calculator.RoundUp(lhs, factor);
		}

		public static Org.Apache.Hadoop.Yarn.Api.Records.Resource RoundDown(ResourceCalculator
			 calculator, Org.Apache.Hadoop.Yarn.Api.Records.Resource lhs, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 factor)
		{
			return calculator.RoundDown(lhs, factor);
		}

		public static bool IsInvalidDivisor(ResourceCalculator resourceCalculator, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 divisor)
		{
			return resourceCalculator.IsInvalidDivisor(divisor);
		}

		public static float Ratio(ResourceCalculator resourceCalculator, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 lhs, Org.Apache.Hadoop.Yarn.Api.Records.Resource rhs)
		{
			return resourceCalculator.Ratio(lhs, rhs);
		}

		public static float Divide(ResourceCalculator resourceCalculator, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource, Org.Apache.Hadoop.Yarn.Api.Records.Resource lhs, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 rhs)
		{
			return resourceCalculator.Divide(clusterResource, lhs, rhs);
		}

		public static Org.Apache.Hadoop.Yarn.Api.Records.Resource DivideAndCeil(ResourceCalculator
			 resourceCalculator, Org.Apache.Hadoop.Yarn.Api.Records.Resource lhs, int rhs)
		{
			return resourceCalculator.DivideAndCeil(lhs, rhs);
		}

		public static bool Equals(Org.Apache.Hadoop.Yarn.Api.Records.Resource lhs, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 rhs)
		{
			return lhs.Equals(rhs);
		}

		public static bool LessThan(ResourceCalculator resourceCalculator, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource, Org.Apache.Hadoop.Yarn.Api.Records.Resource lhs, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 rhs)
		{
			return (resourceCalculator.Compare(clusterResource, lhs, rhs) < 0);
		}

		public static bool LessThanOrEqual(ResourceCalculator resourceCalculator, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource, Org.Apache.Hadoop.Yarn.Api.Records.Resource lhs, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 rhs)
		{
			return (resourceCalculator.Compare(clusterResource, lhs, rhs) <= 0);
		}

		public static bool GreaterThan(ResourceCalculator resourceCalculator, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource, Org.Apache.Hadoop.Yarn.Api.Records.Resource lhs, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 rhs)
		{
			return resourceCalculator.Compare(clusterResource, lhs, rhs) > 0;
		}

		public static bool GreaterThanOrEqual(ResourceCalculator resourceCalculator, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource, Org.Apache.Hadoop.Yarn.Api.Records.Resource lhs, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 rhs)
		{
			return resourceCalculator.Compare(clusterResource, lhs, rhs) >= 0;
		}

		public static Org.Apache.Hadoop.Yarn.Api.Records.Resource Min(ResourceCalculator 
			resourceCalculator, Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource, 
			Org.Apache.Hadoop.Yarn.Api.Records.Resource lhs, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 rhs)
		{
			return resourceCalculator.Compare(clusterResource, lhs, rhs) <= 0 ? lhs : rhs;
		}

		public static Org.Apache.Hadoop.Yarn.Api.Records.Resource Max(ResourceCalculator 
			resourceCalculator, Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource, 
			Org.Apache.Hadoop.Yarn.Api.Records.Resource lhs, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 rhs)
		{
			return resourceCalculator.Compare(clusterResource, lhs, rhs) >= 0 ? lhs : rhs;
		}

		public static bool FitsIn(Org.Apache.Hadoop.Yarn.Api.Records.Resource smaller, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 bigger)
		{
			return smaller.GetMemory() <= bigger.GetMemory() && smaller.GetVirtualCores() <= 
				bigger.GetVirtualCores();
		}

		public static Org.Apache.Hadoop.Yarn.Api.Records.Resource ComponentwiseMin(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 lhs, Org.Apache.Hadoop.Yarn.Api.Records.Resource rhs)
		{
			return CreateResource(Math.Min(lhs.GetMemory(), rhs.GetMemory()), Math.Min(lhs.GetVirtualCores
				(), rhs.GetVirtualCores()));
		}

		public static Org.Apache.Hadoop.Yarn.Api.Records.Resource ComponentwiseMax(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 lhs, Org.Apache.Hadoop.Yarn.Api.Records.Resource rhs)
		{
			return CreateResource(Math.Max(lhs.GetMemory(), rhs.GetMemory()), Math.Max(lhs.GetVirtualCores
				(), rhs.GetVirtualCores()));
		}
	}
}
