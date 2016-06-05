using System;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util.Resource
{
	public class DefaultResourceCalculator : ResourceCalculator
	{
		public override int Compare(Org.Apache.Hadoop.Yarn.Api.Records.Resource unused, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 lhs, Org.Apache.Hadoop.Yarn.Api.Records.Resource rhs)
		{
			// Only consider memory
			return lhs.GetMemory() - rhs.GetMemory();
		}

		public override int ComputeAvailableContainers(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 available, Org.Apache.Hadoop.Yarn.Api.Records.Resource required)
		{
			// Only consider memory
			return available.GetMemory() / required.GetMemory();
		}

		public override float Divide(Org.Apache.Hadoop.Yarn.Api.Records.Resource unused, 
			Org.Apache.Hadoop.Yarn.Api.Records.Resource numerator, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 denominator)
		{
			return Ratio(numerator, denominator);
		}

		public override bool IsInvalidDivisor(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 r)
		{
			if (r.GetMemory() == 0.0f)
			{
				return true;
			}
			return false;
		}

		public override float Ratio(Org.Apache.Hadoop.Yarn.Api.Records.Resource a, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 b)
		{
			return (float)a.GetMemory() / b.GetMemory();
		}

		public override Org.Apache.Hadoop.Yarn.Api.Records.Resource DivideAndCeil(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 numerator, int denominator)
		{
			return Resources.CreateResource(DivideAndCeil(numerator.GetMemory(), denominator)
				);
		}

		public override Org.Apache.Hadoop.Yarn.Api.Records.Resource Normalize(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 r, Org.Apache.Hadoop.Yarn.Api.Records.Resource minimumResource, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 maximumResource, Org.Apache.Hadoop.Yarn.Api.Records.Resource stepFactor)
		{
			int normalizedMemory = Math.Min(RoundUp(Math.Max(r.GetMemory(), minimumResource.GetMemory
				()), stepFactor.GetMemory()), maximumResource.GetMemory());
			return Resources.CreateResource(normalizedMemory);
		}

		public override Org.Apache.Hadoop.Yarn.Api.Records.Resource Normalize(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 r, Org.Apache.Hadoop.Yarn.Api.Records.Resource minimumResource, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 maximumResource)
		{
			return Normalize(r, minimumResource, maximumResource, minimumResource);
		}

		public override Org.Apache.Hadoop.Yarn.Api.Records.Resource RoundUp(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 r, Org.Apache.Hadoop.Yarn.Api.Records.Resource stepFactor)
		{
			return Resources.CreateResource(RoundUp(r.GetMemory(), stepFactor.GetMemory()));
		}

		public override Org.Apache.Hadoop.Yarn.Api.Records.Resource RoundDown(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 r, Org.Apache.Hadoop.Yarn.Api.Records.Resource stepFactor)
		{
			return Resources.CreateResource(RoundDown(r.GetMemory(), stepFactor.GetMemory()));
		}

		public override Org.Apache.Hadoop.Yarn.Api.Records.Resource MultiplyAndNormalizeUp
			(Org.Apache.Hadoop.Yarn.Api.Records.Resource r, double by, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 stepFactor)
		{
			return Resources.CreateResource(RoundUp((int)(r.GetMemory() * by + 0.5), stepFactor
				.GetMemory()));
		}

		public override Org.Apache.Hadoop.Yarn.Api.Records.Resource MultiplyAndNormalizeDown
			(Org.Apache.Hadoop.Yarn.Api.Records.Resource r, double by, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 stepFactor)
		{
			return Resources.CreateResource(RoundDown((int)(r.GetMemory() * by), stepFactor.GetMemory
				()));
		}
	}
}
