using System;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util.Resource
{
	/// <summary>
	/// A
	/// <see cref="ResourceCalculator"/>
	/// which uses the concept of
	/// <em>dominant resource</em> to compare multi-dimensional resources.
	/// Essentially the idea is that the in a multi-resource environment,
	/// the resource allocation should be determined by the dominant share
	/// of an entity (user or queue), which is the maximum share that the
	/// entity has been allocated of any resource.
	/// In a nutshell, it seeks to maximize the minimum dominant share across
	/// all entities.
	/// For example, if user A runs CPU-heavy tasks and user B runs
	/// memory-heavy tasks, it attempts to equalize CPU share of user A
	/// with Memory-share of user B.
	/// In the single resource case, it reduces to max-min fairness for that resource.
	/// See the Dominant Resource Fairness paper for more details:
	/// www.cs.berkeley.edu/~matei/papers/2011/nsdi_drf.pdf
	/// </summary>
	public class DominantResourceCalculator : ResourceCalculator
	{
		public override int Compare(Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource
			, Org.Apache.Hadoop.Yarn.Api.Records.Resource lhs, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 rhs)
		{
			if (lhs.Equals(rhs))
			{
				return 0;
			}
			if (IsInvalidDivisor(clusterResource))
			{
				if ((lhs.GetMemory() < rhs.GetMemory() && lhs.GetVirtualCores() > rhs.GetVirtualCores
					()) || (lhs.GetMemory() > rhs.GetMemory() && lhs.GetVirtualCores() < rhs.GetVirtualCores
					()))
				{
					return 0;
				}
				else
				{
					if (lhs.GetMemory() > rhs.GetMemory() || lhs.GetVirtualCores() > rhs.GetVirtualCores
						())
					{
						return 1;
					}
					else
					{
						if (lhs.GetMemory() < rhs.GetMemory() || lhs.GetVirtualCores() < rhs.GetVirtualCores
							())
						{
							return -1;
						}
					}
				}
			}
			float l = GetResourceAsValue(clusterResource, lhs, true);
			float r = GetResourceAsValue(clusterResource, rhs, true);
			if (l < r)
			{
				return -1;
			}
			else
			{
				if (l > r)
				{
					return 1;
				}
				else
				{
					l = GetResourceAsValue(clusterResource, lhs, false);
					r = GetResourceAsValue(clusterResource, rhs, false);
					if (l < r)
					{
						return -1;
					}
					else
					{
						if (l > r)
						{
							return 1;
						}
					}
				}
			}
			return 0;
		}

		/// <summary>
		/// Use 'dominant' for now since we only have 2 resources - gives us a slight
		/// performance boost.
		/// </summary>
		/// <remarks>
		/// Use 'dominant' for now since we only have 2 resources - gives us a slight
		/// performance boost.
		/// Once we add more resources, we'll need a more complicated (and slightly
		/// less performant algorithm).
		/// </remarks>
		protected internal virtual float GetResourceAsValue(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource, Org.Apache.Hadoop.Yarn.Api.Records.Resource resource, bool dominant
			)
		{
			// Just use 'dominant' resource
			return (dominant) ? Math.Max((float)resource.GetMemory() / clusterResource.GetMemory
				(), (float)resource.GetVirtualCores() / clusterResource.GetVirtualCores()) : Math
				.Min((float)resource.GetMemory() / clusterResource.GetMemory(), (float)resource.
				GetVirtualCores() / clusterResource.GetVirtualCores());
		}

		public override int ComputeAvailableContainers(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 available, Org.Apache.Hadoop.Yarn.Api.Records.Resource required)
		{
			return Math.Min(available.GetMemory() / required.GetMemory(), available.GetVirtualCores
				() / required.GetVirtualCores());
		}

		public override float Divide(Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource
			, Org.Apache.Hadoop.Yarn.Api.Records.Resource numerator, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 denominator)
		{
			return GetResourceAsValue(clusterResource, numerator, true) / GetResourceAsValue(
				clusterResource, denominator, true);
		}

		public override bool IsInvalidDivisor(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 r)
		{
			if (r.GetMemory() == 0.0f || r.GetVirtualCores() == 0.0f)
			{
				return true;
			}
			return false;
		}

		public override float Ratio(Org.Apache.Hadoop.Yarn.Api.Records.Resource a, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 b)
		{
			return Math.Max((float)a.GetMemory() / b.GetMemory(), (float)a.GetVirtualCores() 
				/ b.GetVirtualCores());
		}

		public override Org.Apache.Hadoop.Yarn.Api.Records.Resource DivideAndCeil(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 numerator, int denominator)
		{
			return Resources.CreateResource(DivideAndCeil(numerator.GetMemory(), denominator)
				, DivideAndCeil(numerator.GetVirtualCores(), denominator));
		}

		public override Org.Apache.Hadoop.Yarn.Api.Records.Resource Normalize(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 r, Org.Apache.Hadoop.Yarn.Api.Records.Resource minimumResource, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 maximumResource, Org.Apache.Hadoop.Yarn.Api.Records.Resource stepFactor)
		{
			int normalizedMemory = Math.Min(RoundUp(Math.Max(r.GetMemory(), minimumResource.GetMemory
				()), stepFactor.GetMemory()), maximumResource.GetMemory());
			int normalizedCores = Math.Min(RoundUp(Math.Max(r.GetVirtualCores(), minimumResource
				.GetVirtualCores()), stepFactor.GetVirtualCores()), maximumResource.GetVirtualCores
				());
			return Resources.CreateResource(normalizedMemory, normalizedCores);
		}

		public override Org.Apache.Hadoop.Yarn.Api.Records.Resource RoundUp(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 r, Org.Apache.Hadoop.Yarn.Api.Records.Resource stepFactor)
		{
			return Resources.CreateResource(RoundUp(r.GetMemory(), stepFactor.GetMemory()), RoundUp
				(r.GetVirtualCores(), stepFactor.GetVirtualCores()));
		}

		public override Org.Apache.Hadoop.Yarn.Api.Records.Resource RoundDown(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 r, Org.Apache.Hadoop.Yarn.Api.Records.Resource stepFactor)
		{
			return Resources.CreateResource(RoundDown(r.GetMemory(), stepFactor.GetMemory()), 
				RoundDown(r.GetVirtualCores(), stepFactor.GetVirtualCores()));
		}

		public override Org.Apache.Hadoop.Yarn.Api.Records.Resource MultiplyAndNormalizeUp
			(Org.Apache.Hadoop.Yarn.Api.Records.Resource r, double by, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 stepFactor)
		{
			return Resources.CreateResource(RoundUp((int)Math.Ceil(r.GetMemory() * by), stepFactor
				.GetMemory()), RoundUp((int)Math.Ceil(r.GetVirtualCores() * by), stepFactor.GetVirtualCores
				()));
		}

		public override Org.Apache.Hadoop.Yarn.Api.Records.Resource MultiplyAndNormalizeDown
			(Org.Apache.Hadoop.Yarn.Api.Records.Resource r, double by, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 stepFactor)
		{
			return Resources.CreateResource(RoundDown((int)(r.GetMemory() * by), stepFactor.GetMemory
				()), RoundDown((int)(r.GetVirtualCores() * by), stepFactor.GetVirtualCores()));
		}
	}
}
