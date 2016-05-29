using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>
	/// <see cref="ReservationRequest"/>
	/// represents the request made by an application to
	/// the
	/// <c>ResourceManager</c>
	/// to reserve
	/// <see cref="Resource"/>
	/// s.
	/// <p>
	/// It includes:
	/// <ul>
	/// <li>
	/// <see cref="Resource"/>
	/// required for each request.</li>
	/// <li>
	/// Number of containers, of above specifications, which are required by the
	/// application.
	/// </li>
	/// <li>Concurrency that indicates the gang size of the request.</li>
	/// </ul>
	/// </summary>
	public abstract class ReservationRequest : Comparable<ReservationRequest>
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public static ReservationRequest NewInstance(Resource capability, int numContainers
			)
		{
			return NewInstance(capability, numContainers, 1, -1);
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public static ReservationRequest NewInstance(Resource capability, int numContainers
			, int concurrency, long duration)
		{
			ReservationRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<ReservationRequest
				>();
			request.SetCapability(capability);
			request.SetNumContainers(numContainers);
			request.SetConcurrency(concurrency);
			request.SetDuration(duration);
			return request;
		}

		[System.Serializable]
		public class ReservationRequestComparator : IComparer<ReservationRequest>
		{
			private const long serialVersionUID = 1L;

			public virtual int Compare(ReservationRequest r1, ReservationRequest r2)
			{
				// Compare numContainers, concurrency and capability
				int ret = r1.GetNumContainers() - r2.GetNumContainers();
				if (ret == 0)
				{
					ret = r1.GetConcurrency() - r2.GetConcurrency();
				}
				if (ret == 0)
				{
					ret = r1.GetCapability().CompareTo(r2.GetCapability());
				}
				return ret;
			}
		}

		/// <summary>
		/// Get the
		/// <see cref="Resource"/>
		/// capability of the request.
		/// </summary>
		/// <returns>
		/// 
		/// <see cref="Resource"/>
		/// capability of the request
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract Resource GetCapability();

		/// <summary>
		/// Set the
		/// <see cref="Resource"/>
		/// capability of the request
		/// </summary>
		/// <param name="capability">
		/// 
		/// <see cref="Resource"/>
		/// capability of the request
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetCapability(Resource capability);

		/// <summary>Get the number of containers required with the given specifications.</summary>
		/// <returns>number of containers required with the given specifications</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract int GetNumContainers();

		/// <summary>Set the number of containers required with the given specifications</summary>
		/// <param name="numContainers">
		/// number of containers required with the given
		/// specifications
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetNumContainers(int numContainers);

		/// <summary>Get the number of containers that need to be scheduled concurrently.</summary>
		/// <remarks>
		/// Get the number of containers that need to be scheduled concurrently. The
		/// default value of 1 would fall back to the current non concurrency
		/// constraints on the scheduling behavior.
		/// </remarks>
		/// <returns>the number of containers to be concurrently scheduled</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract int GetConcurrency();

		/// <summary>Set the number of containers that need to be scheduled concurrently.</summary>
		/// <remarks>
		/// Set the number of containers that need to be scheduled concurrently. The
		/// default value of 1 would fall back to the current non concurrency
		/// constraints on the scheduling behavior.
		/// </remarks>
		/// <param name="numContainers">the number of containers to be concurrently scheduled
		/// 	</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetConcurrency(int numContainers);

		/// <summary>Get the duration in milliseconds for which the resource is required.</summary>
		/// <remarks>
		/// Get the duration in milliseconds for which the resource is required. A
		/// default value of -1, indicates an unspecified lease duration, and fallback
		/// to current behavior.
		/// </remarks>
		/// <returns>the duration in milliseconds for which the resource is required</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract long GetDuration();

		/// <summary>Set the duration in milliseconds for which the resource is required.</summary>
		/// <param name="duration">
		/// the duration in milliseconds for which the resource is
		/// required
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetDuration(long duration);

		public override int GetHashCode()
		{
			int prime = 2153;
			int result = 2459;
			Resource capability = GetCapability();
			result = prime * result + ((capability == null) ? 0 : capability.GetHashCode());
			result = prime * result + GetNumContainers();
			result = prime * result + GetConcurrency();
			return result;
		}

		public override bool Equals(object obj)
		{
			if (this == obj)
			{
				return true;
			}
			if (obj == null)
			{
				return false;
			}
			if (GetType() != obj.GetType())
			{
				return false;
			}
			ReservationRequest other = (ReservationRequest)obj;
			Resource capability = GetCapability();
			if (capability == null)
			{
				if (other.GetCapability() != null)
				{
					return false;
				}
			}
			else
			{
				if (!capability.Equals(other.GetCapability()))
				{
					return false;
				}
			}
			if (GetNumContainers() != other.GetNumContainers())
			{
				return false;
			}
			if (GetConcurrency() != other.GetConcurrency())
			{
				return false;
			}
			return true;
		}

		public virtual int CompareTo(ReservationRequest other)
		{
			int numContainersComparison = this.GetNumContainers() - other.GetNumContainers();
			if (numContainersComparison == 0)
			{
				int concurrencyComparison = this.GetConcurrency() - other.GetConcurrency();
				if (concurrencyComparison == 0)
				{
					return this.GetCapability().CompareTo(other.GetCapability());
				}
				else
				{
					return concurrencyComparison;
				}
			}
			else
			{
				return numContainersComparison;
			}
		}
	}
}
