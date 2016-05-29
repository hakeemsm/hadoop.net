using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>
	/// <p><code>Resource</code> models a set of computer resources in the
	/// cluster.</p>
	/// <p>Currently it models both <em>memory</em> and <em>CPU</em>.</p>
	/// <p>The unit for memory is megabytes.
	/// </summary>
	/// <remarks>
	/// <p><code>Resource</code> models a set of computer resources in the
	/// cluster.</p>
	/// <p>Currently it models both <em>memory</em> and <em>CPU</em>.</p>
	/// <p>The unit for memory is megabytes. CPU is modeled with virtual cores
	/// (vcores), a unit for expressing parallelism. A node's capacity should
	/// be configured with virtual cores equal to its number of physical cores. A
	/// container should be requested with the number of cores it can saturate, i.e.
	/// the average number of threads it expects to have runnable at a time.</p>
	/// <p>Virtual cores take integer values and thus currently CPU-scheduling is
	/// very coarse.  A complementary axis for CPU requests that represents processing
	/// power will likely be added in the future to enable finer-grained resource
	/// configuration.</p>
	/// <p>Typically, applications request <code>Resource</code> of suitable
	/// capability to run their component tasks.</p>
	/// </remarks>
	/// <seealso cref="ResourceRequest"/>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationMasterProtocol.Allocate(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.AllocateRequest)
	/// 	"/>
	public abstract class Resource : Comparable<Resource>
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static Resource NewInstance(int memory, int vCores)
		{
			Resource resource = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<Resource>();
			resource.SetMemory(memory);
			resource.SetVirtualCores(vCores);
			return resource;
		}

		/// <summary>Get <em>memory</em> of the resource.</summary>
		/// <returns><em>memory</em> of the resource</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract int GetMemory();

		/// <summary>Set <em>memory</em> of the resource.</summary>
		/// <param name="memory"><em>memory</em> of the resource</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetMemory(int memory);

		/// <summary>Get <em>number of virtual cpu cores</em> of the resource.</summary>
		/// <remarks>
		/// Get <em>number of virtual cpu cores</em> of the resource.
		/// Virtual cores are a unit for expressing CPU parallelism. A node's capacity
		/// should be configured with virtual cores equal to its number of physical cores.
		/// A container should be requested with the number of cores it can saturate, i.e.
		/// the average number of threads it expects to have runnable at a time.
		/// </remarks>
		/// <returns><em>num of virtual cpu cores</em> of the resource</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public abstract int GetVirtualCores();

		/// <summary>Set <em>number of virtual cpu cores</em> of the resource.</summary>
		/// <remarks>
		/// Set <em>number of virtual cpu cores</em> of the resource.
		/// Virtual cores are a unit for expressing CPU parallelism. A node's capacity
		/// should be configured with virtual cores equal to its number of physical cores.
		/// A container should be requested with the number of cores it can saturate, i.e.
		/// the average number of threads it expects to have runnable at a time.
		/// </remarks>
		/// <param name="vCores"><em>number of virtual cpu cores</em> of the resource</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public abstract void SetVirtualCores(int vCores);

		public override int GetHashCode()
		{
			int prime = 263167;
			int result = 3571;
			result = 939769357 + GetMemory();
			// prime * result = 939769357 initially
			result = prime * result + GetVirtualCores();
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
			if (!(obj is Resource))
			{
				return false;
			}
			Resource other = (Resource)obj;
			if (GetMemory() != other.GetMemory() || GetVirtualCores() != other.GetVirtualCores
				())
			{
				return false;
			}
			return true;
		}

		public override string ToString()
		{
			return "<memory:" + GetMemory() + ", vCores:" + GetVirtualCores() + ">";
		}

		public abstract int CompareTo(Resource arg1);
	}
}
