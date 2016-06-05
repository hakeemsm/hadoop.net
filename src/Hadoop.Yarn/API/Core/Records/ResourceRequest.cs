using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>
	/// <c>ResourceRequest</c>
	/// represents the request made
	/// by an application to the
	/// <c>ResourceManager</c>
	/// to obtain various
	/// <c>Container</c>
	/// allocations.
	/// <p>
	/// It includes:
	/// <ul>
	/// <li>
	/// <see cref="Priority"/>
	/// of the request.</li>
	/// <li>
	/// The <em>name</em> of the machine or rack on which the allocation is
	/// desired. A special value of <em>*</em> signifies that
	/// <em>any</em> host/rack is acceptable to the application.
	/// </li>
	/// <li>
	/// <see cref="Resource"/>
	/// required for each request.</li>
	/// <li>
	/// Number of containers, of above specifications, which are required
	/// by the application.
	/// </li>
	/// <li>
	/// A boolean <em>relaxLocality</em> flag, defaulting to
	/// <see langword="true"/>
	/// ,
	/// which tells the
	/// <c>ResourceManager</c>
	/// if the application wants
	/// locality to be loose (i.e. allows fall-through to rack or <em>any</em>)
	/// or strict (i.e. specify hard constraint on resource allocation).
	/// </li>
	/// </ul>
	/// </summary>
	/// <seealso cref="Resource"/>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationMasterProtocol.Allocate(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.AllocateRequest)
	/// 	"/>
	public abstract class ResourceRequest : Comparable<ResourceRequest>
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static ResourceRequest NewInstance(Priority priority, string hostName, Resource
			 capability, int numContainers)
		{
			return NewInstance(priority, hostName, capability, numContainers, true);
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static ResourceRequest NewInstance(Priority priority, string hostName, Resource
			 capability, int numContainers, bool relaxLocality)
		{
			return NewInstance(priority, hostName, capability, numContainers, relaxLocality, 
				null);
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static ResourceRequest NewInstance(Priority priority, string hostName, Resource
			 capability, int numContainers, bool relaxLocality, string labelExpression)
		{
			ResourceRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<ResourceRequest
				>();
			request.SetPriority(priority);
			request.SetResourceName(hostName);
			request.SetCapability(capability);
			request.SetNumContainers(numContainers);
			request.SetRelaxLocality(relaxLocality);
			request.SetNodeLabelExpression(labelExpression);
			return request;
		}

		[System.Serializable]
		public class ResourceRequestComparator : IComparer<ResourceRequest>
		{
			private const long serialVersionUID = 1L;

			public virtual int Compare(ResourceRequest r1, ResourceRequest r2)
			{
				// Compare priority, host and capability
				int ret = r1.GetPriority().CompareTo(r2.GetPriority());
				if (ret == 0)
				{
					string h1 = r1.GetResourceName();
					string h2 = r2.GetResourceName();
					ret = string.CompareOrdinal(h1, h2);
				}
				if (ret == 0)
				{
					ret = r1.GetCapability().CompareTo(r2.GetCapability());
				}
				return ret;
			}
		}

		/// <summary>The constant string representing no locality.</summary>
		/// <remarks>
		/// The constant string representing no locality.
		/// It should be used by all references that want to pass an arbitrary host
		/// name in.
		/// </remarks>
		public const string Any = "*";

		/// <summary>
		/// Check whether the given <em>host/rack</em> string represents an arbitrary
		/// host name.
		/// </summary>
		/// <param name="hostName"><em>host/rack</em> on which the allocation is desired</param>
		/// <returns>
		/// whether the given <em>host/rack</em> string represents an arbitrary
		/// host name
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static bool IsAnyLocation(string hostName)
		{
			return Any.Equals(hostName);
		}

		/// <summary>Get the <code>Priority</code> of the request.</summary>
		/// <returns><code>Priority</code> of the request</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract Priority GetPriority();

		/// <summary>Set the <code>Priority</code> of the request</summary>
		/// <param name="priority"><code>Priority</code> of the request</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetPriority(Priority priority);

		/// <summary>Get the resource (e.g.</summary>
		/// <remarks>
		/// Get the resource (e.g. <em>host/rack</em>) on which the allocation
		/// is desired.
		/// A special value of <em>*</em> signifies that <em>any</em> resource
		/// (host/rack) is acceptable.
		/// </remarks>
		/// <returns>
		/// resource (e.g. <em>host/rack</em>) on which the allocation
		/// is desired
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract string GetResourceName();

		/// <summary>Set the resource name (e.g.</summary>
		/// <remarks>
		/// Set the resource name (e.g. <em>host/rack</em>) on which the allocation
		/// is desired.
		/// A special value of <em>*</em> signifies that <em>any</em> resource name
		/// (e.g. host/rack) is acceptable.
		/// </remarks>
		/// <param name="resourceName">
		/// (e.g. <em>host/rack</em>) on which the
		/// allocation is desired
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetResourceName(string resourceName);

		/// <summary>Get the <code>Resource</code> capability of the request.</summary>
		/// <returns><code>Resource</code> capability of the request</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract Resource GetCapability();

		/// <summary>Set the <code>Resource</code> capability of the request</summary>
		/// <param name="capability"><code>Resource</code> capability of the request</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetCapability(Resource capability);

		/// <summary>Get the number of containers required with the given specifications.</summary>
		/// <returns>number of containers required with the given specifications</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract int GetNumContainers();

		/// <summary>Set the number of containers required with the given specifications</summary>
		/// <param name="numContainers">
		/// number of containers required with the given
		/// specifications
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetNumContainers(int numContainers);

		/// <summary>
		/// Get whether locality relaxation is enabled with this
		/// <code>ResourceRequest</code>.
		/// </summary>
		/// <remarks>
		/// Get whether locality relaxation is enabled with this
		/// <code>ResourceRequest</code>. Defaults to true.
		/// </remarks>
		/// <returns>
		/// whether locality relaxation is enabled with this
		/// <code>ResourceRequest</code>.
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract bool GetRelaxLocality();

		/// <summary>
		/// <p>For a request at a network hierarchy level, set whether locality can be relaxed
		/// to that level and beyond.<p>
		/// <p>If the flag is off on a rack-level <code>ResourceRequest</code>,
		/// containers at that request's priority will not be assigned to nodes on that
		/// request's rack unless requests specifically for those nodes have also been
		/// submitted.<p>
		/// <p>If the flag is off on an
		/// <see cref="Any"/>
		/// -level
		/// <code>ResourceRequest</code>, containers at that request's priority will
		/// only be assigned on racks for which specific requests have also been
		/// submitted.<p>
		/// <p>For example, to request a container strictly on a specific node, the
		/// corresponding rack-level and any-level requests should have locality
		/// relaxation set to false.  Similarly, to request a container strictly on a
		/// specific rack, the corresponding any-level request should have locality
		/// relaxation set to false.<p>
		/// </summary>
		/// <param name="relaxLocality">
		/// whether locality relaxation is enabled with this
		/// <code>ResourceRequest</code>.
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetRelaxLocality(bool relaxLocality);

		/// <summary>Get node-label-expression for this Resource Request.</summary>
		/// <remarks>
		/// Get node-label-expression for this Resource Request. If this is set, all
		/// containers allocated to satisfy this resource-request will be only on those
		/// nodes that satisfy this node-label-expression.
		/// Please note that node label expression now can only take effect when the
		/// resource request has resourceName = ANY
		/// </remarks>
		/// <returns>node-label-expression</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public abstract string GetNodeLabelExpression();

		/// <summary>Set node label expression of this resource request.</summary>
		/// <remarks>
		/// Set node label expression of this resource request. Now only support
		/// specifying a single node label. In the future we will support more complex
		/// node label expression specification like
		/// <c>AND(&&), OR(||)</c>
		/// , etc.
		/// Any please note that node label expression now can only take effect when
		/// the resource request has resourceName = ANY
		/// </remarks>
		/// <param name="nodelabelExpression">node-label-expression of this ResourceRequest</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public abstract void SetNodeLabelExpression(string nodelabelExpression);

		public override int GetHashCode()
		{
			int prime = 2153;
			int result = 2459;
			Resource capability = GetCapability();
			string hostName = GetResourceName();
			Priority priority = GetPriority();
			result = prime * result + ((capability == null) ? 0 : capability.GetHashCode());
			result = prime * result + ((hostName == null) ? 0 : hostName.GetHashCode());
			result = prime * result + GetNumContainers();
			result = prime * result + ((priority == null) ? 0 : priority.GetHashCode());
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
			ResourceRequest other = (ResourceRequest)obj;
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
			string hostName = GetResourceName();
			if (hostName == null)
			{
				if (other.GetResourceName() != null)
				{
					return false;
				}
			}
			else
			{
				if (!hostName.Equals(other.GetResourceName()))
				{
					return false;
				}
			}
			if (GetNumContainers() != other.GetNumContainers())
			{
				return false;
			}
			Priority priority = GetPriority();
			if (priority == null)
			{
				if (other.GetPriority() != null)
				{
					return false;
				}
			}
			else
			{
				if (!priority.Equals(other.GetPriority()))
				{
					return false;
				}
			}
			if (GetNodeLabelExpression() == null)
			{
				if (other.GetNodeLabelExpression() != null)
				{
					return false;
				}
			}
			else
			{
				// do normalize on label expression before compare
				string label1 = GetNodeLabelExpression().ReplaceAll("[\\t ]", string.Empty);
				string label2 = other.GetNodeLabelExpression() == null ? null : other.GetNodeLabelExpression
					().ReplaceAll("[\\t ]", string.Empty);
				if (!label1.Equals(label2))
				{
					return false;
				}
			}
			return true;
		}

		public virtual int CompareTo(ResourceRequest other)
		{
			int priorityComparison = this.GetPriority().CompareTo(other.GetPriority());
			if (priorityComparison == 0)
			{
				int hostNameComparison = string.CompareOrdinal(this.GetResourceName(), other.GetResourceName
					());
				if (hostNameComparison == 0)
				{
					int capabilityComparison = this.GetCapability().CompareTo(other.GetCapability());
					if (capabilityComparison == 0)
					{
						return this.GetNumContainers() - other.GetNumContainers();
					}
					else
					{
						return capabilityComparison;
					}
				}
				else
				{
					return hostNameComparison;
				}
			}
			else
			{
				return priorityComparison;
			}
		}
	}
}
