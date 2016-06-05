using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Timeline
{
	/// <summary>The class that hosts a list of timeline domains.</summary>
	public class TimelineDomains
	{
		private IList<TimelineDomain> domains = new AList<TimelineDomain>();

		public TimelineDomains()
		{
		}

		/// <summary>Get a list of domains</summary>
		/// <returns>a list of domains</returns>
		public virtual IList<TimelineDomain> GetDomains()
		{
			return domains;
		}

		/// <summary>Add a single domain into the existing domain list</summary>
		/// <param name="domain">a single domain</param>
		public virtual void AddDomain(TimelineDomain domain)
		{
			domains.AddItem(domain);
		}

		/// <summary>All a list of domains into the existing domain list</summary>
		/// <param name="domains">a list of domains</param>
		public virtual void AddDomains(IList<TimelineDomain> domains)
		{
			Sharpen.Collections.AddAll(this.domains, domains);
		}

		/// <summary>Set the domain list to the given list of domains</summary>
		/// <param name="domains">a list of domains</param>
		public virtual void SetDomains(IList<TimelineDomain> domains)
		{
			this.domains = domains;
		}
	}
}
