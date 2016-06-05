using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager
{
	/// <summary>A dummy app checker class for testing only.</summary>
	public class DummyAppChecker : AppChecker
	{
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[InterfaceAudience.Private]
		public override bool IsApplicationActive(ApplicationId id)
		{
			return false;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[InterfaceAudience.Private]
		public override ICollection<ApplicationId> GetActiveApplications()
		{
			return new AList<ApplicationId>();
		}
	}
}
