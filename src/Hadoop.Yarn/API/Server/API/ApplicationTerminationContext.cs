using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api
{
	/// <summary>
	/// Initialization context for
	/// <see cref="AuxiliaryService"/>
	/// when stopping an
	/// application.
	/// </summary>
	public class ApplicationTerminationContext
	{
		private readonly ApplicationId applicationId;

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public ApplicationTerminationContext(ApplicationId applicationId)
		{
			this.applicationId = applicationId;
		}

		/// <summary>
		/// Get
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId"/>
		/// of the application being stopped.
		/// </summary>
		/// <returns>applications ID</returns>
		public virtual ApplicationId GetApplicationId()
		{
			return this.applicationId;
		}
	}
}
