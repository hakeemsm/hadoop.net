using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	public abstract class ResourceOption
	{
		public static ResourceOption NewInstance(Resource resource, int overCommitTimeout
			)
		{
			ResourceOption resourceOption = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<ResourceOption
				>();
			resourceOption.SetResource(resource);
			resourceOption.SetOverCommitTimeout(overCommitTimeout);
			resourceOption.Build();
			return resourceOption;
		}

		/// <summary>Get the <em>resource</em> of the ResourceOption.</summary>
		/// <returns><em>resource</em> of the ResourceOption</returns>
		[InterfaceAudience.Private]
		[InterfaceStability.Evolving]
		public abstract Resource GetResource();

		[InterfaceAudience.Private]
		[InterfaceStability.Evolving]
		protected internal abstract void SetResource(Resource resource);

		/// <summary>
		/// Get timeout for tolerant of resource over-commitment
		/// Note: negative value means no timeout so that allocated containers will
		/// keep running until the end even under resource over-commitment cases.
		/// </summary>
		/// <returns><em>overCommitTimeout</em> of the ResourceOption</returns>
		[InterfaceAudience.Private]
		[InterfaceStability.Evolving]
		public abstract int GetOverCommitTimeout();

		[InterfaceAudience.Private]
		[InterfaceStability.Evolving]
		protected internal abstract void SetOverCommitTimeout(int overCommitTimeout);

		[InterfaceAudience.Private]
		[InterfaceStability.Evolving]
		protected internal abstract void Build();

		public override string ToString()
		{
			return "Resource:" + GetResource().ToString() + ", overCommitTimeout:" + GetOverCommitTimeout
				();
		}
	}
}
