using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>Specific container requested back by the <code>ResourceManager</code>.</summary>
	/// <seealso cref="PreemptionContract"/>
	/// <seealso cref="StrictPreemptionContract"/>
	public abstract class PreemptionContainer
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static PreemptionContainer NewInstance(ContainerId id)
		{
			PreemptionContainer container = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<PreemptionContainer
				>();
			container.SetId(id);
			return container;
		}

		/// <returns>Container referenced by this handle.</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public abstract ContainerId GetId();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetId(ContainerId id);
	}
}
