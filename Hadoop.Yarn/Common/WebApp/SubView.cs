using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp
{
	/// <summary>Interface for SubView to avoid top-level inclusion</summary>
	public interface SubView
	{
		/// <summary>render the sub-view</summary>
		void RenderPartial();
	}
}
