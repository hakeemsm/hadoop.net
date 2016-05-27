using Sharpen;

namespace Org.Apache.Hadoop.Util.Bloom
{
	/// <summary>Defines the different remove scheme for retouched Bloom filters.</summary>
	/// <remarks>
	/// Defines the different remove scheme for retouched Bloom filters.
	/// <p>
	/// Originally created by
	/// <a href="http://www.one-lab.org">European Commission One-Lab Project 034819</a>.
	/// </remarks>
	public abstract class RemoveScheme
	{
		/// <summary>Random selection.</summary>
		/// <remarks>
		/// Random selection.
		/// <p>
		/// The idea is to randomly select a bit to reset.
		/// </remarks>
		public const short Random = 0;

		/// <summary>MinimumFN Selection.</summary>
		/// <remarks>
		/// MinimumFN Selection.
		/// <p>
		/// The idea is to select the bit to reset that will generate the minimum
		/// number of false negative.
		/// </remarks>
		public const short MinimumFn = 1;

		/// <summary>MaximumFP Selection.</summary>
		/// <remarks>
		/// MaximumFP Selection.
		/// <p>
		/// The idea is to select the bit to reset that will remove the maximum number
		/// of false positive.
		/// </remarks>
		public const short MaximumFp = 2;

		/// <summary>Ratio Selection.</summary>
		/// <remarks>
		/// Ratio Selection.
		/// <p>
		/// The idea is to select the bit to reset that will, at the same time, remove
		/// the maximum number of false positve while minimizing the amount of false
		/// negative generated.
		/// </remarks>
		public const short Ratio = 3;
	}

	public static class RemoveSchemeConstants
	{
	}
}
