using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util.Resource
{
	/// <summary>
	/// A set of
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Resource"/>
	/// comparison and manipulation interfaces.
	/// </summary>
	public abstract class ResourceCalculator
	{
		public abstract int Compare(Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource
			, Org.Apache.Hadoop.Yarn.Api.Records.Resource lhs, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 rhs);

		public static int DivideAndCeil(int a, int b)
		{
			if (b == 0)
			{
				return 0;
			}
			return (a + (b - 1)) / b;
		}

		public static int RoundUp(int a, int b)
		{
			return DivideAndCeil(a, b) * b;
		}

		public static int RoundDown(int a, int b)
		{
			return (a / b) * b;
		}

		/// <summary>
		/// Compute the number of containers which can be allocated given
		/// <code>available</code> and <code>required</code> resources.
		/// </summary>
		/// <param name="available">available resources</param>
		/// <param name="required">required resources</param>
		/// <returns>number of containers which can be allocated</returns>
		public abstract int ComputeAvailableContainers(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 available, Org.Apache.Hadoop.Yarn.Api.Records.Resource required);

		/// <summary>
		/// Multiply resource <code>r</code> by factor <code>by</code>
		/// and normalize up using step-factor <code>stepFactor</code>.
		/// </summary>
		/// <param name="r">resource to be multiplied</param>
		/// <param name="by">multiplier</param>
		/// <param name="stepFactor">factor by which to normalize up</param>
		/// <returns>resulting normalized resource</returns>
		public abstract Org.Apache.Hadoop.Yarn.Api.Records.Resource MultiplyAndNormalizeUp
			(Org.Apache.Hadoop.Yarn.Api.Records.Resource r, double by, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 stepFactor);

		/// <summary>
		/// Multiply resource <code>r</code> by factor <code>by</code>
		/// and normalize down using step-factor <code>stepFactor</code>.
		/// </summary>
		/// <param name="r">resource to be multiplied</param>
		/// <param name="by">multiplier</param>
		/// <param name="stepFactor">factor by which to normalize down</param>
		/// <returns>resulting normalized resource</returns>
		public abstract Org.Apache.Hadoop.Yarn.Api.Records.Resource MultiplyAndNormalizeDown
			(Org.Apache.Hadoop.Yarn.Api.Records.Resource r, double by, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 stepFactor);

		/// <summary>
		/// Normalize resource <code>r</code> given the base
		/// <code>minimumResource</code> and verify against max allowed
		/// <code>maximumResource</code>
		/// </summary>
		/// <param name="r">resource</param>
		/// <param name="minimumResource">step-factor</param>
		/// <param name="maximumResource">the upper bound of the resource to be allocated</param>
		/// <returns>normalized resource</returns>
		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource Normalize(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 r, Org.Apache.Hadoop.Yarn.Api.Records.Resource minimumResource, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 maximumResource)
		{
			return Normalize(r, minimumResource, maximumResource, minimumResource);
		}

		/// <summary>
		/// Normalize resource <code>r</code> given the base
		/// <code>minimumResource</code> and verify against max allowed
		/// <code>maximumResource</code> using a step factor for hte normalization.
		/// </summary>
		/// <param name="r">resource</param>
		/// <param name="minimumResource">minimum value</param>
		/// <param name="maximumResource">the upper bound of the resource to be allocated</param>
		/// <param name="stepFactor">the increment for resources to be allocated</param>
		/// <returns>normalized resource</returns>
		public abstract Org.Apache.Hadoop.Yarn.Api.Records.Resource Normalize(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 r, Org.Apache.Hadoop.Yarn.Api.Records.Resource minimumResource, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 maximumResource, Org.Apache.Hadoop.Yarn.Api.Records.Resource stepFactor);

		/// <summary>Round-up resource <code>r</code> given factor <code>stepFactor</code>.</summary>
		/// <param name="r">resource</param>
		/// <param name="stepFactor">step-factor</param>
		/// <returns>rounded resource</returns>
		public abstract Org.Apache.Hadoop.Yarn.Api.Records.Resource RoundUp(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 r, Org.Apache.Hadoop.Yarn.Api.Records.Resource stepFactor);

		/// <summary>Round-down resource <code>r</code> given factor <code>stepFactor</code>.
		/// 	</summary>
		/// <param name="r">resource</param>
		/// <param name="stepFactor">step-factor</param>
		/// <returns>rounded resource</returns>
		public abstract Org.Apache.Hadoop.Yarn.Api.Records.Resource RoundDown(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 r, Org.Apache.Hadoop.Yarn.Api.Records.Resource stepFactor);

		/// <summary>
		/// Divide resource <code>numerator</code> by resource <code>denominator</code>
		/// using specified policy (domination, average, fairness etc.); hence overall
		/// <code>clusterResource</code> is provided for context.
		/// </summary>
		/// <param name="clusterResource">cluster resources</param>
		/// <param name="numerator">numerator</param>
		/// <param name="denominator">denominator</param>
		/// <returns>
		/// <code>numerator</code>/<code>denominator</code>
		/// using specific policy
		/// </returns>
		public abstract float Divide(Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource
			, Org.Apache.Hadoop.Yarn.Api.Records.Resource numerator, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 denominator);

		/// <summary>
		/// Determine if a resource is not suitable for use as a divisor
		/// (will result in divide by 0, etc)
		/// </summary>
		/// <param name="r">resource</param>
		/// <returns>true if divisor is invalid (should not be used), false else</returns>
		public abstract bool IsInvalidDivisor(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 r);

		/// <summary>Ratio of resource <code>a</code> to resource <code>b</code>.</summary>
		/// <param name="a">resource</param>
		/// <param name="b">resource</param>
		/// <returns>ratio of resource <code>a</code> to resource <code>b</code></returns>
		public abstract float Ratio(Org.Apache.Hadoop.Yarn.Api.Records.Resource a, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 b);

		/// <summary>Divide-and-ceil <code>numerator</code> by <code>denominator</code>.</summary>
		/// <param name="numerator">numerator resource</param>
		/// <param name="denominator">denominator</param>
		/// <returns>resultant resource</returns>
		public abstract Org.Apache.Hadoop.Yarn.Api.Records.Resource DivideAndCeil(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 numerator, int denominator);
	}
}
