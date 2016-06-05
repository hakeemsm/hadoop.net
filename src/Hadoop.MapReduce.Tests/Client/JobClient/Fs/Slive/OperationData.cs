using System.Text;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>
	/// This class holds the data representing what an operations distribution and
	/// its percentage is (between 0 and 1) and provides operations to access those
	/// types and parse and unparse from and into strings
	/// </summary>
	internal class OperationData
	{
		private const string Sep = ",";

		private Constants.Distribution distribution;

		private double percent;

		internal OperationData(Constants.Distribution d, double p)
		{
			this.distribution = d;
			this.percent = p;
		}

		/// <summary>
		/// Expects a comma separated list (where the first element is the ratio
		/// (between 0 and 100)) and the second element is the distribution (if
		/// non-existent then uniform will be selected).
		/// </summary>
		/// <remarks>
		/// Expects a comma separated list (where the first element is the ratio
		/// (between 0 and 100)) and the second element is the distribution (if
		/// non-existent then uniform will be selected). If an empty list is passed in
		/// then this element will just set the distribution (to uniform) and leave the
		/// percent as null.
		/// </remarks>
		internal OperationData(string data)
		{
			string[] pieces = Helper.GetTrimmedStrings(data);
			distribution = Constants.Distribution.Uniform;
			percent = null;
			if (pieces.Length == 1)
			{
				percent = (double.ParseDouble(pieces[0]) / 100.0d);
			}
			else
			{
				if (pieces.Length >= 2)
				{
					percent = (double.ParseDouble(pieces[0]) / 100.0d);
					distribution = Constants.Distribution.ValueOf(StringUtils.ToUpperCase(pieces[1]));
				}
			}
		}

		/// <summary>Gets the distribution this operation represents</summary>
		/// <returns>Distribution</returns>
		internal virtual Constants.Distribution GetDistribution()
		{
			return distribution;
		}

		/// <summary>Gets the 0 - 1 percent that this operations run ratio should be</summary>
		/// <returns>Double (or null if not given)</returns>
		internal virtual double GetPercent()
		{
			return percent;
		}

		/// <summary>
		/// Returns a string list representation of this object (if the percent is
		/// null) then NaN will be output instead.
		/// </summary>
		/// <remarks>
		/// Returns a string list representation of this object (if the percent is
		/// null) then NaN will be output instead. Format is percent,distribution.
		/// </remarks>
		public override string ToString()
		{
			StringBuilder str = new StringBuilder();
			if (GetPercent() != null)
			{
				str.Append(GetPercent() * 100.0d);
			}
			else
			{
				str.Append(double.NaN);
			}
			str.Append(Sep);
			str.Append(GetDistribution().LowerName());
			return str.ToString();
		}
	}
}
